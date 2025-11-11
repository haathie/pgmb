/*
to explain inner fns: https://stackoverflow.com/a/30547418

-- Enable auto_explain for debugging
LOAD 'auto_explain';
SET auto_explain.log_nested_statements = 'on';
SET auto_explain.log_min_duration = 0;
SET client_min_messages TO log;
*/

CREATE SCHEMA IF NOT EXISTS "pgmb2";

SET search_path TO pgmb2, public;

-- create the configuration table for pgmb2 ----------------

CREATE TYPE config_type AS ENUM(
	'plugin_version',
	'oldest_partition_interval',
	'future_partitions_to_create',
	'partition_size',
	'tx_commit_lookback_interval'
);

CREATE TABLE IF NOT EXISTS subscriptions_config(
	-- unique identifier for the subscription config
	id config_type PRIMARY KEY,
	value TEXT
);

CREATE OR REPLACE FUNCTION get_config_value(
	config_id config_type
) RETURNS TEXT AS $$
	SELECT value FROM subscriptions_config WHERE id = config_id
$$ LANGUAGE sql STRICT STABLE PARALLEL SAFE SET SEARCH_PATH TO pgmb2, public;

INSERT INTO subscriptions_config(id, value)
	VALUES
		('plugin_version', '0.1.0'),
		('oldest_partition_interval', '2 hours'),
		('future_partitions_to_create', '12'),
		('partition_size', 'hour'),
		('tx_commit_lookback_interval', '5 minutes');

-- we'll create the events table next & its functions ---------------

CREATE DOMAIN event_id AS VARCHAR(24);

-- fn to create a random bigint.
CREATE OR REPLACE FUNCTION create_random_bigint()
RETURNS BIGINT AS $$
BEGIN
	-- the message ID allows for 7 hex-bytes of randomness,
	-- i.e. 28 bits of randomness. Thus, the max we allow is 2^28/2
	-- i.e. 0xffffff8, which allows for batch inserts to increment the
	-- randomness for up to another 2^28/2 messages (more than enough)
	RETURN (random() * 0xffffff8)::BIGINT;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

-- Creates a timestamped event ID. It is a 24-character string
-- that consists of:
-- 1. 'ps' prefix
-- 2. 13-character hex representation of the timestamp in microseconds
-- 3. remaining random
CREATE OR REPLACE FUNCTION create_event_id(
	ts timestamptz DEFAULT clock_timestamp(),
	rand bigint DEFAULT create_random_bigint()
)
RETURNS event_id AS $$
SELECT substr(
	-- ensure we're always 28 characters long by right-padding with '0's
	'ps'
	-- we'll give 13 hex characters for microsecond timestamp
	|| lpad(to_hex((extract(epoch from ts) * 1000000)::bigint), 13,	'0')
	-- xids are 32 bits, so 8 hex characters
	-- || lpad(to_hex(tx_id), 8, '0')
	-- fill remaining with randomness
	|| rpad(to_hex(rand), 9, '0'),
	1,
	24
)
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb2, public;

CREATE TABLE IF NOT EXISTS events(
	id event_id PRIMARY KEY DEFAULT create_event_id(),
	topic VARCHAR(255) NOT NULL,
	xid bigint NOT NULL DEFAULT txid_current(),
	payload JSONB NOT NULL,
	metadata JSONB
) PARTITION BY RANGE (id);

CREATE INDEX IF NOT EXISTS idx_events_topic_id
	ON events (topic, id);

CREATE OR REPLACE FUNCTION get_event_partition_name(
	table_name TEXT,
	ts timestamptz
) RETURNS TEXT AS $$
	SELECT table_name || '_' || to_char(ts, 'YYYYMMDDHH24')
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

-- Partition maintenance function for events table. Creates partitions for
-- the current and next interval. Deletes partitions that are older than the
-- configured time interval.
-- Exact partition size and oldest partition interval can be configured
-- using the "subscriptions_config" table.
CREATE OR REPLACE FUNCTION maintain_events_table(
	current_ts timestamptz DEFAULT NOW()
)
RETURNS void AS $$
DECLARE
	schema_name TEXT := 'pgmb2';
	table_name TEXT := 'events';
	partition_size TEXT := get_config_value('partition_size');
	partition_interval INTERVAL := ('1 ' || partition_size);

	oldest_partition_interval INTERVAL :=
		get_config_value('oldest_partition_interval')::INTERVAL;
	future_partitions_to_create INT :=
		get_config_value('future_partitions_to_create')::INT;

	lock_key BIGINT :=
		hashtext(schema_name || '.' || table_name || '.partition_maintenance');

	ts_trunc timestamptz := date_trunc(partition_size, current_ts);
	p_info RECORD;
BEGIN
	IF NOT pg_try_advisory_lock(lock_key) THEN
		-- If can't get lock, means another process is already maintaining the table
		RETURN;
	END IF;

	-- Ensure current and next hour partitions exist
	FOR i IN 0..future_partitions_to_create LOOP
		DECLARE
			target_ts timestamptz := ts_trunc + (i * partition_interval);
		BEGIN
			EXECUTE format(
				'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I
					FOR VALUES FROM (%L) TO (%L)',
				schema_name,
				get_event_partition_name(table_name, target_ts),
				schema_name,
				table_name,
				create_event_id(target_ts, 0),
				-- fill with max possible tx id
				create_event_id(target_ts + partition_interval, 0)
			);
		END;
	END LOOP;

	-- Drop old partitions
	FOR p_info IN (
		SELECT relname FROM pg_class
		WHERE
			relname < get_event_partition_name(
				table_name, current_ts - oldest_partition_interval
			)
			AND relname LIKE (table_name || '_%')
			AND relkind = 'r'
			AND relnamespace 
				= (SELECT oid FROM pg_namespace WHERE nspname = schema_name)
	) LOOP
		EXECUTE format('DROP TABLE IF EXISTS %I.%I', schema_name, p_info.relname);
	END LOOP;

	-- unlock the advisory lock
	PERFORM pg_advisory_unlock(lock_key);
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
	SET search_path TO pgmb2, public;

SELECT maintain_events_table();

-- readers and reader state tables ----------------

CREATE TABLE IF NOT EXISTS readers (
	id VARCHAR(64) PRIMARY KEY,
	created_at timestamptz NOT NULL DEFAULT NOW(),
	last_read_event_id event_id NOT NULL DEFAULT create_event_id(),
	last_read_at timestamptz
);

CREATE TABLE IF NOT EXISTS reader_xid_state (
	reader_id VARCHAR(64) NOT NULL REFERENCES readers(id) ON DELETE CASCADE,
	xid bigint NOT NULL,
	created_at timestamptz NOT NULL DEFAULT NOW(),
	completed_at timestamptz,
	last_read_event_id event_id,
	max_event_id event_id NOT NULL,
	PRIMARY KEY (reader_id, xid)
);

CREATE OR REPLACE FUNCTION read_next_events(
	rid VARCHAR(64), chunk_size INT
) RETURNS SETOF events AS $$
DECLARE
	lookback_interval INTERVAL :=
		get_config_value('tx_commit_lookback_interval')::INTERVAL;
	now_event_id event_id := create_event_id(NOW());
	lookback_start_eid event_id;
	lre event_id;
	r_created_at timestamptz;
BEGIN
	-- get the reader state
	SELECT last_read_event_id, created_at INTO lre, r_created_at
	FROM readers WHERE id = rid;

	-- determine lookback start event ID
	lookback_start_eid := create_event_id(
		GREATEST(NOW() - lookback_interval, r_created_at)
	);

	RETURN QUERY WITH pending_xids AS (
		SELECT
			e.xid,
			lookback_start_eid AS last_read_event_id,
			MAX(e.id) as max_event_id
		FROM events e
		WHERE
			e.id > lookback_start_eid
			AND e.id < now_event_id
			AND e.xid NOT IN (
				SELECT xid FROM reader_xid_state rxs
				WHERE rxs.reader_id = rid
			)
		GROUP BY e.xid

		UNION ALL

		SELECT rxs.xid, rxs.last_read_event_id, rxs.max_event_id
		FROM reader_xid_state rxs
		WHERE rxs.reader_id = rid AND rxs.completed_at IS NULL
	),
	next_events AS (
		SELECT e.* FROM events e
		WHERE
			e.id < now_event_id
			AND (
				e.id > lre
				OR EXISTS (
					SELECT 1 FROM pending_xids p
					WHERE e.xid = p.xid AND e.id > p.last_read_event_id
				)
			)
		ORDER BY e.id
		LIMIT chunk_size
	),
	upsert_states AS (
		INSERT INTO reader_xid_state
			(reader_id, xid, completed_at, last_read_event_id, max_event_id)
			SELECT
				rid,
				ne.xid,
				CASE WHEN
					MAX(p.max_event_id) IS NULL OR MAX(ne.id) >= MAX(p.max_event_id)
					THEN NOW()
				ELSE
					NULL
				END,
				MAX(ne.id),
				COALESCE(MAX(p.max_event_id), MAX(ne.id))
			FROM next_events ne
			LEFT JOIN pending_xids p ON p.xid = ne.xid
			GROUP BY ne.xid
		ON CONFLICT(reader_id, xid) DO UPDATE SET
			completed_at = EXCLUDED.completed_at,
			last_read_event_id = EXCLUDED.last_read_event_id
		WHERE reader_xid_state.completed_at IS NULL
	),
	update_reader AS (
		UPDATE readers r SET
			last_read_event_id
				= GREATEST((SELECT MAX(ne.id) FROM next_events ne), lre),
			last_read_at = NOW()
		WHERE id = rid
	),
	rm_old_rows AS (
		-- drop rows before the lookback interval
		DELETE FROM reader_xid_state
		WHERE reader_id = rid
		AND created_at < (NOW() - lookback_interval)
	)
	SELECT * FROM next_events;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET SEARCH_PATH TO pgmb2, public
	SECURITY INVOKER;

-- subscription management tables and functions will go here ---------------------

CREATE TABLE IF NOT EXISTS subscriptions (
	-- unique identifier for the subscription
	id VARCHAR(48) PRIMARY KEY DEFAULT gen_random_uuid()::varchar,
	reader_id VARCHAR(64) NOT NULL
		REFERENCES readers(id) ON DELETE CASCADE,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	-- A SQL expression that will be used to filter events for this subscription.
	-- The events table will be aliased as "e" in this expression. The subscription
	-- table is available as "s".
	-- Example: "e.topic = s.metadata->>'topic'",
	conditions_sql TEXT NOT NULL DEFAULT 'TRUE',
	-- if set, then this subscription will only receive changes
	-- where the diff between the row_after and row_before
	-- has at least one of the fields in the diff_only_fields array
	diff_only_fields TEXT[],
	-- if temporary, then the subscription will be removed
	-- when the connection closes
	is_temporary BOOLEAN NOT NULL DEFAULT TRUE,
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE FUNCTION get_events_for_subscriptions_by_filter(
	filter_txt TEXT, rid VARCHAR(64)
)
RETURNS TABLE(id event_id, subscription_ids varchar(64)[]) AS $$
BEGIN
	RETURN QUERY EXECUTE '
		SELECT e.id, ARRAY_AGG(s.id)
		FROM subscriptions s
		INNER JOIN tmp_events e ON (' || filter_txt || ')
		WHERE s.reader_id = $1 AND s.conditions_sql = $2
		GROUP BY e.id'
		USING rid, filter_txt;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE
SET search_path TO pgmb2, public
SECURITY INVOKER;

-- Function to send changes to match & send changes to relevant subscriptions
CREATE OR REPLACE FUNCTION read_next_events_for_subscriptions(
	rid VARCHAR(64),
	-- Specify how many events to fetch in a single batch. Useful to limit
	-- compute load, and to avoid overwhelming clients with too many events
	-- at once.
	chunk_size int DEFAULT 250
) RETURNS TABLE(
	id event_id,
	topic varchar(128),
	payload jsonb,
	metadata jsonb,
	subscription_ids varchar(64)[]
) AS $$
BEGIN
	CREATE TEMP TABLE IF NOT EXISTS tmp_events(
		id event_id NOT NULL,
		topic varchar(128) NOT NULL,
		payload jsonb NOT NULL,
		metadata jsonb,
		subscription_ids varchar(64)[] NOT NULL DEFAULT '{}'
	)
	ON COMMIT DROP;

	INSERT INTO tmp_events (id, topic, payload, metadata)
	SELECT e.id, e.topic, e.payload, e.metadata
	FROM read_next_events(rid, chunk_size) e;

	WITH relevant_sqls AS (
		SELECT conditions_sql
		FROM subscriptions s
		WHERE s.reader_id = rid
		GROUP BY conditions_sql
	),
	mapped_subs AS (
		SELECT e.id, ARRAY_AGG(u.subscription_id) AS subscription_ids
		FROM relevant_sqls s
		CROSS JOIN get_events_for_subscriptions_by_filter(s.conditions_sql, rid) e
		JOIN LATERAL unnest(e.subscription_ids) AS u(subscription_id) ON true
		GROUP BY e.id
	)
	UPDATE tmp_events te
		SET subscription_ids = ms.subscription_ids
	FROM mapped_subs ms
	WHERE te.id = ms.id;

	RETURN QUERY
		SELECT * FROM tmp_events te WHERE array_length(te.subscription_ids, 1) > 0;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public
	SECURITY INVOKER;