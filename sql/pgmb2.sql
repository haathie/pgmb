DROP SCHEMA IF EXISTS "pgmb2" CASCADE;
CREATE SCHEMA IF NOT EXISTS "pgmb2";

SET search_path TO pgmb2, public;

-- create the configuration table for pgmb2
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

-- we'll create the events table next

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
	payload JSONB NOT NULL
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

-- create the readers table that will store the state of
-- their last read data
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
	PRIMARY KEY (reader_id, xid)
);

-- function to get active transactions on a given table
CREATE OR REPLACE FUNCTION get_active_txs(
	schema_name varchar(64),
	table_name varchar(64)
) RETURNS TABLE (xid xid) AS $$
	SELECT DISTINCT
		COALESCE(
			transactionid,
			(
				SELECT transactionid
				FROM pg_locks pl2
				WHERE pl2.locktype = 'transactionid' AND pl2.virtualtransaction = pl.virtualtransaction
				LIMIT 1
			)
  	) as tx_id
	FROM pg_locks pl
	JOIN pg_class pc ON pc.oid = pl.relation
	JOIN pg_namespace pn ON pn.oid = pc.relnamespace
	WHERE pn.nspname = schema_name
	AND pc.relname = table_name
	AND pl.granted;
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
	SET search_path TO pgmb2, public;

CREATE OR REPLACE FUNCTION read_next_events(
	rid VARCHAR(64),
	chunk_size INT DEFAULT 100
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

	RETURN QUERY WITH new_xids AS (
		-- insert currently active txs into reader_last_active_xids
		INSERT INTO reader_xid_state(reader_id, xid, completed_at)
		SELECT rid, new_tx.xid, NOW()
		FROM (
			SELECT e.xid, MAX(e.id) FROM events e
			WHERE e.id > lookback_start_eid AND e.id < now_event_id
			GROUP BY e.xid
		) new_tx
		ON CONFLICT DO NOTHING
		RETURNING xid
	),
	pending_xids AS (
		SELECT DISTINCT rxs.xid FROM reader_xid_state rxs
		WHERE rxs.reader_id = rid AND rxs.completed_at IS NULL
	),
	next_events AS (
		SELECT e.* FROM events e
		WHERE
			e.id < now_event_id
			AND (
				e.id > lre
				OR e.xid IN (SELECT * FROM pending_xids)
				OR e.xid IN (SELECT * FROM new_xids)
			)
		ORDER BY e.xid, e.id
		-- LIMIT chunk_size
	),
	upsert_states AS (
		INSERT INTO reader_xid_state(reader_id, xid, completed_at)
			SELECT
				rid,
				ne.xid,
				NOW()
			FROM next_events ne
			GROUP BY ne.xid
		ON CONFLICT DO NOTHING
	),
	update_reader AS (
		UPDATE readers r SET
			last_read_event_id = GREATEST(
				(SELECT MAX(ne.id) FROM next_events ne),
				lre
			),
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
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SET SEARCH_PATH TO pgmb2, public;