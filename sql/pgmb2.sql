DROP SCHEMA IF EXISTS "pgmb2" CASCADE;
CREATE SCHEMA IF NOT EXISTS "pgmb2";

SET search_path TO pgmb2, public;

-- create the configuration table for pgmb2
CREATE TYPE config_type AS ENUM(
	'plugin_version',
	'oldest_partition_interval',
	'future_partitions_to_create',
	'partition_size'
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
		('partition_size', 'hour');

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
	last_read_xid_start bigint NOT NULL DEFAULT txid_current(),
	last_read_at timestamptz
);

CREATE TABLE IF NOT EXISTS reader_xid_state (
	reader_id VARCHAR(64) NOT NULL REFERENCES readers(id) ON DELETE CASCADE,
	xid bigint NOT NULL,
	last_read_event_id event_id,
	PRIMARY KEY (reader_id, xid)
);

-- function to get active transactions on a given table
CREATE OR REPLACE FUNCTION get_min_active_tx(
	schema_name varchar(64),
	table_name varchar(64)
) RETURNS bigint AS $$
	SELECT MIN(pa.backend_xmin::text::bigint) AS xid
	FROM pg_stat_activity pa
	JOIN pg_locks pl ON pl.pid = pa.pid
	JOIN pg_class pc ON pc.oid = pl.relation
	JOIN pg_namespace pn ON pn.oid = pc.relnamespace
	WHERE pa.xact_start IS NOT NULL
		AND pa.state IN ('active', 'idle in transaction')
		AND pn.nspname = schema_name
		AND pc.relname = table_name
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
	SET search_path TO pgmb2, public;

CREATE OR REPLACE FUNCTION read_next_events(
	rid VARCHAR(64),
	chunk_size INT DEFAULT 100
) RETURNS SETOF events AS $$
DECLARE
	min_xid bigint;
	cur_active_xid bigint;
BEGIN
	-- get the reader state
	SELECT last_read_xid_start INTO min_xid FROM readers WHERE id = rid;
	-- insert missing xids into reader_xid_state
	INSERT INTO reader_xid_state(reader_id, xid)
		SELECT rid, xid
		FROM events e
		WHERE e.xid >= min_xid
		GROUP BY xid
	ON CONFLICT DO NOTHING;

	cur_active_xid := get_min_active_tx('pgmb2', 'events');

	UPDATE readers r SET
		last_read_xid_start = COALESCE(cur_active_xid, txid_current()),
		last_read_at = NOW()
	WHERE id = rid;

	RETURN QUERY WITH next_events AS (
		SELECT e.* FROM events e
		INNER JOIN reader_xid_state rla ON e.xid = rla.xid
			AND (e.id > rla.last_read_event_id OR rla.last_read_event_id IS NULL)
		-- WHERE
		-- 	-- either the event is newer than the last read event
		-- 	(
		-- 		e.id > (SELECT last_read_event_id FROM reader_state LIMIT 1)
		-- 		--AND e.xid NOT IN (SELECT pax.xid FROM prev_read_active_txs pax)
		-- 	)
		-- 	OR e.xid IN (SELECT pax.xid FROM prev_read_active_txs pax)
		-- 	-- or it belongs to a previously active transaction,
		-- 	-- from when we last read
		-- 	-- OR EXISTS (
		-- 	-- 	SELECT 1 FROM prev_read_active_txs pax
		-- 	-- 	WHERE e.xid = pax.xid
		-- 	-- 	-- AND (e.id > pax.last_read_event_id OR pax.last_read_event_id IS NULL)
		-- 	-- )
		ORDER BY e.xid, e.id
		LIMIT chunk_size
	),
	-- remove old xid states that are done with reading
	-- i.e. all xids but the last one, which returns < chunk_size events,
	-- the last xid should match the chunk_size + 1 item, otherwise we'd discard it
	xids_to_remove AS (
		SELECT ne.xid, COUNT(*) < chunk_size AS has_more
		FROM next_events ne
		WHERE ne.xid < LEAST(min_xid, COALESCE(cur_active_xid, txid_current()))
		GROUP BY ne.xid
		ORDER BY ne.xid DESC
		OFFSET 1
	),
	-- find the max event ID in the events table for each active transaction
	-- and check if we've read that, in which case we can remove it from
	-- the active transactions table
	-- read_active_txs AS (
	-- 	SELECT
	-- 		pax.xid as xid,
	-- 		(
	-- 			SELECT MAX(e.id) FROM next_events e WHERE e.xid = pax.xid
	-- 		) AS latest_read_event_id,
	-- 		(SELECT MAX(e2.id) FROM events e2 WHERE e2.xid = pax.xid) AS max_event_id,
	-- 		FALSE as is_active
	-- 	FROM prev_read_active_txs pax

	-- 	UNION ALL

	-- 	SELECT
	-- 		atx.xid as xid,
	-- 		NULL AS latest_read_event_id,
	-- 		NULL as max_event_id,
	-- 		TRUE as is_active
	-- 	FROM active_txs atx
	-- 	WHERE NOT EXISTS (
	-- 		SELECT 1 FROM reader_last_active_xids rla
	-- 		WHERE rla.reader_id = reader_id AND rla.xid = atx.xid
	-- 	)
	-- ),
	-- active_txs AS (
	-- 	SELECT * FROM get_active_txs('pgmb2', 'events')
	-- 	WHERE xid != txid_current()
	-- ),
	rm_old_xid_states AS (
		DELETE FROM reader_xid_state rla
		USING xids_to_remove rm
		WHERE rla.reader_id = rid
			AND (
				rla.xid IN (SELECT rm.xid FROM xids_to_remove rm WHERE rm.has_more = FALSE)
				OR rla.xid < min_xid
			)
		RETURNING rla.xid
	),
	update_reader_xid_states AS (
		UPDATE reader_xid_state rla SET
			last_read_event_id = ne.id
		FROM (
			SELECT ne.xid, MAX(ne.id) AS id
			FROM next_events ne
			GROUP BY ne.xid
		) ne
		WHERE rla.reader_id = rid
			AND rla.xid = ne.xid
			AND rla.xid NOT IN (SELECT xid FROM rm_old_xid_states)
	)
	-- save active transactions state
	-- new_active_txs AS (
	-- 	INSERT INTO reader_last_active_xids(reader_id, xid)
	-- 	SELECT rid, atx.xid FROM active_txs atx
	-- 	ON CONFLICT DO NOTHING
	-- )
	-- save_active_txs AS (
	-- 	MERGE INTO reader_last_active_xids rla
	-- 	USING read_active_txs atx
	-- 	ON rla.reader_id = reader_id AND rla.xid = atx.xid
	-- 	WHEN MATCHED AND (
	-- 		(
	-- 			latest_read_event_id = max_event_id
	-- 			AND max_event_id IS NOT NULL -- avoid null = null case
	-- 		)
	-- 		-- OR max_event_id IS NULL
	-- 	) AND NOT is_active
	-- 		THEN DELETE
	-- 	WHEN MATCHED AND latest_read_event_id IS NOT NULL AND NOT is_active
	-- 		THEN UPDATE SET last_read_event_id = latest_read_event_id
	-- 	WHEN NOT MATCHED
	-- 		THEN INSERT (reader_id, xid, last_read_event_id)
	-- 		VALUES (reader_id, atx.xid, atx.latest_read_event_id)
	-- )
	SELECT * FROM next_events;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SET SEARCH_PATH TO pgmb2, public;