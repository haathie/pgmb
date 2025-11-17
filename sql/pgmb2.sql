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
		('oldest_partition_interval', '1 minute'),
		('future_partitions_to_create', '12'),
		('partition_size', 'minute');

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
	payload JSONB NOT NULL,
	metadata JSONB,
	-- if an event is directed to a specific subscription,
	-- this field will be set to that subscription's ID
	subscription_id VARCHAR(48)
) PARTITION BY RANGE (id);

CREATE TABLE unread_events(
	id event_id
);

-- statement level trigger to insert into unread_events table
CREATE OR REPLACE FUNCTION mark_events_as_unread()
RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO unread_events(id)
	SELECT e.id FROM NEW e;
	RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public;

CREATE TRIGGER mark_events_as_unread_trigger
AFTER INSERT ON events
REFERENCING NEW TABLE AS NEW
FOR EACH STATEMENT
EXECUTE FUNCTION mark_events_as_unread();

CREATE OR REPLACE FUNCTION get_event_partition_name(
	table_name TEXT,
	ts timestamptz
) RETURNS TEXT AS $$
	SELECT table_name || '_' || to_char(ts, 'YYYYMMDDHHMI24')
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

			-- turn off autovacuum on the events table, since we're not
			-- going to be updating/deleting rows from it.
			-- Also set fillfactor to 100 since we're only inserting.
			EXECUTE FORMAT('ALTER TABLE %I.%I SET(
				fillfactor = 100,
				autovacuum_enabled = false,
				toast.autovacuum_enabled = false
			);', schema_name, get_event_partition_name(table_name, target_ts));
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

-- reader, subscription management tables and functions will go here ----------------

CREATE TABLE IF NOT EXISTS readers (
	id VARCHAR(64) PRIMARY KEY,
	created_at timestamptz NOT NULL DEFAULT NOW(),
	last_read_event_id event_id NOT NULL DEFAULT create_event_id(),
	last_read_at timestamptz
);

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
	-- if temporary, then the subscription will be removed on reboot of
	-- the reader its attached to.
	is_temporary BOOLEAN NOT NULL DEFAULT TRUE,
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS subscription_unread_events (
	event_id event_id NOT NULL,
	reader_id VARCHAR(64) NOT NULL
		REFERENCES readers(id) ON DELETE CASCADE,
	subscription_ids VARCHAR(48)[] NOT NULL,
	read_at timestamptz,
	PRIMARY KEY(reader_id, event_id)
);

CREATE OR REPLACE FUNCTION distribute_events_to_subscriptions_tmpl(
	-- Specify how many events to fetch in a single batch. Useful to limit
	-- compute load, and to avoid overwhelming clients with too many events
	-- at once.
	chunk_size int DEFAULT 250
)
RETURNS VOID AS $body$
DECLARE
	now_event_id event_id := create_event_id(NOW());

	max_event_id event_id;
	min_event_id event_id;
	event_ids event_id[];
BEGIN
-- fetch all unread events up to now_event_id
	WITH new_ids AS (
		DELETE FROM unread_events ue
		USING (
			SELECT ue.id FROM unread_events ue
			WHERE ue.id < now_event_id
			ORDER BY ue.id
			LIMIT chunk_size
		) uev
		WHERE ue.id = uev.id
		RETURNING ue.id
	)
	SELECT ARRAY_AGG(ue.id), MAX(ue.id), MIN(ue.id)
		INTO event_ids, max_event_id, min_event_id
	FROM new_ids ue;

	IF max_event_id IS NULL THEN
		RETURN;
	END IF;

	INSERT INTO subscription_unread_events(reader_id, event_id, subscription_ids)
	SELECT s.reader_id, e.id, ARRAY_AGG(s.id)
	FROM events e
	INNER JOIN subscriptions s ON
		s.id = e.subscription_id
		OR (
			e.subscription_id IS NULL
			AND (
				TRUE -- CONDITIONS_SQL_PLACEHOLDER --
			)
		)
	INNER JOIN unnest(event_ids) AS u(id) ON e.id = u.id
	WHERE e.id <= max_event_id AND e.id >= min_event_id
	GROUP BY s.reader_id, e.id;
END;
$body$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY INVOKER;

CREATE OR REPLACE FUNCTION prepare_distribute_events_to_subscriptions_fn(
	sql_statements TEXT[]
) RETURNS VOID AS $$
DECLARE
	tmpl_proc_name constant TEXT :=
		'distribute_events_to_subscriptions_tmpl';
	tmpl_proc_placeholder constant TEXT :=
		'TRUE -- CONDITIONS_SQL_PLACEHOLDER --';
	condition_sql TEXT;
	proc_src TEXT;
BEGIN
	condition_sql := FORMAT(
		'('
		|| array_to_string(
			ARRAY(
				SELECT
					'(' || stmt || ') AND s.conditions_sql = %L'
				FROM unnest(sql_statements) AS arr(stmt)
			),
			') OR ('
		)
		|| ')',
		VARIADIC sql_statements
	);

	-- fetch the source of the template procedure
	select pg_get_functiondef(oid) INTO proc_src
	from pg_proc where proname = tmpl_proc_name;
	IF proc_src IS NULL THEN
		RAISE EXCEPTION 'Template procedure % not found', tmpl_proc_name;
	END IF;

	-- replace the placeholder with the actual condition SQL
	proc_src := REPLACE(proc_src, tmpl_proc_placeholder, condition_sql);
	proc_src := REPLACE(
		proc_src,
		tmpl_proc_name,
		'distribute_events_to_subscriptions'
	);

	EXECUTE proc_src;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY DEFINER;

SELECT prepare_distribute_events_to_subscriptions_fn(ARRAY['true']);

-- we'll prepare the subscription read statement whenever subscriptions are created/updated/deleted
CREATE OR REPLACE FUNCTION refresh_subscription_read_statements()
RETURNS TRIGGER AS $$
BEGIN
	PERFORM prepare_distribute_events_to_subscriptions_fn(
		ARRAY(SELECT DISTINCT conditions_sql FROM subscriptions)
	);
	RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public
	SECURITY INVOKER;

CREATE TRIGGER refresh_subscription_read_statements_trigger
AFTER INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_subscription_read_statements();

-- we'll also validate the conditions_sql on insert/update
CREATE OR REPLACE FUNCTION validate_subscription_conditions_sql()
RETURNS TRIGGER AS $$
BEGIN
	EXECUTE 'SELECT * FROM jsonb_populate_recordset(NULL::pgmb2.events, ''[]'') e
		INNER JOIN jsonb_populate_recordset(NULL::pgmb2.subscriptions, ''[{}]'') s
		ON ' || NEW.conditions_sql;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public
	SECURITY INVOKER;

CREATE TRIGGER validate_subscription_conditions_sql_trigger
BEFORE INSERT OR UPDATE ON subscriptions
FOR EACH ROW
EXECUTE FUNCTION validate_subscription_conditions_sql();

-- Function to re-enqueue events for a specific subscription
CREATE OR REPLACE FUNCTION reenqueue_events_for_subscription(
	event_ids event_id[],
	sub_id VARCHAR(48),
	_offset INTERVAL DEFAULT '1 second'
) RETURNS SETOF event_id AS $$
	INSERT INTO events(id, topic, payload, metadata, subscription_id)
	SELECT
		create_event_id(NOW() + _offset),
		e.topic,
		e.payload,
		e.metadata || jsonb_build_object(
			'reenqueued_at', NOW(),
			'retries', COALESCE((e.metadata->>'retries')::int, 0) + 1,
			'original_event_id', COALESCE(e.metadata->>'original_event_id', e.id)
		),
		sub_id
	FROM events e
	INNER JOIN unnest(event_ids) AS u(eid) ON e.id = u.eid
	RETURNING id;
$$ LANGUAGE sql VOLATILE PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY INVOKER;

-- triggers to add events for specific tables ---------------------------

-- Function to create a topic string for subscriptions.
-- Eg. "public" "contacts" "INSERT" -> "public.contacts.INSERT"
CREATE OR REPLACE FUNCTION create_topic(
	schema_name name,
	table_name name,
	kind varchar(16)
) RETURNS varchar(255) AS $$
	SELECT lower(schema_name || '.' || table_name || '.' || kind)
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;


-- Creates a function to compute the difference between two JSONB objects
-- Treats 'null' values, and non-existent keys as equal
-- Eg. jsonb_diff('{"a": 1, "b": 2, "c": null}', '{"a": 1, "b": null}') = '{"b": 2}'
CREATE OR REPLACE FUNCTION jsonb_diff(a jsonb, b jsonb)
RETURNS jsonb AS $$
SELECT jsonb_object_agg(key, value) FROM (
	SELECT key, value FROM jsonb_each(a) WHERE value != 'null'::jsonb
  EXCEPT
  SELECT key, value FROM jsonb_each(b) WHERE value != 'null'::jsonb
)
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

-- Trigger that pushes changes to the events table
CREATE OR REPLACE FUNCTION push_table_event()
RETURNS TRIGGER AS $$
DECLARE
	start_num BIGINT = create_random_bigint();
BEGIN
	IF TG_OP = 'INSERT' THEN
		INSERT INTO events(id, topic, payload)
		SELECT
			create_event_id(rand := start_num + row_number() OVER ()),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			to_jsonb(n)
		FROM NEW n;
	ELSIF TG_OP = 'DELETE' THEN
		INSERT INTO events(id, topic, payload)
		SELECT
			create_event_id(rand := start_num + row_number() OVER ()),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			to_jsonb(o)
		FROM OLD o;
	ELSIF TG_OP = 'UPDATE' THEN
		-- For updates, we can send both old and new data
		INSERT INTO events(id, topic, payload, metadata)
		SELECT
			create_event_id(rand := start_num + n.rn),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			n.data,
			jsonb_build_object('diff', jsonb_diff(n.data, o.data), 'old', o.data)			
		FROM (
			SELECT to_jsonb(n) as data, row_number() OVER () AS rn FROM NEW n
		) AS n
		INNER JOIN (
			SELECT to_jsonb(o) as data, row_number() OVER () AS rn FROM OLD o
		) AS o ON n.rn = o.rn
		-- ignore rows where data didn't change
		WHERE n.data IS DISTINCT FROM o.data;
	END IF;

	RETURN NULL;
END
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public;

-- Pushes table mutations to the events table. I.e. makes the table subscribable.
-- and creates triggers to push changes to the events table.
CREATE OR REPLACE FUNCTION push_table_mutations(
	tbl regclass
)
RETURNS VOID AS $$
BEGIN
	-- Create a trigger to push changes to the subscriptions queue
	BEGIN
		EXECUTE 'CREATE TRIGGER
			post_insert_event
			AFTER INSERT ON ' || tbl::varchar || '
			REFERENCING NEW TABLE AS NEW
			FOR EACH STATEMENT
			EXECUTE FUNCTION push_table_event();';
	EXCEPTION
		WHEN duplicate_object THEN
			NULL;
  END;
	BEGIN
		EXECUTE 'CREATE TRIGGER
			post_delete_event
			AFTER DELETE ON ' || tbl::varchar || '
			REFERENCING OLD TABLE AS OLD
			FOR EACH STATEMENT
			EXECUTE FUNCTION push_table_event();';
	EXCEPTION
		WHEN duplicate_object THEN
			NULL;
  END;
	BEGIN
		EXECUTE 'CREATE TRIGGER
			post_update_event
			AFTER UPDATE ON ' || tbl::varchar || '
			REFERENCING OLD TABLE AS OLD
			NEW TABLE AS NEW
			FOR EACH STATEMENT
			EXECUTE FUNCTION push_table_event();';
	EXCEPTION
		WHEN duplicate_object THEN
			NULL;
  END;
END
$$ LANGUAGE plpgsql SECURITY DEFINER
	VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public;

-- Stops the table from being subscribable.
-- I.e removes the triggers that push changes to the events table.
CREATE OR REPLACE FUNCTION stop_table_mutations_push(
	tbl regclass
) RETURNS VOID AS $$
BEGIN
	-- Remove the triggers for the table
	EXECUTE 'DROP TRIGGER IF EXISTS post_insert_event ON ' || tbl::varchar || ';';
	EXECUTE 'DROP TRIGGER IF EXISTS post_delete_event ON ' || tbl::varchar || ';';
	EXECUTE 'DROP TRIGGER IF EXISTS post_update_event ON ' || tbl::varchar || ';';
END
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE
	SET search_path TO pgmb2, public;