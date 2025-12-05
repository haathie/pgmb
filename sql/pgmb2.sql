/*
to explain inner fns: https://stackoverflow.com/a/30547418

-- Enable auto_explain for debugging
LOAD 'auto_explain';
SET auto_explain.log_nested_statements = 'on';
SET auto_explain.log_min_duration = 0;
SET client_min_messages TO log;
*/

-- DROP SCHEMA IF EXISTS pgmb2 CASCADE;
CREATE SCHEMA IF NOT EXISTS "pgmb2";

SET search_path TO pgmb2, public;

-- create the configuration table for pgmb2 ----------------

CREATE TYPE config_type AS ENUM(
	'plugin_version',
	'partition_retention_period',
	'future_partitions_to_create',
	'partition_interval',
	'poll_chunk_size'
);

CREATE TABLE IF NOT EXISTS config(
	-- unique identifier for the subscription config
	id config_type PRIMARY KEY,
	value TEXT
);

CREATE OR REPLACE FUNCTION get_config_value(
	config_id config_type
) RETURNS TEXT AS $$
	SELECT value FROM config WHERE id = config_id
$$ LANGUAGE sql STRICT STABLE PARALLEL SAFE SET SEARCH_PATH TO pgmb2, public;

INSERT INTO config(id, value)
	VALUES
		('plugin_version', '0.1.0'),
		('partition_retention_period', '60 minutes'),
		('future_partitions_to_create', '12'),
		('partition_interval', '30 minutes'),
		('poll_chunk_size', '10000');

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
CREATE OR REPLACE FUNCTION create_event_id(ts timestamptz, rand bigint)
RETURNS event_id AS $$
SELECT substr(
	-- ensure we're always 28 characters long by right-padding with '0's
	'pm'
	-- we'll give 13 hex characters for microsecond timestamp
	|| lpad(to_hex((extract(epoch from ts) * 1000000)::bigint), 13,	'0')
	-- xids are 32 bits, so 8 hex characters
	-- || lpad(to_hex(tx_id), 8, '0')
	-- fill remaining with randomness
	|| rpad(to_hex(rand), 9, '0'),
	1,
	24
)
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb2, public;

CREATE OR REPLACE FUNCTION create_event_id_default()
RETURNS event_id AS $$
	SELECT create_event_id(clock_timestamp(), create_random_bigint())
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb2, public;

CREATE TABLE IF NOT EXISTS events(
	id event_id PRIMARY KEY DEFAULT create_event_id_default(),
	topic VARCHAR(255) NOT NULL,
	payload JSONB NOT NULL,
	metadata JSONB,
	-- if an event is directed to a specific subscription,
	-- this field will be set to that subscription's ID
	subscription_id VARCHAR(48)
) PARTITION BY RANGE (id);

CREATE UNLOGGED TABLE IF NOT EXISTS unread_events (
	event_id event_id PRIMARY KEY
);

-- statement level trigger to insert into unread_events table
CREATE OR REPLACE FUNCTION mark_events_as_unread()
RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO unread_events(event_id)
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

CREATE OR REPLACE FUNCTION get_time_partition_name(
	table_id regclass,
	ts timestamptz
) RETURNS TEXT AS $$
	SELECT table_id || '_' || to_char(ts, 'YYYYMMDDHH24MI')
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

-- Partition maintenance function for events table. Creates partitions for
-- the current and next interval. Deletes partitions that are older than the
-- configured time interval.
-- Exact partition size and oldest partition interval can be configured
-- using the "subscriptions_config" table.
CREATE OR REPLACE FUNCTION maintain_time_partitions_using_event_id(
	table_id regclass,
	partition_interval INTERVAL,
	future_partitions_to_create INT,
	retention_period INTERVAL,
	additional_sql TEXT DEFAULT NULL,
	current_ts timestamptz DEFAULT NOW()
)
RETURNS void AS $$
DECLARE
	ts_trunc timestamptz := date_bin(partition_interval, current_ts, '2000-1-1');
	oldest_partition_name text := pgmb2
		.get_time_partition_name(table_id, ts_trunc - retention_period);
	p_info RECORD;
	lock_key CONSTANT BIGINT :=
		hashtext('pgmb2.maintain_tp.' || table_id::text);
BEGIN
	IF NOT pg_try_advisory_xact_lock(lock_key) THEN
		-- another process is already maintaining partitions for this table
		RETURN;
	END IF;

	-- Ensure current and next hour partitions exist
	FOR i IN 0..(future_partitions_to_create-1) LOOP
		DECLARE
			target_ts timestamptz := ts_trunc + (i * partition_interval);
			pt_name TEXT := pgmb2.get_time_partition_name(table_id, target_ts);
		BEGIN
			IF pt_name < oldest_partition_name THEN
				RAISE EXCEPTION 'pt_name(%) < op(%); rp=%, ts=%', pt_name, oldest_partition_name, (ts_trunc - retention_period), target_ts;
			END IF;
			-- check if partition already exists
			IF EXISTS (
				SELECT 1
				FROM pg_catalog.pg_inherits
				WHERE inhparent = table_id
					AND inhrelid::regclass::text = pt_name
			) THEN
				CONTINUE;
			END IF;

			RAISE NOTICE 'creating partition %', pt_name;

			EXECUTE format(
				'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
				pt_name,
				table_id,
				pgmb2.create_event_id(target_ts, 0),
				-- fill with max possible tx id
				pgmb2.create_event_id(target_ts + partition_interval, 0)
			);

			IF additional_sql IS NOT NULL THEN
				EXECUTE REPLACE(additional_sql, '$1', pt_name);
			END IF;
		END;
	END LOOP;

	-- Drop old partitions
	FOR p_info IN (
		SELECT inhrelid::regclass AS child
		FROM pg_catalog.pg_inherits
		WHERE inhparent = table_id
			AND inhrelid::regclass::text < oldest_partition_name
	) LOOP
		EXECUTE format('DROP TABLE %I', p_info.child);
	END LOOP;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_current_partition(
	table_id regclass,
	current_ts timestamptz DEFAULT NOW()
) RETURNS regclass AS $$
	SELECT inhrelid::regclass
	FROM pg_catalog.pg_inherits
	WHERE inhparent = table_id
		AND inhrelid::regclass::text <= pgmb2.get_time_partition_name(table_id, current_ts)
	ORDER BY inhrelid DESC
	LIMIT 1
$$ LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER;

-- subscriptions table and related functions ----------------

CREATE TYPE subscription_type AS ENUM(
	'http',
	'webhook',
	'custom'
);

CREATE DOMAIN subscription_id AS VARCHAR(24);

-- reader, subscription management tables and functions will go here ----------------
CREATE TABLE IF NOT EXISTS subscriptions (
	-- unique identifier for the subscription
	id subscription_id PRIMARY KEY DEFAULT gen_random_uuid()::varchar,
	-- define how the subscription is grouped. subscriptions belonging
	-- to the same group can be read in one batch.
	-- Leave NULL to only allow independent fetching
	group_id VARCHAR(48),
	created_at TIMESTAMPTZ DEFAULT NOW(),
	-- A SQL expression that will be used to filter events for this subscription.
	-- The events table will be aliased as "e" in this expression. The subscription
	-- table is available as "s".
	-- Example: "e.topic = s.metadata->>'topic'",
	conditions_sql TEXT NOT NULL DEFAULT 'TRUE',
	-- if temporary, then the subscription will be removed on reboot of
	-- the reader its attached to.
	type subscription_type NOT NULL DEFAULT 'custom',
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE MATERIALIZED VIEW IF NOT EXISTS subscription_cond_sqls AS (
	SELECT DISTINCT conditions_sql FROM subscriptions
	ORDER BY conditions_sql
);

CREATE UNIQUE INDEX IF NOT EXISTS
	subscription_cond_sqls_idx ON subscription_cond_sqls(conditions_sql);

CREATE UNLOGGED TABLE IF NOT EXISTS subscription_events (
	fetch_id VARCHAR(48),
	event_id event_id,
	subscription_id subscription_id,
	PRIMARY KEY (fetch_id, event_id, subscription_id)
);

CREATE OR REPLACE FUNCTION create_subscription_id(type subscription_type)
RETURNS subscription_id AS $$
	SELECT substring(type::text, 1, 2) || substring(
		create_event_id(NOW(), create_random_bigint())
		FROM 3
	);
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb2, public;

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

CREATE OR REPLACE FUNCTION poll_for_events_tmpl()
RETURNS INT AS $body$
DECLARE
	read_ids event_id[];
	max_id event_id;
	min_id event_id;

	chunk_size INT := get_config_value('poll_chunk_size')::INT;

	inserted_rows integer;

	lock_key CONSTANT BIGINT :=
		hashtext('pgmb2.poll_for_events');
BEGIN
	-- acquire lock to prevent concurrent polling, maintenance
	IF NOT pg_try_advisory_xact_lock(lock_key) THEN
		RETURN 0;
	END IF;

	WITH to_delete AS (
		SELECT td.event_id
		FROM unread_events td
		WHERE td.event_id < create_event_id(NOW(), 0)
		FOR UPDATE SKIP LOCKED
		-- ORDER BY td.event_id
		LIMIT chunk_size
	),
	deleted AS (
		DELETE FROM unread_events re
		USING to_delete td
		WHERE re.event_id = td.event_id
	)
	SELECT
		MAX(event_id),
		MIN(event_id),
		ARRAY_AGG(event_id)
	INTO max_id, min_id, read_ids
	FROM to_delete;

	IF max_id IS NULL THEN
		RETURN 0;
	END IF;

	WITH read_events AS (
		SELECT e.*
		FROM events e
		INNER JOIN unnest(read_ids) r(id) ON e.id = r.id
		WHERE e.id <= max_id AND e.id >= min_id
	)
	INSERT INTO subscription_events(fetch_id, subscription_id, event_id)
	SELECT
		CASE WHEN s.group_id IS NOT NULL THEN s.group_id ELSE s.id END,
		CASE WHEN s.group_id IS NOT NULL THEN s.id ELSE '' END,
		e.id
	FROM read_events e
	INNER JOIN subscriptions s ON
		s.id = e.subscription_id
		OR (
			e.subscription_id IS NULL
			AND (
				-- Do not edit this line directly. Will be replaced
				-- in the prepared function.
				TRUE -- CONDITIONS_SQL_PLACEHOLDER --
			)
		);

	GET DIAGNOSTICS inserted_rows = ROW_COUNT;

	-- return total inserted events
	RETURN inserted_rows;
END;
$body$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY INVOKER;

CREATE OR REPLACE FUNCTION prepare_poll_for_events_fn(
	sql_statements TEXT[]
) RETURNS VOID AS $$
DECLARE
	tmpl_proc_name constant TEXT :=
		'poll_for_events_tmpl';
	tmpl_proc_placeholder constant TEXT :=
		'TRUE -- CONDITIONS_SQL_PLACEHOLDER --';
	condition_sql TEXT;
	proc_src TEXT;
BEGIN
	IF sql_statements = '{}' THEN
		-- no subscriptions, so just use 'FALSE' to avoid any matches
		sql_statements := ARRAY['FALSE'];
	END IF;

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
	condition_sql := FORMAT('/* updated at %s */', NOW()) || condition_sql;

	-- fetch the source of the template procedure
	select pg_get_functiondef(oid) INTO proc_src
	from pg_proc where proname = tmpl_proc_name;
	IF proc_src IS NULL THEN
		RAISE EXCEPTION 'Template procedure % not found', tmpl_proc_name;
	END IF;

	-- replace the placeholder with the actual condition SQL
	proc_src := REPLACE(proc_src, tmpl_proc_placeholder, condition_sql);
	proc_src := REPLACE(proc_src, tmpl_proc_name, 'poll_for_events');

	EXECUTE proc_src;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY DEFINER;

SELECT prepare_poll_for_events_fn(ARRAY['true']);

-- we'll prepare the subscription read statement whenever subscriptions are created/updated/deleted
CREATE OR REPLACE FUNCTION refresh_subscription_read_statements()
RETURNS TRIGGER AS $$
DECLARE
	needs_refresh BOOLEAN := FALSE;
	old_conditions_sql TEXT[];
	conditions_sql TEXT[];

	lk_name CONSTANT bigint :=
		hashtext('pgmb2.refresh_subscription_read_statements');
BEGIN
	old_conditions_sql := ARRAY(SELECT * FROM subscription_cond_sqls);

	REFRESH MATERIALIZED VIEW CONCURRENTLY subscription_cond_sqls;

	conditions_sql := ARRAY(SELECT * FROM subscription_cond_sqls);

	IF conditions_sql = old_conditions_sql THEN
		RETURN NULL;
	END IF;

	PERFORM prepare_poll_for_events_fn(conditions_sql);
	RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public
	SECURITY INVOKER;

CREATE TRIGGER refresh_subscription_read_statements_trigger
AFTER INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_subscription_read_statements();

CREATE OR REPLACE FUNCTION read_events(
	event_ids event_id[]
) RETURNS SETOF events AS $$
DECLARE
	max_id event_id;
	min_id event_id;
BEGIN
	IF array_length(event_ids, 1) = 0 THEN
		RETURN;
	END IF;

	-- get min and max ids
	SELECT
		MAX(eid),
		MIN(eid)
	INTO max_id, min_id
	FROM unnest(event_ids) AS u(eid);

	RETURN QUERY
		SELECT e.*
		FROM events e
		INNER JOIN unnest(event_ids) AS u(eid) ON e.id = u.eid
		WHERE e.id <= max_id AND e.id >= min_id;
END;
$$ LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
SET search_path TO pgmb2, public;

CREATE OR REPLACE FUNCTION read_next_events(
	fid VARCHAR(48),
	chunk_size INT DEFAULT 100
) RETURNS TABLE(
	id event_id,
	topic VARCHAR(255),
	payload JSONB,
	metadata JSONB,
	subscription_ids subscription_id[]
) AS $$
	WITH next_events AS (
		SELECT
			se.event_id,
			CASE WHEN se.subscription_id = ''
				THEN se.fetch_id
				ELSE se.subscription_id END
			AS subscription_id
		FROM subscription_events se
		WHERE se.fetch_id = fid
			AND se.event_id < create_event_id(NOW(), 0)
		-- ORDER BY se.event_id
		LIMIT chunk_size
		FOR UPDATE SKIP LOCKED
	),
	dels AS (
		DELETE FROM subscription_events se
		USING next_events se2
		WHERE se.fetch_id = fid
		AND se.event_id = se2.event_id
	),
	next_events_grp AS (
		SELECT ne.event_id, ARRAY_AGG(ne.subscription_id) AS subscription_ids
		FROM next_events ne
		GROUP BY ne.event_id
		ORDER BY ne.event_id
	)
	SELECT
		e.id,
		e.topic,
		e.payload,
		e.metadata,
		ne.subscription_ids
	FROM read_events(ARRAY(SELECT ne.event_id FROM next_events_grp ne)) e
	INNER JOIN next_events_grp ne ON ne.event_id = e.id
$$ LANGUAGE sql VOLATILE PARALLEL UNSAFE
SET search_path TO pgmb2, public
SECURITY INVOKER;

-- Function to re-enqueue events for a specific subscription
CREATE OR REPLACE FUNCTION reenqueue_events_for_subscription(
	event_ids event_id[],
	sub_id VARCHAR(48),
	_offset INTERVAL DEFAULT '1 second'
) RETURNS SETOF event_id AS $$
	INSERT INTO events(id, topic, payload, metadata, subscription_id)
	SELECT
		create_event_id(NOW() + _offset, create_random_bigint()),
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

CREATE OR REPLACE FUNCTION maintain_events_table(
	current_ts timestamptz DEFAULT NOW()
)
RETURNS VOID AS $$
BEGIN
	PERFORM maintain_time_partitions_using_event_id(
		'pgmb2.events'::regclass,
		partition_interval := get_config_value('partition_interval')::INTERVAL,
		future_partitions_to_create := get_config_value('future_partitions_to_create')::INT,
		retention_period := get_config_value('partition_retention_period')::INTERVAL,
		-- turn off autovacuum on the events table, since we're not
		-- going to be updating/deleting rows from it.
		-- Also set fillfactor to 100 since we're only inserting.
		additional_sql := 'ALTER TABLE $1 SET(
			fillfactor = 100,
			autovacuum_enabled = false,
			toast.autovacuum_enabled = false
		);',
		current_ts := current_ts
	);
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
SET search_path TO pgmb2, public;

SELECT maintain_events_table();

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

CREATE OR REPLACE FUNCTION serialise_record_for_event(
	tabl oid,
	op TEXT,
	record RECORD,
	serialised OUT JSONB,
	emit OUT BOOLEAN
) AS $$
BEGIN
	serialised := to_jsonb(record);
	emit := TRUE;
	RETURN;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
	SECURITY INVOKER;

-- Trigger that pushes changes to the events table
CREATE OR REPLACE FUNCTION push_table_event()
RETURNS TRIGGER AS $$
DECLARE
	start_num BIGINT = create_random_bigint();
BEGIN
	IF TG_OP = 'INSERT' THEN
		INSERT INTO events(id, topic, payload)
		SELECT
			create_event_id(clock_timestamp(), rand := start_num + row_number() OVER ()),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			jsonb_strip_nulls(s.data)
		FROM NEW n
		CROSS JOIN LATERAL
			serialise_record_for_event(TG_RELID, TG_OP, n) AS s(data, emit)
		WHERE s.emit;
	ELSIF TG_OP = 'DELETE' THEN
		INSERT INTO events(id, topic, payload)
		SELECT
			create_event_id(clock_timestamp(), rand := start_num + row_number() OVER ()),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			jsonb_strip_nulls(to_jsonb(s.data))
		FROM OLD o
		CROSS JOIN LATERAL
			serialise_record_for_event(TG_RELID, TG_OP, o) AS s(data, emit)
		WHERE s.emit;
	ELSIF TG_OP = 'UPDATE' THEN
		-- For updates, we can send both old and new data
		INSERT INTO events(id, topic, payload, metadata)
		SELECT
			create_event_id(clock_timestamp(), rand := start_num + n.rn),
			create_topic(TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP),
			jsonb_strip_nulls(jsonb_diff(n.data, o.data)),
			jsonb_build_object('old', jsonb_strip_nulls(o.data))
		FROM (
			SELECT s.data, s.emit, row_number() OVER () AS rn
			FROM NEW n
			CROSS JOIN LATERAL
				serialise_record_for_event(TG_RELID, TG_OP, n) AS s(data, emit)
		) AS n
		INNER JOIN (
			SELECT s.data, row_number() OVER () AS rn FROM OLD o
			CROSS JOIN LATERAL
				serialise_record_for_event(TG_RELID, TG_OP, o) AS s(data, emit)
		) AS o ON n.rn = o.rn
		-- ignore rows where data didn't change
		WHERE n.data IS DISTINCT FROM o.data AND n.emit;
	END IF;

	RETURN NULL;
END
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb2, public;

-- Pushes table mutations to the events table. I.e. makes the table subscribable.
-- and creates triggers to push changes to the events table.
CREATE OR REPLACE FUNCTION push_table_mutations(
	tbl regclass,
	insert BOOLEAN DEFAULT TRUE,
	delete BOOLEAN DEFAULT TRUE,
	update BOOLEAN DEFAULT TRUE
)
RETURNS VOID AS $$
BEGIN
	IF insert THEN
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
	END IF;

	IF delete THEN
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
	END IF;

	IF update THEN
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
	END IF;
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
