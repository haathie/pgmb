/*
to explain inner fns: https://stackoverflow.com/a/30547418

-- Enable auto_explain for debugging
LOAD 'auto_explain';
SET auto_explain.log_nested_statements = 'on';
SET auto_explain.log_min_duration = 0;
SET client_min_messages TO log;
*/

CREATE SCHEMA IF NOT EXISTS "pgmb";

SET search_path TO pgmb;

-- create the configuration table for pgmb ----------------

CREATE TYPE config_type AS ENUM(
	'plugin_version',
	-- how long to retain old partitions?
	-- partitions older than this interval will be deleted
	'partition_retention_period',
	-- how far into the future to create partitions
	'future_intervals_to_create',
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
$$ LANGUAGE sql STRICT STABLE PARALLEL SAFE SET SEARCH_PATH TO pgmb;

INSERT INTO config(id, value)
	VALUES
		('plugin_version', '0.2.0'),
		('partition_retention_period', '60 minutes'),
		('future_intervals_to_create', '120 minutes'),
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
-- 1. 'pm' prefix
-- 2. 13-character hex representation of the timestamp in microseconds
-- 3. remaining random
CREATE OR REPLACE FUNCTION create_event_id(ts timestamptz, rand bigint)
RETURNS event_id AS $$
SELECT substr(
	-- ensure we're always 24 characters long by right-padding with '0's
	'pm'
	-- we'll give 13 hex characters for microsecond timestamp
	|| lpad(to_hex((extract(epoch from ts) * 1000000)::bigint), 13,	'0')
	-- fill remaining with randomness
	|| rpad(to_hex(rand), 9, '0'),
	1,
	24
)
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb;

CREATE OR REPLACE FUNCTION create_event_id_default()
RETURNS event_id AS $$
	SELECT create_event_id(clock_timestamp(), create_random_bigint())
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb;

 -- fn to extract the date from a message ID.
CREATE OR REPLACE FUNCTION extract_date_from_event_id(id event_id)
RETURNS TIMESTAMPTZ AS $$
	SELECT to_timestamp(('0x' || substr(id, 3, 13))::numeric / 1000000)
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER
 SET search_path TO pgmb;

CREATE DOMAIN subscription_id AS VARCHAR(24);

CREATE TABLE IF NOT EXISTS events(
	id event_id PRIMARY KEY DEFAULT create_event_id_default(),
	topic VARCHAR(255) NOT NULL,
	payload JSONB NOT NULL,
	metadata JSONB,
	-- if an event is directed to a specific subscription,
	-- this field will be set to that subscription's ID
	subscription_id subscription_id
) PARTITION BY RANGE (id);

CREATE UNLOGGED TABLE IF NOT EXISTS unread_events (
	event_id event_id PRIMARY KEY
) WITH (
	-- tune autovacuum for high insert & delete rates
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 5000,
  autovacuum_analyze_scale_factor = 0.005,
  autovacuum_analyze_threshold = 1000,
  autovacuum_vacuum_cost_delay = 0
);


-- statement level trigger to insert new events into unread_events.
-- The "poll_for_events" function will read from this table, and
-- dispatch events to subscriptions.
CREATE OR REPLACE FUNCTION mark_events_as_unread()
RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO unread_events(event_id)
	SELECT e.id FROM NEW e;
	RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb;

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

-- finds the series of contiguous partitions and their bounds
CREATE OR REPLACE FUNCTION get_partitions_and_bounds(
	table_id regclass
) RETURNS TABLE(
	lower_bound event_id,
	upper_bound event_id,
	partition_ids oid[]
) AS $$
	WITH partitions AS (
		select
			pc.oid,
			REGEXP_MATCH(
				pg_get_expr(pc.relpartbound, pc.oid),
				'^FOR VALUES FROM \(''(.*)''\) TO \(''(.*)''\)$'
			) AS bounds
		from pg_inherits pts
		inner join pg_class pc on pc.oid = pts.inhrelid
		where pts.inhparent = table_id
	),
	-- from: https://dba.stackexchange.com/a/101010
	ordered_intervals AS (
		SELECT
			*,
			(LAG(bounds[2]) OVER (ORDER BY bounds[1]) < bounds[1] OR NULL) as step
		FROM partitions
	),
	grouped_intervals AS (
		select *, count(step) over (order by bounds[1]) as grp
		from ordered_intervals
	)
	select
		MIN(bounds[1]),
		MAX(bounds[2]),
		array_agg(oid)
	FROM grouped_intervals
	GROUP BY grp;
$$ LANGUAGE sql STABLE PARALLEL SAFE SECURITY INVOKER
 SET search_path TO pgmb;

-- Partition maintenance function for events table. Creates partitions for
-- the current and next interval. Deletes partitions that are older than the
-- configured time interval.
-- Exact partition size and oldest partition interval can be configured
-- using the "subscriptions_config" table.
CREATE OR REPLACE FUNCTION maintain_time_partitions_using_event_id(
	table_id regclass,
	partition_interval INTERVAL,
	future_interval INTERVAL,
	retention_period INTERVAL,
	additional_sql TEXT DEFAULT NULL,
	current_ts timestamptz DEFAULT NOW()
)
RETURNS void AS $$
DECLARE
	ts_trunc timestamptz := date_bin(partition_interval, current_ts, '2000-1-1');
	oldest_pt_to_keep text := pgmb
		.get_time_partition_name(table_id, ts_trunc - retention_period);
	p_info RECORD;
	lock_key CONSTANT BIGINT :=
		hashtext('pgmb.maintain_tp.' || table_id::text);
	ranges_to_create tstzrange[];
	cur_range tstzrange;
BEGIN
	ASSERT partition_interval >= interval '1 minute',
		'partition_interval must be at least 1 minute';
	ASSERT future_interval >= partition_interval,
		'future_interval must be at least as large as partition_interval';

	IF NOT pg_try_advisory_xact_lock(lock_key) THEN
		-- another process is already maintaining partitions for this table
		RETURN;
	END IF;

	-- find all intervals we need to create partitions for
	WITH existing_part_ranges AS (
		SELECT
			tstzrange(
	  		extract_date_from_event_id(lower_bound),
	     	extract_date_from_event_id(upper_bound),
	      '[]'
	    ) as range
    FROM pgmb.get_partitions_and_bounds(table_id)
	),
	future_tzs AS (
		SELECT
			tstzrange(dt, dt + partition_interval, '[]') AS range
		FROM generate_series(
			ts_trunc,
			ts_trunc + future_interval,
			partition_interval
		) AS gs(dt)
	),
	diffs AS (
		SELECT
			CASE WHEN epr.range IS NOT NULL
			THEN (ftz.range::tstzmultirange - epr.range::tstzmultirange)
			ELSE ftz.range::tstzmultirange
			END AS ranges
		FROM future_tzs ftz
		LEFT JOIN existing_part_ranges epr ON ftz.range && epr.range
	)
	select ARRAY_AGG(u.range) FROM diffs
	CROSS JOIN LATERAL unnest(diffs.ranges) AS u(range)
	INTO ranges_to_create;

	ranges_to_create := COALESCE(ranges_to_create, ARRAY[]::tstzrange[]);

	-- go from now to future_interval
	FOREACH cur_range IN ARRAY ranges_to_create LOOP
		DECLARE
			start_ev_id event_id := pgmb.create_event_id(lower(cur_range), 0);
			end_ev_id event_id := pgmb.create_event_id(upper(cur_range), 0);
			pt_name TEXT := pgmb.get_time_partition_name(table_id, lower(cur_range));
		BEGIN
			RAISE NOTICE 'creating partition "%". start: %, end: %',
				pt_name, lower(cur_range), upper(cur_range);

			EXECUTE FORMAT(
				'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
				pt_name, table_id, start_ev_id, end_ev_id
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
			AND inhrelid::regclass::text < oldest_pt_to_keep
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
		AND inhrelid::regclass::text
			<= pgmb.get_time_partition_name(table_id, current_ts)
	ORDER BY inhrelid DESC
	LIMIT 1
$$ LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER;

-- subscriptions table and related functions ----------------

CREATE DOMAIN group_id AS VARCHAR(48);

CREATE OR REPLACE FUNCTION create_subscription_id()
RETURNS subscription_id AS $$
	SELECT 'su' || substring(
		create_event_id(NOW(), create_random_bigint())
		FROM 3
	);
$$ LANGUAGE sql VOLATILE STRICT PARALLEL SAFE SECURITY DEFINER
 SET search_path TO pgmb;

-- subscription, groups tables and functions will go here ----------------

CREATE TABLE subscription_groups(
	id group_id PRIMARY KEY,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	last_read_event_id event_id DEFAULT create_event_id(NOW(), 0)
);

CREATE TABLE subscriptions (
	-- unique identifier for the subscription
	id subscription_id PRIMARY KEY DEFAULT create_subscription_id(),
	-- define how the subscription is grouped. subscriptions belonging
	-- to the same group are read in one batch.
	group_id group_id NOT NULL REFERENCES subscription_groups(id)
		ON DELETE RESTRICT,
	-- A SQL expression that will be used to filter events for this subscription.
	-- The events table will be aliased as "e" in this expression. The subscription
	-- table is available as "s".
	-- Example: "e.topic = s.metadata->>'topic'",
	conditions_sql TEXT NOT NULL DEFAULT 'TRUE',
	-- params will be indexed, and can be used to store
	-- additional parameters for the subscription's conditions_sql.
	-- It's more efficient to have the same conditions_sql for multiple
	-- subscriptions, and differentiate them using params.
	params JSONB NOT NULL DEFAULT '{}'::jsonb,

	identity bigint GENERATED ALWAYS AS (
		hashtext(
			group_id
			|| '/' || conditions_sql
			|| '/' || jsonb_hash(params)::text
		)
	) STORED UNIQUE,
	-- when was this subscription last active
	last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	-- how long before this subscription expires since
	-- its last_active_at time. NULL means it never expires.
	expiry_interval INTERVAL
);

-- immutable fn to add interval to timestamptz
CREATE FUNCTION add_interval_imm(tstz TIMESTAMPTZ, itvl INTERVAL)
RETURNS TIMESTAMPTZ AS $$
	SELECT tstz + itvl;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE
	SET search_path TO pgmb;

-- note: index to quickly find expired subscriptions, not creating
-- a column separately because there's some weird deadlock issue
-- when creating a separate generated "expires_at" column.
CREATE INDEX ON subscriptions(
	group_id,
	add_interval_imm(last_active_at, expiry_interval)
) WHERE expiry_interval IS NOT NULL;

DO $$
DECLARE
	has_btree_gin BOOLEAN;
BEGIN
	has_btree_gin := (
		SELECT EXISTS (
			SELECT 1
			FROM pg_available_extensions
			WHERE name = 'btree_gin'
		)
	);
	-- create btree_gin extension if not exists, if the extension
	-- is not available, we create a simpler regular GIN index instead.
	IF has_btree_gin THEN
		CREATE EXTENSION IF NOT EXISTS btree_gin;
		-- fastupdate=false, slows down subscription creation, but ensures the costlier
		-- "poll_for_events" function is executed faster.
		CREATE INDEX "sub_gin" ON subscriptions USING GIN(conditions_sql, params)
			WITH (fastupdate = false);
	ELSE
		RAISE NOTICE 'btree_gin extension is not available, using
			regular GIN index for subscriptions.params';
		CREATE INDEX "sub_gin" ON subscriptions USING GIN(params)
			WITH (fastupdate = false);
	END IF;
END
$$;

-- materialized view to hold distinct conditions_sql statements.
-- We utilise changes in this view to determine when to prepare the
-- "poll_for_events" function.
CREATE MATERIALIZED VIEW IF NOT EXISTS subscription_cond_sqls AS (
	SELECT DISTINCT conditions_sql FROM subscriptions
	ORDER BY conditions_sql
);

CREATE UNIQUE INDEX IF NOT EXISTS
	subscription_cond_sqls_idx ON subscription_cond_sqls(conditions_sql);

-- subscription events holds the events dispatched to each subscription
-- for each group. Like events, this is also an insert-only table. Groups
-- move their cursors forward as they read events. We can implement very safe
-- cursor movement as only a single writer (poll_for_events) writes to this table,
-- and multiple readers read from it.
CREATE TABLE IF NOT EXISTS subscription_events(
	id event_id,
	group_id group_id,
	event_id event_id,
	subscription_id subscription_id
) PARTITION BY RANGE (id);

CREATE INDEX IF NOT EXISTS subscription_events_group_idx
	ON subscription_events(group_id, id);

-- Create a role with minimal access to the database. As we deal with
-- custom SQL quite often, we don't want an accidentally malicious or bad
-- SQL to have too much access to the database.
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_roles WHERE rolname = 'pgmb_reader'
	) THEN
		CREATE ROLE pgmb_reader NOLOGIN NOSUPERUSER NOCREATEDB
	 	NOCREATEROLE NOINHERIT NOREPLICATION;
	END IF;
END
$$;

-- Give schema usage access
GRANT USAGE, CREATE ON SCHEMA pgmb TO pgmb_reader;
GRANT SELECT ON TABLE events TO pgmb_reader;
GRANT SELECT ON TABLE config TO pgmb_reader;
GRANT SELECT ON TABLE subscriptions TO pgmb_reader;
GRANT SELECT, UPDATE, DELETE ON TABLE unread_events TO pgmb_reader;
-- Grant insert-only access to "subscription_events"
GRANT UPDATE, INSERT ON TABLE subscription_events TO pgmb_reader;

SET ROLE pgmb_reader;
-- This trigger puts the conditions_sql through a syntax check
CREATE OR REPLACE FUNCTION validate_subscription_conditions_sql()
RETURNS TRIGGER AS $$
BEGIN
	EXECUTE 'SELECT * FROM jsonb_populate_recordset(NULL::pgmb.events, ''[]'') e
		INNER JOIN jsonb_populate_recordset(NULL::pgmb.subscriptions, ''[{}]'') s
		ON ' || NEW.conditions_sql;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE
	SET search_path TO pgmb
	SECURITY DEFINER;
RESET ROLE;

CREATE TRIGGER validate_subscription_conditions_sql_trigger
BEFORE INSERT OR UPDATE ON subscriptions
FOR EACH ROW
EXECUTE FUNCTION validate_subscription_conditions_sql();

-- poll_for_events function template. As we add/remove different subscriptions,
-- we'll prepare a new version of the "poll_for_events" function with
-- the updated "conditions_sql" statements.
-- A template function is used so we get syntax highlighting and checking
-- when editing this function.
CREATE OR REPLACE FUNCTION poll_for_events_tmpl()
RETURNS INT AS $body$
DECLARE
	read_ids event_id[];
	max_id event_id;
	min_id event_id;

	chunk_size INT := get_config_value('poll_chunk_size')::INT;

	inserted_rows integer;

	start_num BIGINT := create_random_bigint();
	write_start TIMESTAMPTZ;

	lock_key CONSTANT BIGINT :=
		hashtext('pgmb.poll_for_events');
BEGIN
	IF NOT pg_try_advisory_xact_lock(lock_key) THEN
		-- another process is already polling for events
		RETURN 0;
	END IF;

	WITH to_delete AS (
		SELECT td.event_id
		FROM unread_events td
		WHERE td.event_id < create_event_id(NOW(), 0)
		FOR UPDATE SKIP LOCKED
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

	write_start := clock_timestamp();

	WITH read_events AS (
		SELECT e.*
		FROM events e
		INNER JOIN unnest(read_ids) r(id) ON e.id = r.id
		WHERE e.id <= max_id AND e.id >= min_id
	)
	INSERT INTO subscription_events(id, group_id, subscription_id, event_id)
	SELECT
		create_event_id(write_start, start_num + row_number() OVER ()),
		s.group_id,
		s.id,
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
		)
	ON CONFLICT DO NOTHING;

	GET DIAGNOSTICS inserted_rows = ROW_COUNT;

	-- return total inserted events
	RETURN inserted_rows;
END;
$body$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb
SECURITY DEFINER;

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
	-- build the condition SQL
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
	from pg_proc where proname = tmpl_proc_name and
		pronamespace = 'pgmb'::regnamespace;
	IF proc_src IS NULL THEN
		RAISE EXCEPTION 'Template procedure % not found', tmpl_proc_name;
	END IF;

	-- replace the placeholder with the actual condition SQL
	proc_src := REPLACE(proc_src, tmpl_proc_placeholder, condition_sql);
	proc_src := REPLACE(proc_src, tmpl_proc_name, 'poll_for_events');

	-- the new poll_for_events function will be created with
	-- the pgmb_reader role, to avoid a bad "conditions_sql"
	-- from having any destructive access to the database.
	SET ROLE pgmb_reader;
	EXECUTE proc_src;
	RESET ROLE;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb
SECURITY INVOKER;

SELECT prepare_poll_for_events_fn('{}'::text[]);

-- we'll prepare the subscription read statement whenever subscriptions
-- are created/updated/deleted
CREATE OR REPLACE FUNCTION refresh_subscription_read_statements()
RETURNS TRIGGER AS $$
DECLARE
	needs_refresh BOOLEAN := FALSE;
	old_conditions_sql TEXT[];
	conditions_sql TEXT[];
BEGIN
	old_conditions_sql := ARRAY(
		SELECT * FROM subscription_cond_sqls
		ORDER BY conditions_sql
	);

	REFRESH MATERIALIZED VIEW CONCURRENTLY subscription_cond_sqls;

	conditions_sql := ARRAY(
		SELECT * FROM subscription_cond_sqls
		ORDER BY conditions_sql
	);

	-- conditions_sql hasn't changed, no need to refresh the
	-- poll_for_events function
	IF conditions_sql = old_conditions_sql THEN
		RETURN NULL;
	END IF;

	PERFORM prepare_poll_for_events_fn(conditions_sql);
	RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb
	SECURITY INVOKER;

CREATE TRIGGER refresh_subscription_read_statements_trigger
AFTER INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_subscription_read_statements();

-- Utility fn to read events by their IDs. This exists as postgres
-- doesn't correctly filter which partitions to read from when using
-- an IN/JOIN clause on a partitioned table.
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

	-- get min and max ids, allows PG to correctly prune partitions
	SELECT
		MAX(eid),
		MIN(eid)
	INTO max_id, min_id
	FROM unnest(event_ids) AS u(eid);

	RETURN QUERY
		SELECT e.*
		FROM events e
		INNER JOIN unnest(event_ids) AS u(eid) ON e.id = u.eid
		WHERE e.id <= max_id AND e.id >= min_id
		ORDER BY u.eid;
END;
$$ LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
SET search_path TO pgmb;

-- fn to read next events for a subscription group
CREATE OR REPLACE FUNCTION read_next_events(
	gid VARCHAR(48),
	cursor event_id DEFAULT NULL,
	chunk_size INT DEFAULT get_config_value('poll_chunk_size')::INT,
	-- if peek is true, we do not require having to acquire the advisory
	-- lock to read events. Useful to debug without blocking other readers.
	peek BOOLEAN DEFAULT FALSE
) RETURNS TABLE(
	id event_id,
	topic VARCHAR(255),
	payload JSONB,
	metadata JSONB,
	subscription_ids subscription_id[],
	next_cursor event_id
) AS $$
DECLARE
	lock_key CONSTANT BIGINT :=
		hashtext('pgmb.read_next_events.' || gid);
BEGIN
	-- provide a lock for the group, so that if we temporarily
	-- or accidentally have multiple readers for the same group,
	-- they don't interfere with each other.
	IF NOT pg_try_advisory_lock(lock_key) AND NOT peek THEN
		RETURN;
	END IF;
	-- fetch the cursor to read from
	-- if no cursor is provided, fetch from the group's last read event id
	IF cursor IS NULL THEN
		SELECT sc.last_read_event_id
		FROM subscription_groups sc
		WHERE sc.id = gid
		INTO cursor;
	END IF;
	-- if still null, don't return anything
	IF cursor IS NULL THEN
		RETURN;
	END IF;

	RETURN QUERY WITH next_events AS (
		SELECT
			se.id,
			se.event_id,
			se.subscription_id
		FROM subscription_events se
		INNER JOIN subscriptions s ON s.id = se.subscription_id
		WHERE se.group_id = gid
			AND se.id < create_event_id(NOW(), 0)
			AND se.id > cursor
		LIMIT chunk_size
	),
	next_events_grp AS (
		SELECT
			ne.event_id,
			ARRAY_AGG(ne.subscription_id) AS subscription_ids
		FROM next_events ne
		GROUP BY ne.event_id
		ORDER BY ne.event_id
	)
	SELECT
		e.id,
		e.topic,
		e.payload,
		e.metadata,
		ne.subscription_ids,
		(SELECT MAX(ne2.id)::event_id FROM next_events ne2)
	FROM read_events(ARRAY(SELECT ne.event_id FROM next_events_grp ne)) e
	INNER JOIN next_events_grp ne ON ne.event_id = e.id;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE
	SET search_path TO pgmb
	SECURITY INVOKER;

CREATE OR REPLACE FUNCTION replay_events(
	gid VARCHAR(48),
	sid VARCHAR(24),
	from_event_id event_id,
	max_events INT
) RETURNS SETOF events AS $$
DECLARE
	event_ids event_id[];
	now_id event_id := create_event_id(NOW(), 0);
BEGIN
	SELECT ARRAY_AGG(se.event_id) INTO event_ids
	FROM subscription_events se
	WHERE se.group_id = gid
		AND se.subscription_id = sid
		AND se.event_id > from_event_id
		AND se.event_id <= now_id
		-- we filter "id" by the same range too, because
		-- 1. the format of se.id and e.id are the same. And rows are
		-- inserted into the se table after the corresponding e row is created,
		-- so if we find rows > from_event_id in se.event_id, the corresponding
		-- e.id will also be > from_event_id
		-- 2. it helps postgres prune which partitions to read from
		AND se.id <= now_id
		AND se.id > from_event_id
	LIMIT (max_events + 1);
	IF array_length(event_ids, 1) > max_events THEN
		RAISE EXCEPTION
			'Too many events to replay. Please replay in smaller batches.';
	END IF;

	RETURN QUERY SELECT * FROM read_events(event_ids);
END $$ LANGUAGE plpgsql STABLE PARALLEL SAFE
	SET search_path TO pgmb
	SECURITY INVOKER;

CREATE OR REPLACE FUNCTION release_group_lock(gid VARCHAR(48))
RETURNS VOID AS $$
DECLARE
	lock_key CONSTANT BIGINT :=
		hashtext('pgmb.read_next_events.' || gid);
BEGIN
	PERFORM pg_advisory_unlock(lock_key);
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
SET search_path TO pgmb;

-- upsert the group's cursor
CREATE OR REPLACE FUNCTION set_group_cursor(
	gid VARCHAR(48),
	new_cursor event_id,
	-- if true, release any existing lock for this group
	release_lock BOOLEAN
) RETURNS VOID AS $$
BEGIN
	-- upsert the new cursor
	INSERT INTO subscription_groups(id, last_read_event_id)
		VALUES (gid, new_cursor)
		ON CONFLICT (id) DO UPDATE
		SET last_read_event_id = EXCLUDED.last_read_event_id;

	-- release any existing lock for this group, if we hold one
	IF release_lock THEN
		PERFORM release_group_lock(gid);
	END IF;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE
SET search_path TO pgmb;

CREATE OR REPLACE FUNCTION maintain_events_table(
	current_ts timestamptz DEFAULT NOW()
)
RETURNS VOID AS $$
DECLARE
	pi INTERVAL := get_config_value('partition_interval');
	fic INTERVAL := get_config_value('future_intervals_to_create');
	rp INTERVAL := get_config_value('partition_retention_period');
BEGIN
	PERFORM maintain_time_partitions_using_event_id(
		'pgmb.events'::regclass,
		partition_interval := pi,
		future_interval := fic,
		retention_period := rp,
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

	PERFORM maintain_time_partitions_using_event_id(
		'pgmb.subscription_events'::regclass,
		partition_interval := pi,
		future_interval := fic,
		retention_period := rp,
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
SET search_path TO pgmb;

-- create the initial partitions
SELECT maintain_events_table();

-- triggers to add events for specific tables ---------------------------

-- Function to create a topic string for subscriptions.
-- Eg. "public" "contacts" "INSERT" -> "public.contacts.insert"
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

-- Function to serialise a record for an event, and tell us
-- whether to emit the event or not.
-- Note: Regardless of whether to emit the event, the serialised
-- JSONB is returned.
-- By default, we always emit the event.
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
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE SECURITY INVOKER;

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
			jsonb_diff(n.data, o.data),
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
		WHERE jsonb_diff(n.data, o.data) is not null AND n.emit;
	END IF;

	RETURN NULL;
END
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb;

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
		EXCEPTION WHEN duplicate_object THEN
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
		EXCEPTION WHEN duplicate_object THEN
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
		EXCEPTION WHEN duplicate_object THEN
			NULL;
	  END;
	END IF;
END
$$ LANGUAGE plpgsql SECURITY DEFINER
	VOLATILE PARALLEL UNSAFE
	SET search_path TO pgmb;

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
	SET search_path TO pgmb;
