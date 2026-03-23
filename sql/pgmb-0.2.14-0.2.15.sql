SET search_path TO pgmb;

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
	lock_key CONSTANT BIGINT :=
		hashtext('pgmb.maintain_tp.' || table_id::text);
	ranges_to_create tstzrange[];
	partitions_to_drop regclass[];
	p_to_drop regclass;
	cur_range tstzrange;
	max_retries constant int = 50;
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

	SELECT ARRAY_AGG(inhrelid::regclass) INTO partitions_to_drop
	FROM pg_catalog.pg_inherits
	WHERE inhparent = table_id
		AND inhrelid::regclass::text < oldest_pt_to_keep;
	partitions_to_drop := COALESCE(partitions_to_drop, ARRAY[]::regclass[]);

	-- check if nothing to do
	IF
		array_length(partitions_to_drop, 1) = 0
		AND array_length(ranges_to_create, 1) = 0
	THEN
		RETURN;
	END IF;

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
	FOREACH p_to_drop IN ARRAY partitions_to_drop LOOP
		EXECUTE format('DROP TABLE %I', p_to_drop);
	END LOOP;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER;

CREATE FUNCTION maintain_append_only_table(
	tbl regclass,
	current_ts timestamptz DEFAULT NOW()
)
RETURNS VOID AS $$
	SELECT maintain_time_partitions_using_event_id(
		tbl,
		partition_interval := get_config_value('partition_interval')::interval,
		future_interval := get_config_value('future_intervals_to_create')::interval,
		retention_period := get_config_value('partition_retention_period')::interval,
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
$$ LANGUAGE sql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path TO pgmb;

DROP FUNCTION IF EXISTS maintain_events_table(timestamptz);

CREATE OR REPLACE PROCEDURE maintain_events_table(
	current_ts timestamptz DEFAULT NOW()
) AS $$
DECLARE
	pi INTERVAL := pgmb.get_config_value('partition_interval');
	fic INTERVAL := pgmb.get_config_value('future_intervals_to_create');
	rp INTERVAL := pgmb.get_config_value('partition_retention_period');
BEGIN
	SET search_path TO pgmb;

	PERFORM maintain_append_only_table('events'::regclass, current_ts);
	COMMIT;

	PERFORM maintain_append_only_table('subscription_events'::regclass, current_ts);
	COMMIT;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
	IF get_config_value('use_pg_cron') <> 'true' THEN
		RETURN;
	END IF;

	PERFORM cron.schedule(
		'pgmb_maintain_table_partitions',
		get_config_value('pg_cron_partition_maintenance_cron'),
		$CMD$ CALL pgmb.maintain_events_table(); $CMD$
	);
END
$$;
