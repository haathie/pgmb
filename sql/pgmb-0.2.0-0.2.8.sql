SET search_path TO pgmb;

ALTER TYPE config_type ADD VALUE 'use_pg_cron';
ALTER TYPE config_type ADD VALUE 'pg_cron_poll_for_events_cron';
ALTER TYPE config_type ADD VALUE 'pg_cron_partition_maintenance_cron';

INSERT INTO config(id, value) VALUES
('pg_cron_poll_for_events_cron', '1 second'),
-- every 30 minutes
('pg_cron_partition_maintenance_cron', '*/30 * * * *');

CREATE OR REPLACE FUNCTION manage_cron_jobs_trigger_fn()
RETURNS TRIGGER AS $$
DECLARE
	poll_job_name CONSTANT TEXT := 'pgmb_poll';
	maintain_job_name CONSTANT TEXT := 'pgmb_maintain_table_partitions';
BEGIN
	IF get_config_value('use_pg_cron') = 'true' THEN
		-- Schedule/update event polling job
		PERFORM cron.schedule(
			poll_job_name,
			get_config_value('pg_cron_poll_for_events_cron'),
			$CMD$
				-- ensure we don't accidentally run for too long
				SET SESSION statement_timeout = '10s';
				SELECT pgmb.poll_for_events();
			$CMD$
		);
		RAISE LOG 'Scheduled pgmb polling job: %', poll_job_name;

		-- Schedule/update partition maintenance job
		PERFORM cron.schedule(
			'pgmb_maintain_table_partitions',
			get_config_value('pg_cron_partition_maintenance_cron'),
			$CMD$ SELECT pgmb.maintain_events_table(); $CMD$
		);

		RAISE LOG 'Scheduled pgmb partition maintenance job: %',
			maintain_job_name;
	ELSIF (SELECT 1 FROM pg_namespace WHERE nspname = 'cron') THEN
		RAISE LOG 'Unscheduling pgmb cron jobs.';
		-- Unschedule jobs. cron.unschedule does not fail if job does not exist.
		PERFORM cron.unschedule(poll_job_name);
		PERFORM cron.unschedule(maintain_job_name);
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path TO pgmb;

CREATE TRIGGER manage_cron_jobs_trigger
AFTER INSERT OR UPDATE OF value ON config
FOR EACH ROW
WHEN (
	NEW.id IN (
		'use_pg_cron',
		'pg_cron_poll_for_events_cron',
		'pg_cron_partition_maintenance_cron'
	)
)
EXECUTE FUNCTION manage_cron_jobs_trigger_fn();

DO $$
BEGIN
	IF (
		SELECT true
		FROM pg_available_extensions
		WHERE name = 'pg_cron'
	) AND (
		-- ensure the current database is where pg_cron can be installed
		current_database()
		= coalesce(current_setting('cron.database_name', true), 'postgres')
	) THEN
		CREATE EXTENSION IF NOT EXISTS pg_cron;
		INSERT INTO config(id, value) VALUES ('use_pg_cron', 'true');
	ELSE
		RAISE LOG 'pg_cron extension not available. Skipping pg_cron setup.';
		INSERT INTO config(id, value) VALUES ('use_pg_cron', 'false');
	END IF;
END
$$;
