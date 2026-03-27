SET search_path TO pgmb;

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
	EXECUTE proc_src;
	-- changing the owner will ensure that the function is executed with
	-- the pgmb_reader's permissions.
	-- https://www.postgresql.org/docs/current/sql-alterfunction.html
	EXECUTE 'ALTER FUNCTION poll_for_events() OWNER TO pgmb_reader';
END;
$$ LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path TO pgmb
SECURITY INVOKER;
