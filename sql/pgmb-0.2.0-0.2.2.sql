SET search_path TO pgmb;
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
