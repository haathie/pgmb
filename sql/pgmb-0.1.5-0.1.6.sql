SET search_path = pgmb, pg_catalog;

-- Find all existing queues via the "pgmb.queues" table,
-- and drop the default on the "id" column
DO $$
DECLARE
  queue_schema VARCHAR(64);
BEGIN
	-- get all queue schemas
	-- drop the default on the "id" column for each queue
	FOR queue_schema IN (SELECT schema_name FROM pgmb.queues) LOOP
		EXECUTE 'ALTER TABLE '
			|| quote_ident(queue_schema)
			|| '.live_messages ALTER COLUMN id DROP DEFAULT';
	END LOOP;
END $$;

DROP FUNCTION create_message_id(timestamp with time zone,bigint);

DROP FUNCTION create_random_bigint(additive double precision);

DROP FUNCTION extract_date_from_message_id(message_id VARCHAR(64));

DROP FUNCTION get_queue_metrics(queue_name VARCHAR(64));

DROP FUNCTION get_all_queue_metrics();

CREATE OR REPLACE FUNCTION create_queue_table(queue_name VARCHAR(64), schema_name VARCHAR(64), queue_type pgmb.queue_type) RETURNS VOID AS $$
BEGIN
	-- create the live_messages table
	EXECUTE 'CREATE TABLE ' || quote_ident(schema_name) || '.live_messages (
		id VARCHAR(22) PRIMARY KEY,
		message BYTEA NOT NULL,
		headers JSONB NOT NULL DEFAULT ''{}''::JSONB,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)';
	IF queue_type = 'unlogged' THEN
		EXECUTE 'ALTER TABLE ' || quote_ident(schema_name)
			|| '.live_messages SET UNLOGGED';
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_random_bigint() RETURNS BIGINT AS $$
BEGIN
	-- the message ID allows for 7 hex-bytes of randomness,
	-- i.e. 28 bits of randomness. Thus, the max we allow is 2^28/2
	-- i.e. 0xffffff8, which allows for batch inserts to increment the
	-- randomness for up to another 2^28/2 messages (more than enough)
	RETURN (random() * 0xffffff8)::BIGINT;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION create_message_id(dt timestamptz = clock_timestamp(), rand bigint = pgmb.create_random_bigint()) RETURNS VARCHAR(22) AS $$
BEGIN
	-- create a unique message ID, 16 chars of hex-date
	-- some additional bytes of randomness
	-- ensure the string is always, at most 32 bytes
	RETURN substr(
		'pm'
		|| substr(lpad(to_hex((extract(epoch from dt) * 1000000)::bigint), 13, '0'), 1, 13)
		|| lpad(to_hex(rand), 7, '0'),
		1,
		22
	);
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_max_message_id(dt timestamptz = clock_timestamp()) RETURNS VARCHAR(22) AS $$
BEGIN
	RETURN pgmb.create_message_id(
		dt,
		rand := 999999999999  -- max randomness
	);
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION extract_date_from_message_id(message_id VARCHAR(22)) RETURNS TIMESTAMPTZ AS $$
BEGIN
	-- convert it to a timestamp
	RETURN to_timestamp(('0x' || substr(message_id, 3, 13))::numeric / 1000000);
END
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION send(queue_name VARCHAR(64), messages pgmb.enqueue_msg[]) RETURNS SETOF VARCHAR(22) AS $$
DECLARE
	-- we'll have a starting random number, and each successive message ID's
	-- random component will be this number + the ordinality of the message.
	start_rand constant BIGINT = pgmb.create_random_bigint();
BEGIN
	-- create the ID for each message, and then send to the internal _send fn
	RETURN QUERY
	WITH msg_records AS (
		SELECT (
			pgmb.create_message_id(
				COALESCE(m.consume_at, clock_timestamp()),
				start_rand + m.ordinality
			),
			m.message,
			m.headers
		)::pgmb.msg_record AS record
		FROM unnest(messages) WITH ORDINALITY AS m
	)
	SELECT pgmb._send(queue_name, ARRAY_AGG(m.record)::pgmb.msg_record[])
	FROM msg_records m;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _send(queue_name VARCHAR(64), messages pgmb.msg_record[]) RETURNS SETOF VARCHAR(22) AS $$
DECLARE
	-- check if the queue already exists
	schema_name VARCHAR(64);
	default_headers JSONB;
BEGIN
	-- each queue would have its own channel to listen on, so a consumer can
	-- listen to a specific queue. This'll be used to notify the consumer when
	-- new messages are added to the queue.
	PERFORM pg_notify(
		'chn_' || queue_name,
		('{"count":' || array_length(messages, 1)::varchar || '}')::varchar
	);

	-- get schema name and default headers
	SELECT q.schema_name, q.default_headers FROM pgmb.queues q
	WHERE q.name = queue_name INTO schema_name, default_headers;
	-- Insert the message into the queue and return all message IDs. We use the
	-- ordinality of the array to ensure that each message is inserted in the same
	-- order as it was sent. This is important for the consumer to process the
	-- messages in the same order as they were sent.
	RETURN QUERY
	EXECUTE 'INSERT INTO '
		|| quote_ident(schema_name)
		|| '.live_messages (id, message, headers)
	SELECT
		id,
		message,
		COALESCE($1, ''{}''::JSONB) || COALESCE(headers, ''{}''::JSONB)
	FROM unnest($2)
	RETURNING id' USING default_headers, messages;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_from_queue(queue_name VARCHAR(64), limit_count INTEGER = 1) RETURNS SETOF pgmb.msg_record AS $$
DECLARE
	schema_name VARCHAR(64);
BEGIN
	-- get schema name
	SELECT q.schema_name FROM pgmb.queues q
	WHERE q.name = queue_name INTO schema_name;
	-- read the messages from the queue
	RETURN QUERY EXECUTE 'SELECT id, message, headers
		FROM ' || quote_ident(schema_name) || '.live_messages
		WHERE id <= pgmb.get_max_message_id()
		ORDER BY id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1'
	USING limit_count;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION publish(messages pgmb.publish_msg[]) RETURNS SETOF VARCHAR(22) AS $$
DECLARE
	start_rand constant BIGINT = pgmb.create_random_bigint();
BEGIN
	-- Create message IDs for each message, then we'll send them to the individual
	-- queues. The ID will be the same for all queues, but the headers may vary
	-- across queues.
	RETURN QUERY
	WITH msg_records AS (
		SELECT
			pgmb.create_message_id(
				COALESCE(consume_at, clock_timestamp()),
				start_rand + ordinality
			) AS id,
			message,
			JSONB_SET(
				COALESCE(headers, '{}'::JSONB),
				'{exchange}',
				TO_JSONB(exchange)
			) as headers,
			exchange,
			ordinality
		FROM unnest(messages) WITH ORDINALITY
	),
	sends AS (
		SELECT
			pgmb._send(
				q.queue_name,
				ARRAY_AGG((m.id, m.message, m.headers)::pgmb.msg_record)
			) as id
		FROM msg_records m,
		LATERAL (
			SELECT DISTINCT name, unnest(queues) AS queue_name
			FROM pgmb.exchanges e
			WHERE e.name = m.exchange
		) q
		GROUP BY q.queue_name
	)
	-- we'll select an aggregate of "sends", to ensure that each "send" call
	-- is executed. If this is not done, PG may optimize the query
	-- and not execute the "sends" CTE at all, resulting in no messages being sent.
	-- So, this aggregate call ensures PG does not optimize it away.
	SELECT
		CASE WHEN count(*) FILTER (WHERE sends.id IS NOT NULL) > 0 THEN m.id END
	FROM msg_records m
	LEFT JOIN sends ON sends.id = m.id
	GROUP BY m.id, m.ordinality
	ORDER BY m.ordinality;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_queue_metrics(queue_name VARCHAR(64), approximate BOOLEAN = FALSE) RETURNS SETOF pgmb.metrics_result AS $$
DECLARE
	schema_name VARCHAR(64);
BEGIN
	-- get schema name
	SELECT q.schema_name FROM pgmb.queues q
	WHERE q.name = queue_name INTO schema_name;
	-- get the metrics of the queue
	RETURN QUERY EXECUTE 'SELECT
		''' || queue_name || '''::varchar(64) AS queue_name,
		' ||
			(CASE WHEN approximate THEN
				'COALESCE(pgmb.get_approximate_count(' || quote_literal(schema_name || '.live_messages') || '), 0) AS total_length,'
				|| '0 AS consumable_length,'
			ELSE
				'count(*)::int AS total_length,'
				|| '(count(*) FILTER (WHERE id <= pgmb.get_max_message_id()))::int AS consumable_length,'
			END) || '
		(clock_timestamp() - pgmb.extract_date_from_message_id(max(id))) AS newest_msg_age_sec,
		(clock_timestamp() - pgmb.extract_date_from_message_id(min(id))) AS oldest_msg_age_sec
		FROM ' || quote_ident(schema_name) || '.live_messages';
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_all_queue_metrics(approximate BOOLEAN = FALSE) RETURNS SETOF pgmb.metrics_result AS $$
BEGIN
	RETURN QUERY
	SELECT m.*
	FROM pgmb.queues q, pgmb.get_queue_metrics(q.name, approximate) m
	ORDER BY q.name ASC;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_approximate_count(table_name regclass) RETURNS INTEGER AS $$
	SELECT (
		CASE WHEN c.reltuples < 0 THEN NULL       -- never vacuumed
		WHEN c.relpages = 0 THEN float8 '0'  -- empty table
		ELSE c.reltuples / c.relpages END
		* (pg_catalog.pg_relation_size(c.oid)
		/ pg_catalog.current_setting('block_size')::int)
	)::bigint
	FROM  pg_catalog.pg_class c
	WHERE c.oid = table_name
	LIMIT 1;
$$ LANGUAGE sql;
