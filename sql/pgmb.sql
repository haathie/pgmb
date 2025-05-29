CREATE SCHEMA IF NOT EXISTS pgmb;

-- type to create a message that's sent to a queue
CREATE TYPE pgmb.enqueue_msg AS (
	message BYTEA, headers JSONB, consume_at TIMESTAMPTZ
);
-- type to create a message that's published to an exchange
-- This'll be used to publish messages to exchanges
CREATE TYPE pgmb.publish_msg AS (
	exchange VARCHAR(64), message BYTEA, headers JSONB, consume_at TIMESTAMPTZ
);
-- type to store an existing message record
CREATE TYPE pgmb.msg_record AS (
	id VARCHAR(22), message BYTEA, headers JSONB
);
-- type to store the result of a queue's metrics
CREATE TYPE pgmb.metrics_result AS (
	queue_name VARCHAR(64),
	total_length int,
	consumable_length int,
	newest_msg_age interval,
	oldest_msg_age interval
);

CREATE TYPE pgmb.queue_ack_setting AS ENUM ('archive', 'delete');
CREATE TYPE pgmb.queue_type AS ENUM ('logged', 'unlogged');

-- table for exchanges
CREATE TABLE pgmb.exchanges (
	name VARCHAR(64) PRIMARY KEY,
	queues VARCHAR(64)[] NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ DEFAULT NOW()
);

-- fn to create/delete/add queues/remove queues to exchanges
CREATE OR REPLACE FUNCTION pgmb.assert_exchange(nm VARCHAR(64))
RETURNS VOID AS $$
BEGIN
	INSERT INTO pgmb.exchanges (name) (VALUES (nm))
	ON CONFLICT (name) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- fn to delete an exchange
CREATE OR REPLACE FUNCTION pgmb.delete_exchange(nm VARCHAR(64))
RETURNS VOID AS $$
BEGIN
	DELETE FROM pgmb.exchanges WHERE name = nm;
END;
$$ LANGUAGE plpgsql;

-- fn to bind a queue to an exchange
CREATE OR REPLACE FUNCTION pgmb.bind_queue(
	queue_name VARCHAR(64), exchange VARCHAR(64) 
)
RETURNS VOID AS $$
BEGIN
	UPDATE pgmb.exchanges
	SET queues = array_append(queues, queue_name)
	WHERE name = exchange AND NOT queue_name = ANY(queues);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmb.unbind_queue(
	queue_name VARCHAR(64), exchange VARCHAR(64)
)
RETURNS VOID AS $$
BEGIN
	UPDATE pgmb.exchanges
	SET queues = array_remove(queues, queue_name)
	WHERE name = exchange;
END;
$$ LANGUAGE plpgsql;

-- table for queue metadata
CREATE TABLE pgmb.queues (
	name VARCHAR(64) PRIMARY KEY,
	schema_name VARCHAR(64) NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	ack_setting pgmb.queue_ack_setting DEFAULT 'delete',
	queue_type pgmb.queue_type DEFAULT 'logged',
	default_headers JSONB DEFAULT '{}'::JSONB
);

-- utility function to create a queue table
CREATE OR REPLACE FUNCTION pgmb.create_queue_table(
	queue_name VARCHAR(64),
	schema_name VARCHAR(64),
	queue_type pgmb.queue_type
) RETURNS VOID AS $$
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


-- fn to ensure a queue exists. If bindings are provided, all existing
-- bindings are removed and the queue is bound to the new exchanges.
-- @returns true if the queue was created, false if it already exists
CREATE OR REPLACE FUNCTION pgmb.assert_queue(
	queue_name VARCHAR(64),
	ack_setting pgmb.queue_ack_setting DEFAULT NULL,
	default_headers JSONB DEFAULT NULL,
	queue_type pgmb.queue_type DEFAULT NULL,
	bindings VARCHAR(64)[] DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
	-- check if the queue already exists
	schema_name VARCHAR(64);
	_ack_setting pgmb.queue_ack_setting;
	_queue_type pgmb.queue_type;
BEGIN
	schema_name := 'pgmb_q_' || queue_name;

	-- if bindings are provided, assert the exchanges,
	-- and bind the queue to them
	IF bindings IS NOT NULL THEN
		-- remove all existing bindings
		UPDATE pgmb.exchanges
		SET queues = array_remove(queues, queue_name)
		WHERE queue_name = ANY(queues);
		-- create the exchanges
		PERFORM pgmb.assert_exchange(binding)
		FROM unnest(bindings) AS binding;
		-- bind the queue to the exchanges
		PERFORM pgmb.bind_queue(queue_name, binding)
		FROM unnest(bindings) AS binding;
	END IF;

	-- check if the queue already exists
	IF EXISTS (SELECT 1 FROM pgmb.queues WHERE name = queue_name) THEN
		-- queue already exists
		RETURN FALSE;
	END IF;
	-- store in the queues table
	EXECUTE 'INSERT INTO pgmb.queues
		(name, schema_name, ack_setting, default_headers, queue_type)
		VALUES (
			$1,
			$2,'
			|| (CASE WHEN ack_setting IS NULL THEN 'DEFAULT' ELSE '$3' END) || ','
			|| (CASE WHEN default_headers IS NULL THEN 'DEFAULT' ELSE '$4' END) || ','
			|| (CASE WHEN queue_type IS NULL THEN 'DEFAULT' ELSE '$5' END)
			|| ')' USING queue_name, schema_name,
			ack_setting, default_headers, queue_type;
	-- create schema
	EXECUTE 'CREATE SCHEMA ' || quote_ident(schema_name);

	-- get the saved settings
	SELECT q.ack_setting, q.queue_type FROM pgmb.queues q
	WHERE q.name = queue_name INTO _ack_setting, _queue_type;

	-- create the live_messages table
	PERFORM pgmb.create_queue_table(queue_name, schema_name, _queue_type);
	-- create the consumed_messages table (if ack_setting is archive)
	IF _ack_setting = 'archive' THEN
		EXECUTE 'CREATE TABLE ' || quote_ident(schema_name) || '.consumed_messages (
			id VARCHAR(22) PRIMARY KEY,
			message BYTEA NOT NULL,
			headers JSONB NOT NULL DEFAULT ''{}''::JSONB,
			success BOOLEAN NOT NULL,
			consumed_at TIMESTAMPTZ DEFAULT NOW()
		)';
		IF _queue_type = 'unlogged' THEN
			EXECUTE 'ALTER TABLE ' || quote_ident(schema_name)
				|| '.consumed_messages SET UNLOGGED';
		END IF;
	END IF;
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- fn to delete a queue
CREATE OR REPLACE FUNCTION pgmb.delete_queue(queue_name VARCHAR(64))
RETURNS VOID AS $$
DECLARE
	-- check if the queue already exists
	schema_name VARCHAR(64);
BEGIN
	-- get schema name
	SELECT q.schema_name FROM pgmb.queues q
	WHERE q.name = queue_name INTO schema_name;
	-- drop the schema
	EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(schema_name) || ' CASCADE';
	-- remove from exchanges
	UPDATE pgmb.exchanges
	SET queues = array_remove(queues, queue_name)
	WHERE queue_name = ANY(queues);
	-- remove from queues
	DELETE FROM pgmb.queues WHERE name = queue_name;
END;
$$ LANGUAGE plpgsql;

-- fn to purge a queue. Will drop the table and recreate it.
-- This will delete all messages in the queue.
CREATE OR REPLACE FUNCTION pgmb.purge_queue(queue_name VARCHAR(64))
RETURNS VOID AS $$
DECLARE
	schema_name VARCHAR(64);
	queue_type pgmb.queue_type;
BEGIN
	-- get schema name
	SELECT q.schema_name, q.queue_type FROM pgmb.queues q
	WHERE q.name = queue_name INTO schema_name, queue_type;
	-- drop the live_messages table
	EXECUTE 'DROP TABLE IF EXISTS '
		|| quote_ident(schema_name) || '.live_messages';
	-- create the live_messages table
	PERFORM pgmb.create_queue_table(queue_name, schema_name, queue_type);
END;
$$ LANGUAGE plpgsql;

-- fn to create a random bigint. Used for message IDs
CREATE OR REPLACE FUNCTION pgmb.create_random_bigint()
RETURNS BIGINT AS $$
BEGIN
	RETURN (random() * 10000000)::BIGINT;
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

-- fn to create a unique message ID. This'll be the current timestamp
-- + a random number
CREATE OR REPLACE FUNCTION pgmb.create_message_id(
	dt timestamptz DEFAULT clock_timestamp(),
	ord int DEFAULT 0,
	rand bigint DEFAULT pgmb.create_random_bigint()
)
RETURNS VARCHAR(22) AS $$
BEGIN
	-- create a unique message ID, 16 chars of hex-date
	-- some additional bytes of randomness
	-- ensure the string is always, at most 32 bytes
	RETURN substr(
		'pm'
		|| substr(lpad(to_hex((extract(epoch from dt) * 1000000)::bigint), 13, '0'), 1, 13)
		|| lpad(to_hex(ord), 4, '0')
		|| rpad(to_hex(rand), 8, '0'),
		1,
		22
	);
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pgmb.get_max_message_id(
	dt timestamptz DEFAULT clock_timestamp()
)
RETURNS VARCHAR(22) AS $$
BEGIN
	RETURN pgmb.create_message_id(
		dt,
		ord := 16384,  -- max ordinality for a message
		rand := 999999999999  -- max randomness
	);
END
$$ LANGUAGE plpgsql VOLATILE PARALLEL SAFE;

-- fn to extract the date from a message ID.
CREATE OR REPLACE FUNCTION pgmb.extract_date_from_message_id(
	message_id VARCHAR(22)
)
RETURNS TIMESTAMPTZ AS $$
BEGIN
	-- convert it to a timestamp
	RETURN to_timestamp(('0x' || substr(message_id, 3, 13))::numeric / 1000000);
END
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- fn to send multiple messages into a queue
CREATE OR REPLACE FUNCTION pgmb.send(
	queue_name VARCHAR(64),
	messages pgmb.enqueue_msg[]
)
RETURNS SETOF VARCHAR(22) AS $$
BEGIN
	-- create the ID for each message, and then send to the internal _send fn
	RETURN QUERY
	WITH msg_records AS (
		SELECT (
			pgmb.create_message_id(
				COALESCE(m.consume_at, clock_timestamp()),
				m.ordinality::int
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

-- internal fn to send multiple messages with an existing ID into a queue
CREATE OR REPLACE FUNCTION pgmb._send(
	queue_name VARCHAR(64),
	messages pgmb.msg_record[]
)
RETURNS SETOF VARCHAR(22) AS $$
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

-- fn to positively/negatively ack 1 or more messages.
-- If "success": will send to consumed_messages or delete,
--  based on the queue's ack_setting
-- If "failure": will ack the message, and requeue it if retries are left
CREATE OR REPLACE FUNCTION pgmb.ack_msgs(
	queue_name VARCHAR(64),
	success BOOLEAN,
	ids VARCHAR(22)[]
)
RETURNS VOID AS $$
DECLARE
	schema_name VARCHAR(64);
	ack_setting pgmb.queue_ack_setting;
	deleted_records pgmb.msg_record[];
BEGIN
	-- get schema name and ack setting
	SELECT q.schema_name, q.ack_setting
	FROM pgmb.queues q
	WHERE q.name = queue_name
	INTO schema_name, ack_setting;
	-- Delete the messages from live_messages and insert them into
	-- consumed_messages in one operation,
	-- if the queue's ack_setting is set to 'archive'
	EXECUTE 'WITH deleted_msgs AS (
		DELETE FROM ' || quote_ident(schema_name) || '.live_messages
		WHERE id = ANY($1)
		RETURNING id, message, headers
	)
	SELECT array_agg((id, message, headers)::pgmb.msg_record)
	FROM deleted_msgs;'
	INTO deleted_records USING (ids);

	-- Raise exception if no rows were affected
	IF deleted_records IS NULL OR array_length(deleted_records, 1) != array_length(ids, 1) THEN
		RAISE EXCEPTION 'Only removed % out of % expected message(s).',
		array_length(deleted_records, 1), array_length(ids, 1);
	END IF;

	IF ack_setting = 'archive' THEN
		EXECUTE 'INSERT INTO ' || quote_ident(schema_name) ||  '.consumed_messages
			(id, message, headers, success)
			SELECT t.id, t.message, t.headers, $1::boolean
			FROM unnest($2) t'
		USING success, deleted_records;
	END IF;

	-- re-insert messages that can be retried
	IF NOT success THEN
		PERFORM pgmb.send(
			queue_name,
			ARRAY_AGG(
				(
					t.message,
					t.headers
						-- set retriesLeftS to the next retry
						|| jsonb_build_object('retriesLeftS', (t.headers->'retriesLeftS') #- '{0}')
						-- set the originalMessageId
						-- to the original message ID if it exists
						|| jsonb_build_object(
							'originalMessageId', COALESCE(t.headers->'originalMessageId', to_jsonb(t.id))
						)
						-- set the tries
						|| jsonb_build_object(
							'tries',
							CASE
								WHEN jsonb_typeof(t.headers->'tries') = 'number' THEN
									to_jsonb((t.headers->>'tries')::INTEGER + 1)
								ELSE
									to_jsonb(1)
							END
						),
					-- set the last consumed time
					clock_timestamp() + (interval '1 second') * (t.headers->'retriesLeftS'->0)::int
				)::pgmb.enqueue_msg
			)
		)
		FROM unnest(deleted_records) AS t(id, message, headers)
		WHERE jsonb_typeof(t.headers -> 'retriesLeftS' -> 0) = 'number'
		GROUP BY queue_name;
	END IF;
END
$$ LANGUAGE plpgsql;

-- fn to read the next available messages from the queue
-- the messages read, will remain invisible to other consumers
-- until the transaction is either committed or rolled back.
CREATE OR REPLACE FUNCTION pgmb.read_from_queue(
	queue_name VARCHAR(64),
	limit_count INTEGER DEFAULT 1
)
RETURNS SETOF pgmb.msg_record AS $$
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

-- fn to publish a message to 1 or more exchanges.
-- Will find all queues subscribed to it and insert the message into
-- each of them.
-- Each queue will receive a copy of the message, with the exchange name
-- added to the headers. The ID of the message will remain the same
-- across all queues.
-- @returns ID of the published message, if sent to any queues -- NULL at the
--  index of the messages that were not sent to any queues.
CREATE OR REPLACE FUNCTION pgmb.publish(
	messages pgmb.publish_msg[]
)
RETURNS SETOF VARCHAR(22) AS $$
BEGIN
	-- Create message IDs for each message, then we'll send them to the individual
	-- queues. The ID will be the same for all queues, but the headers may vary
	-- across queues.
	RETURN QUERY
	WITH msg_records AS (
		SELECT
			pgmb.create_message_id(
				COALESCE(consume_at, clock_timestamp()),
				ordinality::int
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

-- get the metrics of a queue.
-- Pass "approximate" as true to get an approximate count of the messages, which
-- is much faster than counting all rows. "consumable_length" will always be 0
-- when "approximate" is passed as true.
CREATE OR REPLACE FUNCTION pgmb.get_queue_metrics(
	queue_name VARCHAR(64),
	approximate BOOLEAN DEFAULT FALSE
)
RETURNS SETOF pgmb.metrics_result AS $$
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

-- fn to get metrics for all queues
CREATE OR REPLACE FUNCTION pgmb.get_all_queue_metrics(
	approximate BOOLEAN DEFAULT FALSE
)
RETURNS SETOF pgmb.metrics_result AS $$
BEGIN
	RETURN QUERY
	SELECT m.*
	FROM pgmb.queues q, pgmb.get_queue_metrics(q.name, approximate) m
	ORDER BY q.name ASC;
END
$$ LANGUAGE plpgsql;

-- fn to get an approximate count of rows in a table
-- See: https://stackoverflow.com/a/7945274
CREATE OR REPLACE FUNCTION pgmb.get_approximate_count(
	table_name regclass
)
RETURNS INTEGER AS $$
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

-- uninstall pgmb
CREATE OR REPLACE FUNCTION pgmb.uninstall()
RETURNS VOID AS $$
DECLARE
	schema_name VARCHAR(64);
BEGIN
	-- find all queues and drop their schemas
	FOR schema_name IN (SELECT q.schema_name FROM pgmb.queues q) LOOP
		-- drop the schemas of queues
		EXECUTE 'DROP SCHEMA ' || quote_ident(schema_name) || ' CASCADE';
	END LOOP;
	-- drop the schema
	DROP SCHEMA pgmb CASCADE;
END
$$ LANGUAGE plpgsql;