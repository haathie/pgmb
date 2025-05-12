CREATE SCHEMA IF NOT EXISTS pgmb;

-- type to create a message that's routed to a queue
CREATE TYPE pgmb.enqueue_msg AS (
	message BYTEA, headers JSONB, consume_at TIMESTAMPTZ
);
-- type to create a message that's routed to an exchange
-- This'll be used to publish messages to exchanges
CREATE TYPE pgmb.publish_msg AS (
	route VARCHAR(64), message BYTEA, headers JSONB, consume_at TIMESTAMPTZ
);
-- type to store an existing message record
CREATE TYPE pgmb.msg_record AS (
	id VARCHAR(32), message BYTEA, headers JSONB
);
CREATE TYPE pgmb.queue_ack_setting AS ENUM ('archive', 'delete');

-- table for exchanges
CREATE TABLE pgmb.exchanges (
	name VARCHAR(64) PRIMARY KEY,
	subscribed_queues VARCHAR(64)[] NOT NULL DEFAULT '{}',
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

CREATE OR REPLACE FUNCTION pgmb.subscribe_to_exchange(
	exchange VARCHAR(64), queue_name VARCHAR(64)
)
RETURNS VOID AS $$
BEGIN
	UPDATE pgmb.exchanges
	SET subscribed_queues = array_append(subscribed_queues, queue_name)
	WHERE name = exchange;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmb.unsubscribe_from_exchange(
	exchange VARCHAR(64), queue_name VARCHAR(64)
)
RETURNS VOID AS $$
BEGIN
	UPDATE pgmb.exchanges
	SET subscribed_queues = array_remove(subscribed_queues, queue_name)
	WHERE name = exchange;
END;
$$ LANGUAGE plpgsql;

-- table for queue metadata
CREATE TABLE pgmb.queues (
	name VARCHAR(64) PRIMARY KEY,
	schema_name VARCHAR(64) NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	ack_setting pgmb.queue_ack_setting DEFAULT 'delete',
	default_headers JSONB DEFAULT '{}'::JSONB
);

-- utility function to create a queue table
CREATE OR REPLACE FUNCTION pgmb.create_queue_table(
	queue_name VARCHAR(64),
	schema_name VARCHAR(64)
) RETURNS VOID AS $$
BEGIN
	-- create the live_messages table
	EXECUTE 'CREATE TABLE ' || quote_ident(schema_name) || '.live_messages (
		id VARCHAR(32) DEFAULT pgmb.create_message_id() PRIMARY KEY,
		message BYTEA NOT NULL,
		headers JSONB NOT NULL DEFAULT ''{}''::JSONB,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)';
END;
$$ LANGUAGE plpgsql;

-- fn to ensure a queue exists. Each queue will have its own schema.
-- The schema will have a table for messages, and a table for consumed messages.
-- @returns true if the queue was created, false if it already exists
CREATE OR REPLACE FUNCTION pgmb.assert_queue(
	queue_name VARCHAR(64),
	ack_setting pgmb.queue_ack_setting DEFAULT 'delete',
	default_headers JSONB DEFAULT '{}'::JSONB
)
RETURNS BOOLEAN AS $$
DECLARE
	-- check if the queue already exists
	schema_name VARCHAR(64);
BEGIN
	schema_name := 'pgmb_q_' || queue_name;
	-- check if the queue already exists
	IF EXISTS (SELECT 1 FROM pgmb.queues WHERE name = queue_name) THEN
		-- queue already exists
		RETURN FALSE;
	END IF;
	-- store in the queues table
	INSERT INTO pgmb.queues (name, schema_name, ack_setting, default_headers)
	VALUES (queue_name, schema_name, ack_setting, default_headers);
	-- create schema
	EXECUTE 'CREATE SCHEMA ' || quote_ident(schema_name);
	-- create the live_messages table
	PERFORM pgmb.create_queue_table(queue_name, schema_name);
	-- create the consumed_messages table
	EXECUTE 'CREATE TABLE ' || quote_ident(schema_name) || '.consumed_messages (
		id VARCHAR(32) PRIMARY KEY,
		message BYTEA NOT NULL,
		headers JSONB NOT NULL DEFAULT ''{}''::JSONB,
		success BOOLEAN NOT NULL,
		consumed_at TIMESTAMPTZ DEFAULT NOW()
	)';
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
	SELECT q.schema_name FROM pgmb.queues q WHERE q.name = queue_name INTO schema_name;
	-- drop the schema
	EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(schema_name) || ' CASCADE';
	-- remove from exchanges
	UPDATE pgmb.exchanges
	SET subscribed_queues = array_remove(subscribed_queues, queue_name)
	WHERE queue_name = ANY(subscribed_queues);
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
BEGIN
	-- get schema name
	SELECT q.schema_name FROM pgmb.queues q WHERE q.name = queue_name INTO schema_name;
	-- drop the live_messages table
	EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(schema_name) || '.live_messages';
	-- create the live_messages table
	PERFORM pgmb.create_queue_table(queue_name, schema_name);
END;
$$ LANGUAGE plpgsql;

-- fn to create a random bigint. Used for message IDs
CREATE OR REPLACE FUNCTION pgmb.create_random_bigint(
	additive DOUBLE PRECISION DEFAULT 0
)
RETURNS BIGINT AS $$
BEGIN
	-- create a random bigint
	RETURN floor((random() + additive) * 10000000000000)::bigint;
END
$$ LANGUAGE plpgsql VOLATILE LEAKPROOF PARALLEL SAFE;

-- fn to create a unique message ID. This'll be the current timestamp + a random number
CREATE OR REPLACE FUNCTION pgmb.create_message_id(
	dt timestamptz DEFAULT clock_timestamp(),
	rand bigint DEFAULT pgmb.create_random_bigint()
)
RETURNS VARCHAR(32) AS $$
BEGIN
	-- create a unique message ID, 16 chars of hex-date
	-- some additional bytes of randomness
	-- ensure the string is always, at most 32 bytes
	RETURN substr(
		'pqm_'
		|| substr(lpad(to_hex((extract(epoch from dt) * 1000000)::bigint), 14, '0'), 1, 14)
		|| lpad(to_hex(rand), 14, '0'),
		1,
		32
	);
END
$$ LANGUAGE plpgsql VOLATILE LEAKPROOF PARALLEL SAFE;

-- fn to extract the date from a message ID.
CREATE OR REPLACE FUNCTION pgmb.extract_date_from_message_id(message_id VARCHAR(64))
RETURNS TIMESTAMPTZ AS $$
BEGIN
	-- convert it to a timestamp
	RETURN to_timestamp(('0x' || substr(message_id, 5, 14))::numeric / 1000000);
END
$$ LANGUAGE plpgsql IMMUTABLE LEAKPROOF PARALLEL SAFE;

-- fn to send multiple messages into a queue
CREATE OR REPLACE FUNCTION pgmb.send_to_queue(
	queue_name VARCHAR(64),
	messages pgmb.enqueue_msg[]
)
RETURNS SETOF VARCHAR(32) AS $$
DECLARE
	-- check if the queue already exists
	schema_name VARCHAR(64);
	default_headers JSONB;
BEGIN
	-- each queue would have its own channel to listen on, so a consumer can listen
	-- to a specific queue. This'll be used to notify the consumer when new messages
	-- are added to the queue.
	PERFORM pg_notify(
		'chn_' || queue_name,
		('{"count":' || array_length(messages, 1)::varchar || '}')::varchar
	);

	-- get schema name and default headers
	SELECT q.schema_name, q.default_headers
	FROM pgmb.queues q
	WHERE q.name = queue_name
	INTO schema_name, default_headers;
	-- Insert the message into the queue and return all message IDs. We use the
	-- ordinality of the array to ensure that each message is inserted in the same
	-- order as it was sent. This is important for the consumer to process the
	-- messages in the same order as they were sent.
	RETURN QUERY
	EXECUTE 'INSERT INTO ' || quote_ident(schema_name) || '.live_messages (id, message, headers)
	SELECT
		pgmb.create_message_id(
			COALESCE(m.consume_at, clock_timestamp()),
			pgmb.create_random_bigint(m.ordinality)
		),
		m.message,
		COALESCE($1, ''{}''::JSONB) || COALESCE(m.headers, ''{}''::JSONB)
	FROM unnest($2) WITH ORDINALITY m 
	RETURNING id;' USING default_headers, messages;
END
$$ LANGUAGE plpgsql;

-- fn to positively/negatively ack 1 or more messages.
-- If "success": will send to consumed_messages or delete,
--  based on the queue's ack_setting
-- If "failure": will ack the message, and requeue it if retries are left
CREATE OR REPLACE FUNCTION pgmb.ack_msgs(
	queue_name VARCHAR(64),
	success BOOLEAN,
	ids VARCHAR(32)[]
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
		PERFORM pgmb.send_to_queue(
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
		WHERE id <= pgmb.create_message_id(rand=>999999999999)
		ORDER BY id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1'
	USING limit_count;
END
$$ LANGUAGE plpgsql;

-- fn to publish a message to 1 or more exchanges.
-- Will find all queues subscribed to it and insert the message into
-- each of them.
CREATE OR REPLACE FUNCTION pgmb.publish_to_exchange(
	messages pgmb.publish_msg[]
)
RETURNS SETOF VARCHAR(32) AS $$
DECLARE
	update_count INTEGER;
BEGIN
	-- Insert the message into all subscribed queues and return all message IDs
	RETURN QUERY
	WITH expanded_msgs AS (
		SELECT
			unnest(subscribed_queues) AS queue_name,
			e.name as exchange_name,
			m.message,
			m.headers,
			m.consume_at
		FROM pgmb.exchanges e
		INNER JOIN unnest(messages) AS m ON m.route = e.name
	)
	SELECT
		pgmb.send_to_queue(
			m.queue_name,
			ARRAY_AGG(
				(
					m.message,
					JSONB_SET(
						COALESCE(m.headers, '{}'::JSONB), '{exchange}', TO_JSONB(m.exchange_name)
					),
					m.consume_at
				)::pgmb.enqueue_msg
			)
		)
	FROM expanded_msgs m
	GROUP BY m.queue_name;
END
$$ LANGUAGE plpgsql;

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