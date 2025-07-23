CREATE OR REPLACE FUNCTION pgmb.ack_msgs(
	queue_name VARCHAR(64),
	success BOOLEAN,
	ids VARCHAR(22)[]
)
RETURNS VOID AS $$
DECLARE
	schema_name VARCHAR(64);
	ack_setting pgmb.queue_ack_setting;
	query_str TEXT;
	deleted_msg_count int;
BEGIN
	-- get schema name and ack setting
	SELECT q.schema_name, q.ack_setting
	FROM pgmb.queues q
	WHERE q.name = queue_name
	INTO schema_name, ack_setting;

	-- we'll construct a single CTE query that'll delete messages,
	-- requeue them if needed, and archive them if ack_setting is 'archive'.
	query_str := 'WITH deleted_msgs AS (
		DELETE FROM ' || quote_ident(schema_name) || '.live_messages
		WHERE id = ANY($1)
		RETURNING id, message, headers
	)';

	-- re-insert messages that can be retried
	IF NOT success THEN
		query_str := query_str || ',
		requeued AS (
			INSERT INTO '
				|| quote_ident(schema_name)
				|| '.live_messages (id, message, headers)
			SELECT
				pgmb.create_message_id(
					clock_timestamp() + (interval ''1 second'') * (t.headers->''retriesLeftS''->0)::int,
					rn
				),
				t.message,
				t.headers
					-- set retriesLeftS to the next retry
					|| jsonb_build_object(''retriesLeftS'', (t.headers->''retriesLeftS'') #- ''{0}'')
					-- set the originalMessageId
					-- to the original message ID if it exists
					|| jsonb_build_object(
						''originalMessageId'', COALESCE(t.headers->''originalMessageId'', to_jsonb(t.id))
					)
					-- set the tries
					|| jsonb_build_object(
						''tries'',
						CASE
							WHEN jsonb_typeof(t.headers->''tries'') = ''number'' THEN
								to_jsonb((t.headers->>''tries'')::INTEGER + 1)
							ELSE
								to_jsonb(1)
						END
					)
			FROM (select *, row_number() over () AS rn FROM deleted_msgs) t
			WHERE jsonb_typeof(t.headers -> ''retriesLeftS'' -> 0) = ''number''
			RETURNING id
		),
		requeued_notify AS (
			SELECT pg_notify(
				''chn_' || queue_name || ''',
				''{"count":'' || (select count(*) from requeued)::varchar || ''}''
			)
		)
		';
	END IF;

	IF ack_setting = 'archive' THEN
		-- Delete the messages from live_messages and insert them into
		-- consumed_messages in one operation,
		-- if the queue's ack_setting is set to 'archive'
		query_str := query_str || ',
		archived_records AS (
			INSERT INTO ' || quote_ident(schema_name) ||  '.consumed_messages
				(id, message, headers, success)
			SELECT t.id, t.message, t.headers, $2::boolean
			FROM deleted_msgs t
		)';
	END IF;

	query_str := query_str || '
	SELECT COUNT(*) FROM deleted_msgs';

	EXECUTE query_str USING ids, success INTO deleted_msg_count;

	-- Raise exception if no rows were affected
	IF deleted_msg_count != array_length(ids, 1) THEN
		RAISE EXCEPTION 'Only removed % out of % expected message(s).',
			deleted_msg_count, array_length(ids, 1);
	END IF;
END
$$ LANGUAGE plpgsql;