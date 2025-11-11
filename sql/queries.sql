/* @name createReader */
INSERT INTO pgmb2.readers (id)
VALUES (:readerId!);

/* @name createSubscription */
INSERT INTO pgmb2.subscriptions (reader_id, conditions_sql, metadata)
VALUES (:readerId!, COALESCE(:conditionsSql, 'TRUE'), COALESCE(:metadata::jsonb, '{}'))
RETURNING id AS "id!";

/* @name readReaderXidStates */
SELECT
	reader_id AS "readerId!",
	xid AS "xid!",
	completed_at AS "completedAt!"
FROM pgmb2.reader_xid_state
WHERE reader_id = :readerId!;

/* @name readNextEventsForSubscriptions */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!",
	subscription_ids AS "subscriptionIds!"
FROM pgmb2.read_next_events_for_subscriptions(
	:readerId!,
	:chunkSize!
);

/* @name readNextEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!"
FROM pgmb2.read_next_events(
	:readerId!,
	:chunkSize!
);

/* @name writeEvents */
INSERT INTO pgmb2.events (topic, payload, metadata)
SELECT
	topic,
	payload,
	metadata
FROM unnest(
	:topics!::TEXT[],
	:payloads!::JSONB[],
	:metadatas!::JSONB[]
) AS t(topic, payload, metadata)
RETURNING id AS "id!";

/* @name writeScheduledEvents */
INSERT INTO pgmb2.events (id, topic, payload, metadata)
SELECT
	pgmb2.create_event_id( COALESCE(ts, clock_timestamp()) ),
	topic,
	payload,
	metadata
FROM unnest(
	:ts!::TIMESTAMPTZ[],
	:topics!::TEXT[],
	:payloads!::JSONB[],
	:metadatas!::JSONB[]
) AS t(ts, topic, payload, metadata)
RETURNING id AS "id!";

/* @name removeTemporarySubscriptions */
DELETE FROM pgmb2.subscriptions
WHERE reader_id = :readerId! AND is_temporary;
