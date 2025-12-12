/* @name assertGroup */
INSERT INTO pgmb2.subscription_groups (id)
VALUES (:id!)
ON CONFLICT DO NOTHING;

/* @name assertSubscription */
INSERT INTO pgmb2.subscriptions (group_id, conditions_sql, metadata, type)
VALUES (
	:groupId!,
	COALESCE(:conditionsSql, 'TRUE'),
	COALESCE(:metadata::jsonb, '{}'),
	COALESCE(:type::pgmb2.subscription_type, 'custom')
)
ON CONFLICT (id) DO UPDATE
SET
	group_id = EXCLUDED.group_id,
	conditions_sql = EXCLUDED.conditions_sql,
	metadata = EXCLUDED.metadata
RETURNING id AS "id!";

/*
 @name deleteSubscriptions
 @param ids -> (...)
 */
DELETE FROM pgmb2.subscriptions
WHERE id IN :ids!;

/* @name pollForEvents */
SELECT count AS "count!" FROM pgmb2.poll_for_events() AS count;

/* @name readNextEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!",
	subscription_ids::text[] AS "subscriptionIds!",
	subscription_metadatas AS "subscriptionMetadatas!",
	next_cursor AS "nextCursor!"
FROM pgmb2.read_next_events(:groupId!, :chunkSize!);

/* @name readNextEventsText */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload::text AS "payload!"
FROM pgmb2.read_next_events(:groupId!, :chunkSize!);

/* @name setGroupCursor */
SELECT pgmb2.set_group_cursor(:groupId!,	:cursor!::pgmb2.event_id) AS "success!";

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
	pgmb2.create_event_id(COALESCE(ts, clock_timestamp()), pgmb2.create_random_bigint()),
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

/* @name removeHttpSubscriptionsInGroup */
DELETE FROM pgmb2.subscriptions
WHERE group_id = :groupId! AND type = 'http';

/* @name reenqueueEventsForSubscription */
SELECT pgmb2.reenqueue_events_for_subscription(
	:eventIds!::text[],
	:subscriptionId!,
	:offsetInterval!::INTERVAL
) AS "reenqueuedEventIds!";

/* @name maintainEventsTable */
SELECT pgmb2.maintain_events_table();
