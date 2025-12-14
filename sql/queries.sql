/* @name assertGroup */
INSERT INTO pgmb2.subscription_groups (id)
VALUES (:id!)
ON CONFLICT DO NOTHING;

/* @name assertSubscription */
INSERT INTO pgmb2.subscriptions AS s(group_id, conditions_sql, params, expires_at)
VALUES (
	:groupId!,
	COALESCE(:conditionsSql, 'TRUE'),
	COALESCE(:params::jsonb, '{}'),
	:expiresAt
)
ON CONFLICT (identity) DO UPDATE
SET
	-- set expires_at to the new value only if it's greater than the existing one
	-- or if the new value is NULL (indicating no expiration)
	expires_at = CASE
		WHEN EXCLUDED.expires_at IS NULL OR s.expires_at IS NULL THEN NULL
		ELSE GREATEST(s.expires_at, EXCLUDED.expires_at)
	END
RETURNING id AS "id!", expires_at AS "expiresAt!";

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

/* @name replayEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!"
FROM pgmb2.replay_events(
	:groupId!,
	:subscriptionId!,
	:fromEventId!::pgmb2.event_id,
	:maxEvents!
);

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

/* @name removeExpiredSubscriptions */
DELETE FROM pgmb2.subscriptions
WHERE group_id = :groupId! AND expires_at IS NOT NULL AND expires_at < NOW();

/* @name reenqueueEventsForSubscription */
SELECT pgmb2.reenqueue_events_for_subscription(
	:eventIds!::text[],
	:subscriptionId!,
	:offsetInterval!::INTERVAL
) AS "reenqueuedEventIds!";

/* @name maintainEventsTable */
SELECT pgmb2.maintain_events_table();
