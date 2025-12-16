/* @name assertGroup */
INSERT INTO pgmb.subscription_groups (id)
VALUES (:id!)
ON CONFLICT DO NOTHING;

/* @name assertSubscription */
INSERT INTO pgmb.subscriptions
	AS s(group_id, conditions_sql, params, expiry_interval)
VALUES (
	:groupId!,
	COALESCE(:conditionsSql, 'TRUE'),
	COALESCE(:params::jsonb, '{}'),
	:expiryInterval::interval
)
ON CONFLICT (identity) DO UPDATE
SET
	-- set expiry_interval to the new value only if it's greater than the existing one
	-- or if the new value is NULL (indicating no expiration)
	expiry_interval = CASE
		WHEN EXCLUDED.expiry_interval IS NULL OR s.expiry_interval IS NULL
			THEN NULL
		ELSE
			GREATEST(s.expiry_interval, EXCLUDED.expiry_interval)
	END,
	last_active_at = NOW()
RETURNING id AS "id!";

/*
 @name deleteSubscriptions
 @param ids -> (...)
 */
DELETE FROM pgmb.subscriptions
WHERE id IN :ids!;

/*
 @name markSubscriptionsActive
*/
UPDATE pgmb.subscriptions
SET
	last_active_at = NOW()
WHERE id IN (SELECT * FROM unnest(:ids!::pgmb.subscription_id[]));

/* @name pollForEvents */
SELECT count AS "count!" FROM pgmb.poll_for_events() AS count;

/* @name readNextEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!",
	subscription_ids::text[] AS "subscriptionIds!",
	next_cursor AS "nextCursor!"
FROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!);

/* @name readNextEventsText */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload::text AS "payload!"
FROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!);

/* @name replayEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!"
FROM pgmb.replay_events(
	:groupId!,
	:subscriptionId!,
	:fromEventId!::pgmb.event_id,
	:maxEvents!
);

/* @name setGroupCursor */
SELECT pgmb.set_group_cursor(
	:groupId!,
	:cursor!::pgmb.event_id,
	:releaseLock::boolean
) AS "success!";

/* @name writeEvents */
INSERT INTO pgmb.events (topic, payload, metadata)
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
INSERT INTO pgmb.events (id, topic, payload, metadata)
SELECT
	pgmb.create_event_id(COALESCE(ts, clock_timestamp()), pgmb.create_random_bigint()),
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

/* @name scheduleEventRetry */
INSERT INTO pgmb.events (id, topic, payload, subscription_id)
SELECT
	pgmb.create_event_id(
		NOW() + (:delayInterval!::INTERVAL),
		pgmb.create_random_bigint()
	),
	'pgmb-retry',
	jsonb_build_object(
		'ids',
		:ids!::pgmb.event_id[],
		'retryNumber',
		:retryNumber!::int
	),
	:subscriptionId!::pgmb.subscription_id
RETURNING id AS "id!";

/* @name findEvents */
SELECT
	id AS "id!",
	topic AS "topic!",
	payload AS "payload!",
	metadata AS "metadata!"
FROM pgmb.events
WHERE id = ANY(:ids!::pgmb.event_id[]);

/* @name removeExpiredSubscriptions */
WITH deleted AS (
	DELETE FROM pgmb.subscriptions
	WHERE group_id = :groupId!
		AND expiry_interval IS NOT NULL
		AND pgmb.add_interval_imm(last_active_at, expiry_interval) < NOW()
		AND id NOT IN (select * from unnest(:activeIds!::pgmb.subscription_id[]))
	RETURNING id
)
SELECT COUNT(*) AS "deleted!" FROM deleted;

/* @name reenqueueEventsForSubscription */
SELECT pgmb.reenqueue_events_for_subscription(
	:eventIds!::text[],
	:subscriptionId!,
	:offsetInterval!::INTERVAL
) AS "reenqueuedEventIds!";

/* @name maintainEventsTable */
SELECT pgmb.maintain_events_table();
