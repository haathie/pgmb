/** Types generated for queries found in "sql/queries.sql" */
import { PreparedQuery } from '@pgtyped/runtime';

export type DateOrString = Date | string;

export type DateOrStringArray = (DateOrString)[];

export type stringArray = (string)[];

export type unknownArray = (unknown)[];

/** 'AssertGroup' parameters type */
export interface IAssertGroupParams {
  id: string;
}

/** 'AssertGroup' return type */
export type IAssertGroupResult = void;

/** 'AssertGroup' query type */
export interface IAssertGroupQuery {
  params: IAssertGroupParams;
  result: IAssertGroupResult;
}

const assertGroupIR: any = {"usedParamSet":{"id":true},"params":[{"name":"id","required":true,"transform":{"type":"scalar"},"locs":[{"a":50,"b":53}]}],"statement":"INSERT INTO pgmb.subscription_groups (id)\nVALUES (:id!)\nON CONFLICT DO NOTHING"};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb.subscription_groups (id)
 * VALUES (:id!)
 * ON CONFLICT DO NOTHING
 * ```
 */
export const assertGroup = new PreparedQuery<IAssertGroupParams,IAssertGroupResult>(assertGroupIR);


/** 'AssertSubscription' parameters type */
export interface IAssertSubscriptionParams {
  conditionsSql?: string | null | void;
  expiryInterval?: DateOrString | null | void;
  groupId: string;
  params?: unknown | null | void;
}

/** 'AssertSubscription' return type */
export interface IAssertSubscriptionResult {
  id: string;
}

/** 'AssertSubscription' query type */
export interface IAssertSubscriptionQuery {
  params: IAssertSubscriptionParams;
  result: IAssertSubscriptionResult;
}

const assertSubscriptionIR: any = {"usedParamSet":{"groupId":true,"conditionsSql":true,"params":true,"expiryInterval":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":98,"b":106}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":119,"b":132}]},{"name":"params","required":false,"transform":{"type":"scalar"},"locs":[{"a":154,"b":160}]},{"name":"expiryInterval","required":false,"transform":{"type":"scalar"},"locs":[{"a":178,"b":192}]}],"statement":"INSERT INTO pgmb.subscriptions\n\tAS s(group_id, conditions_sql, params, expiry_interval)\nVALUES (\n\t:groupId!,\n\tCOALESCE(:conditionsSql, 'TRUE'),\n\tCOALESCE(:params::jsonb, '{}'),\n\t:expiryInterval::interval\n)\nON CONFLICT (identity) DO UPDATE\nSET\n\t-- set expiry_interval to the new value only if it's greater than the existing one\n\t-- or if the new value is NULL (indicating no expiration)\n\texpiry_interval = CASE\n\t\tWHEN EXCLUDED.expiry_interval IS NULL OR s.expiry_interval IS NULL\n\t\t\tTHEN NULL\n\t\tELSE\n\t\t\tGREATEST(s.expiry_interval, EXCLUDED.expiry_interval)\n\tEND,\n\tlast_active_at = NOW()\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb.subscriptions
 * 	AS s(group_id, conditions_sql, params, expiry_interval)
 * VALUES (
 * 	:groupId!,
 * 	COALESCE(:conditionsSql, 'TRUE'),
 * 	COALESCE(:params::jsonb, '{}'),
 * 	:expiryInterval::interval
 * )
 * ON CONFLICT (identity) DO UPDATE
 * SET
 * 	-- set expiry_interval to the new value only if it's greater than the existing one
 * 	-- or if the new value is NULL (indicating no expiration)
 * 	expiry_interval = CASE
 * 		WHEN EXCLUDED.expiry_interval IS NULL OR s.expiry_interval IS NULL
 * 			THEN NULL
 * 		ELSE
 * 			GREATEST(s.expiry_interval, EXCLUDED.expiry_interval)
 * 	END,
 * 	last_active_at = NOW()
 * RETURNING id AS "id!"
 * ```
 */
export const assertSubscription = new PreparedQuery<IAssertSubscriptionParams,IAssertSubscriptionResult>(assertSubscriptionIR);


/** 'DeleteSubscriptions' parameters type */
export interface IDeleteSubscriptionsParams {
  ids: readonly (string)[];
}

/** 'DeleteSubscriptions' return type */
export type IDeleteSubscriptionsResult = void;

/** 'DeleteSubscriptions' query type */
export interface IDeleteSubscriptionsQuery {
  params: IDeleteSubscriptionsParams;
  result: IDeleteSubscriptionsResult;
}

const deleteSubscriptionsIR: any = {"usedParamSet":{"ids":true},"params":[{"name":"ids","required":true,"transform":{"type":"array_spread"},"locs":[{"a":43,"b":47}]}],"statement":"DELETE FROM pgmb.subscriptions\nWHERE id IN :ids!"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb.subscriptions
 * WHERE id IN :ids!
 * ```
 */
export const deleteSubscriptions = new PreparedQuery<IDeleteSubscriptionsParams,IDeleteSubscriptionsResult>(deleteSubscriptionsIR);


/** 'MarkSubscriptionsActive' parameters type */
export interface IMarkSubscriptionsActiveParams {
  ids: stringArray;
}

/** 'MarkSubscriptionsActive' return type */
export type IMarkSubscriptionsActiveResult = void;

/** 'MarkSubscriptionsActive' query type */
export interface IMarkSubscriptionsActiveQuery {
  params: IMarkSubscriptionsActiveParams;
  result: IMarkSubscriptionsActiveResult;
}

const markSubscriptionsActiveIR: any = {"usedParamSet":{"ids":true},"params":[{"name":"ids","required":true,"transform":{"type":"scalar"},"locs":[{"a":88,"b":92}]}],"statement":"UPDATE pgmb.subscriptions\nSET\n\tlast_active_at = NOW()\nWHERE id IN (SELECT * FROM unnest(:ids!::pgmb.subscription_id[]))"};

/**
 * Query generated from SQL:
 * ```
 * UPDATE pgmb.subscriptions
 * SET
 * 	last_active_at = NOW()
 * WHERE id IN (SELECT * FROM unnest(:ids!::pgmb.subscription_id[]))
 * ```
 */
export const markSubscriptionsActive = new PreparedQuery<IMarkSubscriptionsActiveParams,IMarkSubscriptionsActiveResult>(markSubscriptionsActiveIR);


/** 'PollForEvents' parameters type */
export type IPollForEventsParams = void;

/** 'PollForEvents' return type */
export interface IPollForEventsResult {
  count: number;
}

/** 'PollForEvents' query type */
export interface IPollForEventsQuery {
  params: IPollForEventsParams;
  result: IPollForEventsResult;
}

const pollForEventsIR: any = {"usedParamSet":{},"params":[],"statement":"SELECT count AS \"count!\" FROM pgmb.poll_for_events() AS count"};

/**
 * Query generated from SQL:
 * ```
 * SELECT count AS "count!" FROM pgmb.poll_for_events() AS count
 * ```
 */
export const pollForEvents = new PreparedQuery<IPollForEventsParams,IPollForEventsResult>(pollForEventsIR);


/** 'ReadNextEvents' parameters type */
export interface IReadNextEventsParams {
  chunkSize: number;
  cursor?: string | null | void;
  groupId: string;
}

/** 'ReadNextEvents' return type */
export interface IReadNextEventsResult {
  id: string;
  metadata: unknown;
  nextCursor: string;
  payload: unknown;
  subscriptionIds: stringArray;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"groupId":true,"cursor":true,"chunkSize":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":197,"b":205}]},{"name":"cursor","required":false,"transform":{"type":"scalar"},"locs":[{"a":208,"b":214}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":217,"b":227}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\",\n\tsubscription_ids::text[] AS \"subscriptionIds!\",\n\tnext_cursor AS \"nextCursor!\"\nFROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!",
 * 	subscription_ids::text[] AS "subscriptionIds!",
 * 	next_cursor AS "nextCursor!"
 * FROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!)
 * ```
 */
export const readNextEvents = new PreparedQuery<IReadNextEventsParams,IReadNextEventsResult>(readNextEventsIR);


/** 'ReadNextEventsText' parameters type */
export interface IReadNextEventsTextParams {
  chunkSize: number;
  cursor?: string | null | void;
  groupId: string;
}

/** 'ReadNextEventsText' return type */
export interface IReadNextEventsTextResult {
  id: string;
  payload: string;
  topic: string;
}

/** 'ReadNextEventsText' query type */
export interface IReadNextEventsTextQuery {
  params: IReadNextEventsTextParams;
  result: IReadNextEventsTextResult;
}

const readNextEventsTextIR: any = {"usedParamSet":{"groupId":true,"cursor":true,"chunkSize":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":97,"b":105}]},{"name":"cursor","required":false,"transform":{"type":"scalar"},"locs":[{"a":108,"b":114}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":117,"b":127}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\"\nFROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!"
 * FROM pgmb.read_next_events(:groupId!, :cursor, :chunkSize!)
 * ```
 */
export const readNextEventsText = new PreparedQuery<IReadNextEventsTextParams,IReadNextEventsTextResult>(readNextEventsTextIR);


/** 'ReplayEvents' parameters type */
export interface IReplayEventsParams {
  fromEventId: string;
  groupId: string;
  maxEvents: number;
  subscriptionId: string;
}

/** 'ReplayEvents' return type */
export interface IReplayEventsResult {
  id: string;
  metadata: unknown;
  payload: unknown;
  topic: string;
}

/** 'ReplayEvents' query type */
export interface IReplayEventsQuery {
  params: IReplayEventsParams;
  result: IReplayEventsResult;
}

const replayEventsIR: any = {"usedParamSet":{"groupId":true,"subscriptionId":true,"fromEventId":true,"maxEvents":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":116,"b":124}]},{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":128,"b":143}]},{"name":"fromEventId","required":true,"transform":{"type":"scalar"},"locs":[{"a":147,"b":159}]},{"name":"maxEvents","required":true,"transform":{"type":"scalar"},"locs":[{"a":178,"b":188}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\"\nFROM pgmb.replay_events(\n\t:groupId!,\n\t:subscriptionId!,\n\t:fromEventId!::pgmb.event_id,\n\t:maxEvents!\n)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!"
 * FROM pgmb.replay_events(
 * 	:groupId!,
 * 	:subscriptionId!,
 * 	:fromEventId!::pgmb.event_id,
 * 	:maxEvents!
 * )
 * ```
 */
export const replayEvents = new PreparedQuery<IReplayEventsParams,IReplayEventsResult>(replayEventsIR);


/** 'SetGroupCursor' parameters type */
export interface ISetGroupCursorParams {
  cursor: string;
  groupId: string;
  releaseLock?: boolean | null | void;
}

/** 'SetGroupCursor' return type */
export interface ISetGroupCursorResult {
  success: undefined;
}

/** 'SetGroupCursor' query type */
export interface ISetGroupCursorQuery {
  params: ISetGroupCursorParams;
  result: ISetGroupCursorResult;
}

const setGroupCursorIR: any = {"usedParamSet":{"groupId":true,"cursor":true,"releaseLock":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":31,"b":39}]},{"name":"cursor","required":true,"transform":{"type":"scalar"},"locs":[{"a":43,"b":50}]},{"name":"releaseLock","required":false,"transform":{"type":"scalar"},"locs":[{"a":69,"b":80}]}],"statement":"SELECT pgmb.set_group_cursor(\n\t:groupId!,\n\t:cursor!::pgmb.event_id,\n\t:releaseLock::boolean\n) AS \"success!\""};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb.set_group_cursor(
 * 	:groupId!,
 * 	:cursor!::pgmb.event_id,
 * 	:releaseLock::boolean
 * ) AS "success!"
 * ```
 */
export const setGroupCursor = new PreparedQuery<ISetGroupCursorParams,ISetGroupCursorResult>(setGroupCursorIR);


/** 'ReleaseGroupLock' parameters type */
export interface IReleaseGroupLockParams {
  groupId: string;
}

/** 'ReleaseGroupLock' return type */
export interface IReleaseGroupLockResult {
  success: undefined;
}

/** 'ReleaseGroupLock' query type */
export interface IReleaseGroupLockQuery {
  params: IReleaseGroupLockParams;
  result: IReleaseGroupLockResult;
}

const releaseGroupLockIR: any = {"usedParamSet":{"groupId":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":31,"b":39}]}],"statement":"SELECT pgmb.release_group_lock(:groupId!) AS \"success!\""};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb.release_group_lock(:groupId!) AS "success!"
 * ```
 */
export const releaseGroupLock = new PreparedQuery<IReleaseGroupLockParams,IReleaseGroupLockResult>(releaseGroupLockIR);


/** 'WriteEvents' parameters type */
export interface IWriteEventsParams {
  metadatas: unknownArray;
  payloads: unknownArray;
  topics: stringArray;
}

/** 'WriteEvents' return type */
export interface IWriteEventsResult {
  id: string;
}

/** 'WriteEvents' query type */
export interface IWriteEventsQuery {
  params: IWriteEventsParams;
  result: IWriteEventsResult;
}

const writeEventsIR: any = {"usedParamSet":{"topics":true,"payloads":true,"metadatas":true},"params":[{"name":"topics","required":true,"transform":{"type":"scalar"},"locs":[{"a":100,"b":107}]},{"name":"payloads","required":true,"transform":{"type":"scalar"},"locs":[{"a":119,"b":128}]},{"name":"metadatas","required":true,"transform":{"type":"scalar"},"locs":[{"a":141,"b":151}]}],"statement":"INSERT INTO pgmb.events (topic, payload, metadata)\nSELECT\n\ttopic,\n\tpayload,\n\tmetadata\nFROM unnest(\n\t:topics!::TEXT[],\n\t:payloads!::JSONB[],\n\t:metadatas!::JSONB[]\n) AS t(topic, payload, metadata)\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb.events (topic, payload, metadata)
 * SELECT
 * 	topic,
 * 	payload,
 * 	metadata
 * FROM unnest(
 * 	:topics!::TEXT[],
 * 	:payloads!::JSONB[],
 * 	:metadatas!::JSONB[]
 * ) AS t(topic, payload, metadata)
 * RETURNING id AS "id!"
 * ```
 */
export const writeEvents = new PreparedQuery<IWriteEventsParams,IWriteEventsResult>(writeEventsIR);


/** 'WriteScheduledEvents' parameters type */
export interface IWriteScheduledEventsParams {
  metadatas: unknownArray;
  payloads: unknownArray;
  topics: stringArray;
  ts: DateOrStringArray;
}

/** 'WriteScheduledEvents' return type */
export interface IWriteScheduledEventsResult {
  id: string;
}

/** 'WriteScheduledEvents' query type */
export interface IWriteScheduledEventsQuery {
  params: IWriteScheduledEventsParams;
  result: IWriteScheduledEventsResult;
}

const writeScheduledEventsIR: any = {"usedParamSet":{"ts":true,"topics":true,"payloads":true,"metadatas":true},"params":[{"name":"ts","required":true,"transform":{"type":"scalar"},"locs":[{"a":189,"b":192}]},{"name":"topics","required":true,"transform":{"type":"scalar"},"locs":[{"a":211,"b":218}]},{"name":"payloads","required":true,"transform":{"type":"scalar"},"locs":[{"a":230,"b":239}]},{"name":"metadatas","required":true,"transform":{"type":"scalar"},"locs":[{"a":252,"b":262}]}],"statement":"INSERT INTO pgmb.events (id, topic, payload, metadata)\nSELECT\n\tpgmb.create_event_id(COALESCE(ts, clock_timestamp()), pgmb.create_random_bigint()),\n\ttopic,\n\tpayload,\n\tmetadata\nFROM unnest(\n\t:ts!::TIMESTAMPTZ[],\n\t:topics!::TEXT[],\n\t:payloads!::JSONB[],\n\t:metadatas!::JSONB[]\n) AS t(ts, topic, payload, metadata)\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb.events (id, topic, payload, metadata)
 * SELECT
 * 	pgmb.create_event_id(COALESCE(ts, clock_timestamp()), pgmb.create_random_bigint()),
 * 	topic,
 * 	payload,
 * 	metadata
 * FROM unnest(
 * 	:ts!::TIMESTAMPTZ[],
 * 	:topics!::TEXT[],
 * 	:payloads!::JSONB[],
 * 	:metadatas!::JSONB[]
 * ) AS t(ts, topic, payload, metadata)
 * RETURNING id AS "id!"
 * ```
 */
export const writeScheduledEvents = new PreparedQuery<IWriteScheduledEventsParams,IWriteScheduledEventsResult>(writeScheduledEventsIR);


/** 'ScheduleEventRetry' parameters type */
export interface IScheduleEventRetryParams {
  delayInterval: DateOrString;
  ids: stringArray;
  retryNumber: number;
  subscriptionId: string;
}

/** 'ScheduleEventRetry' return type */
export interface IScheduleEventRetryResult {
  id: string;
}

/** 'ScheduleEventRetry' query type */
export interface IScheduleEventRetryQuery {
  params: IScheduleEventRetryParams;
  result: IScheduleEventRetryResult;
}

const scheduleEventRetryIR: any = {"usedParamSet":{"delayInterval":true,"ids":true,"retryNumber":true,"subscriptionId":true},"params":[{"name":"delayInterval","required":true,"transform":{"type":"scalar"},"locs":[{"a":103,"b":117}]},{"name":"ids","required":true,"transform":{"type":"scalar"},"locs":[{"a":212,"b":216}]},{"name":"retryNumber","required":true,"transform":{"type":"scalar"},"locs":[{"a":255,"b":267}]},{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":279,"b":294}]}],"statement":"INSERT INTO pgmb.events (id, topic, payload, subscription_id)\nSELECT\n\tpgmb.create_event_id(\n\t\tNOW() + (:delayInterval!::INTERVAL),\n\t\tpgmb.create_random_bigint()\n\t),\n\t'pgmb-retry',\n\tjsonb_build_object(\n\t\t'ids',\n\t\t:ids!::pgmb.event_id[],\n\t\t'retryNumber',\n\t\t:retryNumber!::int\n\t),\n\t:subscriptionId!::pgmb.subscription_id\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb.events (id, topic, payload, subscription_id)
 * SELECT
 * 	pgmb.create_event_id(
 * 		NOW() + (:delayInterval!::INTERVAL),
 * 		pgmb.create_random_bigint()
 * 	),
 * 	'pgmb-retry',
 * 	jsonb_build_object(
 * 		'ids',
 * 		:ids!::pgmb.event_id[],
 * 		'retryNumber',
 * 		:retryNumber!::int
 * 	),
 * 	:subscriptionId!::pgmb.subscription_id
 * RETURNING id AS "id!"
 * ```
 */
export const scheduleEventRetry = new PreparedQuery<IScheduleEventRetryParams,IScheduleEventRetryResult>(scheduleEventRetryIR);


/** 'FindEvents' parameters type */
export interface IFindEventsParams {
  ids: stringArray;
}

/** 'FindEvents' return type */
export interface IFindEventsResult {
  id: string;
  metadata: unknown;
  payload: unknown;
  topic: string;
}

/** 'FindEvents' query type */
export interface IFindEventsQuery {
  params: IFindEventsParams;
  result: IFindEventsResult;
}

const findEventsIR: any = {"usedParamSet":{"ids":true},"params":[{"name":"ids","required":true,"transform":{"type":"scalar"},"locs":[{"a":122,"b":126}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\"\nFROM pgmb.events\nWHERE id = ANY(:ids!::pgmb.event_id[])"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!"
 * FROM pgmb.events
 * WHERE id = ANY(:ids!::pgmb.event_id[])
 * ```
 */
export const findEvents = new PreparedQuery<IFindEventsParams,IFindEventsResult>(findEventsIR);


/** 'RemoveExpiredSubscriptions' parameters type */
export interface IRemoveExpiredSubscriptionsParams {
  activeIds: stringArray;
  groupId: string;
}

/** 'RemoveExpiredSubscriptions' return type */
export interface IRemoveExpiredSubscriptionsResult {
  deleted: string;
}

/** 'RemoveExpiredSubscriptions' query type */
export interface IRemoveExpiredSubscriptionsQuery {
  params: IRemoveExpiredSubscriptionsParams;
  result: IRemoveExpiredSubscriptionsResult;
}

const removeExpiredSubscriptionsIR: any = {"usedParamSet":{"groupId":true,"activeIds":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":68,"b":76}]},{"name":"activeIds","required":true,"transform":{"type":"scalar"},"locs":[{"a":219,"b":229}]}],"statement":"WITH deleted AS (\n\tDELETE FROM pgmb.subscriptions\n\tWHERE group_id = :groupId!\n\t\tAND expiry_interval IS NOT NULL\n\t\tAND pgmb.add_interval_imm(last_active_at, expiry_interval) < NOW()\n\t\tAND id NOT IN (select * from unnest(:activeIds!::pgmb.subscription_id[]))\n\tRETURNING id\n)\nSELECT COUNT(*) AS \"deleted!\" FROM deleted"};

/**
 * Query generated from SQL:
 * ```
 * WITH deleted AS (
 * 	DELETE FROM pgmb.subscriptions
 * 	WHERE group_id = :groupId!
 * 		AND expiry_interval IS NOT NULL
 * 		AND pgmb.add_interval_imm(last_active_at, expiry_interval) < NOW()
 * 		AND id NOT IN (select * from unnest(:activeIds!::pgmb.subscription_id[]))
 * 	RETURNING id
 * )
 * SELECT COUNT(*) AS "deleted!" FROM deleted
 * ```
 */
export const removeExpiredSubscriptions = new PreparedQuery<IRemoveExpiredSubscriptionsParams,IRemoveExpiredSubscriptionsResult>(removeExpiredSubscriptionsIR);


/** 'ReenqueueEventsForSubscription' parameters type */
export interface IReenqueueEventsForSubscriptionParams {
  eventIds: stringArray;
  offsetInterval: DateOrString;
  subscriptionId: string;
}

/** 'ReenqueueEventsForSubscription' return type */
export interface IReenqueueEventsForSubscriptionResult {
  reenqueuedEventIds: string;
}

/** 'ReenqueueEventsForSubscription' query type */
export interface IReenqueueEventsForSubscriptionQuery {
  params: IReenqueueEventsForSubscriptionParams;
  result: IReenqueueEventsForSubscriptionResult;
}

const reenqueueEventsForSubscriptionIR: any = {"usedParamSet":{"eventIds":true,"subscriptionId":true,"offsetInterval":true},"params":[{"name":"eventIds","required":true,"transform":{"type":"scalar"},"locs":[{"a":48,"b":57}]},{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":69,"b":84}]},{"name":"offsetInterval","required":true,"transform":{"type":"scalar"},"locs":[{"a":88,"b":103}]}],"statement":"SELECT pgmb.reenqueue_events_for_subscription(\n\t:eventIds!::text[],\n\t:subscriptionId!,\n\t:offsetInterval!::INTERVAL\n) AS \"reenqueuedEventIds!\""};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb.reenqueue_events_for_subscription(
 * 	:eventIds!::text[],
 * 	:subscriptionId!,
 * 	:offsetInterval!::INTERVAL
 * ) AS "reenqueuedEventIds!"
 * ```
 */
export const reenqueueEventsForSubscription = new PreparedQuery<IReenqueueEventsForSubscriptionParams,IReenqueueEventsForSubscriptionResult>(reenqueueEventsForSubscriptionIR);


/** 'MaintainEventsTable' parameters type */
export type IMaintainEventsTableParams = void;

/** 'MaintainEventsTable' return type */
export interface IMaintainEventsTableResult {
  maintainEventsTable: undefined | null;
}

/** 'MaintainEventsTable' query type */
export interface IMaintainEventsTableQuery {
  params: IMaintainEventsTableParams;
  result: IMaintainEventsTableResult;
}

const maintainEventsTableIR: any = {"usedParamSet":{},"params":[],"statement":"SELECT pgmb.maintain_events_table()"};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb.maintain_events_table()
 * ```
 */
export const maintainEventsTable = new PreparedQuery<IMaintainEventsTableParams,IMaintainEventsTableResult>(maintainEventsTableIR);


