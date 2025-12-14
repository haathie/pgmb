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

const assertGroupIR: any = {"usedParamSet":{"id":true},"params":[{"name":"id","required":true,"transform":{"type":"scalar"},"locs":[{"a":51,"b":54}]}],"statement":"INSERT INTO pgmb2.subscription_groups (id)\nVALUES (:id!)\nON CONFLICT DO NOTHING"};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscription_groups (id)
 * VALUES (:id!)
 * ON CONFLICT DO NOTHING
 * ```
 */
export const assertGroup = new PreparedQuery<IAssertGroupParams,IAssertGroupResult>(assertGroupIR);


/** 'AssertSubscription' parameters type */
export interface IAssertSubscriptionParams {
  conditionsSql?: string | null | void;
  expiresAt?: DateOrString | null | void;
  groupId: string;
  params?: unknown | null | void;
}

/** 'AssertSubscription' return type */
export interface IAssertSubscriptionResult {
  expiresAt: Date;
  id: string;
}

/** 'AssertSubscription' query type */
export interface IAssertSubscriptionQuery {
  params: IAssertSubscriptionParams;
  result: IAssertSubscriptionResult;
}

const assertSubscriptionIR: any = {"usedParamSet":{"groupId":true,"conditionsSql":true,"params":true,"expiresAt":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":93,"b":101}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":114,"b":127}]},{"name":"params","required":false,"transform":{"type":"scalar"},"locs":[{"a":149,"b":155}]},{"name":"expiresAt","required":false,"transform":{"type":"scalar"},"locs":[{"a":173,"b":182}]}],"statement":"INSERT INTO pgmb2.subscriptions AS s(group_id, conditions_sql, params, expires_at)\nVALUES (\n\t:groupId!,\n\tCOALESCE(:conditionsSql, 'TRUE'),\n\tCOALESCE(:params::jsonb, '{}'),\n\t:expiresAt\n)\nON CONFLICT (identity) DO UPDATE\nSET\n\t-- set expires_at to the new value only if it's greater than the existing one\n\t-- or if the new value is NULL (indicating no expiration)\n\texpires_at = CASE\n\t\tWHEN EXCLUDED.expires_at IS NULL OR s.expires_at IS NULL THEN NULL\n\t\tELSE GREATEST(s.expires_at, EXCLUDED.expires_at)\n\tEND\nRETURNING id AS \"id!\", expires_at AS \"expiresAt!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscriptions AS s(group_id, conditions_sql, params, expires_at)
 * VALUES (
 * 	:groupId!,
 * 	COALESCE(:conditionsSql, 'TRUE'),
 * 	COALESCE(:params::jsonb, '{}'),
 * 	:expiresAt
 * )
 * ON CONFLICT (identity) DO UPDATE
 * SET
 * 	-- set expires_at to the new value only if it's greater than the existing one
 * 	-- or if the new value is NULL (indicating no expiration)
 * 	expires_at = CASE
 * 		WHEN EXCLUDED.expires_at IS NULL OR s.expires_at IS NULL THEN NULL
 * 		ELSE GREATEST(s.expires_at, EXCLUDED.expires_at)
 * 	END
 * RETURNING id AS "id!", expires_at AS "expiresAt!"
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

const deleteSubscriptionsIR: any = {"usedParamSet":{"ids":true},"params":[{"name":"ids","required":true,"transform":{"type":"array_spread"},"locs":[{"a":44,"b":48}]}],"statement":"DELETE FROM pgmb2.subscriptions\nWHERE id IN :ids!"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb2.subscriptions
 * WHERE id IN :ids!
 * ```
 */
export const deleteSubscriptions = new PreparedQuery<IDeleteSubscriptionsParams,IDeleteSubscriptionsResult>(deleteSubscriptionsIR);


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

const pollForEventsIR: any = {"usedParamSet":{},"params":[],"statement":"SELECT count AS \"count!\" FROM pgmb2.poll_for_events() AS count"};

/**
 * Query generated from SQL:
 * ```
 * SELECT count AS "count!" FROM pgmb2.poll_for_events() AS count
 * ```
 */
export const pollForEvents = new PreparedQuery<IPollForEventsParams,IPollForEventsResult>(pollForEventsIR);


/** 'ReadNextEvents' parameters type */
export interface IReadNextEventsParams {
  chunkSize: number;
  groupId: string;
}

/** 'ReadNextEvents' return type */
export interface IReadNextEventsResult {
  id: string;
  metadata: unknown;
  nextCursor: string;
  payload: unknown;
  subscriptionIds: stringArray;
  subscriptionMetadatas: unknownArray;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"groupId":true,"chunkSize":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":251,"b":259}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":262,"b":272}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\",\n\tsubscription_ids::text[] AS \"subscriptionIds!\",\n\tsubscription_metadatas AS \"subscriptionMetadatas!\",\n\tnext_cursor AS \"nextCursor!\"\nFROM pgmb2.read_next_events(:groupId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!",
 * 	subscription_ids::text[] AS "subscriptionIds!",
 * 	subscription_metadatas AS "subscriptionMetadatas!",
 * 	next_cursor AS "nextCursor!"
 * FROM pgmb2.read_next_events(:groupId!, :chunkSize!)
 * ```
 */
export const readNextEvents = new PreparedQuery<IReadNextEventsParams,IReadNextEventsResult>(readNextEventsIR);


/** 'ReadNextEventsText' parameters type */
export interface IReadNextEventsTextParams {
  chunkSize: number;
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

const readNextEventsTextIR: any = {"usedParamSet":{"groupId":true,"chunkSize":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":98,"b":106}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":109,"b":119}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\"\nFROM pgmb2.read_next_events(:groupId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!"
 * FROM pgmb2.read_next_events(:groupId!, :chunkSize!)
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

const replayEventsIR: any = {"usedParamSet":{"groupId":true,"subscriptionId":true,"fromEventId":true,"maxEvents":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":129,"b":137}]},{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":160,"b":175}]},{"name":"fromEventId","required":true,"transform":{"type":"scalar"},"locs":[{"a":196,"b":208}]},{"name":"maxEvents","required":true,"transform":{"type":"scalar"},"locs":[{"a":242,"b":252}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\"\nFROM pgmb2.replay_events(\n\tgroup_id := :groupId!,\n\tsubscription_id := :subscriptionId!,\n\tfrom_event_id := :fromEventId!::pgmb2.event_id,\n\tmax_events := :maxEvents!\n)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!"
 * FROM pgmb2.replay_events(
 * 	group_id := :groupId!,
 * 	subscription_id := :subscriptionId!,
 * 	from_event_id := :fromEventId!::pgmb2.event_id,
 * 	max_events := :maxEvents!
 * )
 * ```
 */
export const replayEvents = new PreparedQuery<IReplayEventsParams,IReplayEventsResult>(replayEventsIR);


/** 'SetGroupCursor' parameters type */
export interface ISetGroupCursorParams {
  cursor: string;
  groupId: string;
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

const setGroupCursorIR: any = {"usedParamSet":{"groupId":true,"cursor":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":30,"b":38}]},{"name":"cursor","required":true,"transform":{"type":"scalar"},"locs":[{"a":41,"b":48}]}],"statement":"SELECT pgmb2.set_group_cursor(:groupId!,\t:cursor!::pgmb2.event_id) AS \"success!\""};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb2.set_group_cursor(:groupId!,	:cursor!::pgmb2.event_id) AS "success!"
 * ```
 */
export const setGroupCursor = new PreparedQuery<ISetGroupCursorParams,ISetGroupCursorResult>(setGroupCursorIR);


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

const writeEventsIR: any = {"usedParamSet":{"topics":true,"payloads":true,"metadatas":true},"params":[{"name":"topics","required":true,"transform":{"type":"scalar"},"locs":[{"a":101,"b":108}]},{"name":"payloads","required":true,"transform":{"type":"scalar"},"locs":[{"a":120,"b":129}]},{"name":"metadatas","required":true,"transform":{"type":"scalar"},"locs":[{"a":142,"b":152}]}],"statement":"INSERT INTO pgmb2.events (topic, payload, metadata)\nSELECT\n\ttopic,\n\tpayload,\n\tmetadata\nFROM unnest(\n\t:topics!::TEXT[],\n\t:payloads!::JSONB[],\n\t:metadatas!::JSONB[]\n) AS t(topic, payload, metadata)\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.events (topic, payload, metadata)
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

const writeScheduledEventsIR: any = {"usedParamSet":{"ts":true,"topics":true,"payloads":true,"metadatas":true},"params":[{"name":"ts","required":true,"transform":{"type":"scalar"},"locs":[{"a":192,"b":195}]},{"name":"topics","required":true,"transform":{"type":"scalar"},"locs":[{"a":214,"b":221}]},{"name":"payloads","required":true,"transform":{"type":"scalar"},"locs":[{"a":233,"b":242}]},{"name":"metadatas","required":true,"transform":{"type":"scalar"},"locs":[{"a":255,"b":265}]}],"statement":"INSERT INTO pgmb2.events (id, topic, payload, metadata)\nSELECT\n\tpgmb2.create_event_id(COALESCE(ts, clock_timestamp()), pgmb2.create_random_bigint()),\n\ttopic,\n\tpayload,\n\tmetadata\nFROM unnest(\n\t:ts!::TIMESTAMPTZ[],\n\t:topics!::TEXT[],\n\t:payloads!::JSONB[],\n\t:metadatas!::JSONB[]\n) AS t(ts, topic, payload, metadata)\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.events (id, topic, payload, metadata)
 * SELECT
 * 	pgmb2.create_event_id(COALESCE(ts, clock_timestamp()), pgmb2.create_random_bigint()),
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


/** 'RemoveExpiredSubscriptions' parameters type */
export interface IRemoveExpiredSubscriptionsParams {
  groupId: string;
}

/** 'RemoveExpiredSubscriptions' return type */
export type IRemoveExpiredSubscriptionsResult = void;

/** 'RemoveExpiredSubscriptions' query type */
export interface IRemoveExpiredSubscriptionsQuery {
  params: IRemoveExpiredSubscriptionsParams;
  result: IRemoveExpiredSubscriptionsResult;
}

const removeExpiredSubscriptionsIR: any = {"usedParamSet":{"groupId":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":49,"b":57}]}],"statement":"DELETE FROM pgmb2.subscriptions\nWHERE group_id = :groupId! AND expires_at IS NOT NULL AND expires_at < NOW()"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb2.subscriptions
 * WHERE group_id = :groupId! AND expires_at IS NOT NULL AND expires_at < NOW()
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

const reenqueueEventsForSubscriptionIR: any = {"usedParamSet":{"eventIds":true,"subscriptionId":true,"offsetInterval":true},"params":[{"name":"eventIds","required":true,"transform":{"type":"scalar"},"locs":[{"a":49,"b":58}]},{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":70,"b":85}]},{"name":"offsetInterval","required":true,"transform":{"type":"scalar"},"locs":[{"a":89,"b":104}]}],"statement":"SELECT pgmb2.reenqueue_events_for_subscription(\n\t:eventIds!::text[],\n\t:subscriptionId!,\n\t:offsetInterval!::INTERVAL\n) AS \"reenqueuedEventIds!\""};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb2.reenqueue_events_for_subscription(
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

const maintainEventsTableIR: any = {"usedParamSet":{},"params":[],"statement":"SELECT pgmb2.maintain_events_table()"};

/**
 * Query generated from SQL:
 * ```
 * SELECT pgmb2.maintain_events_table()
 * ```
 */
export const maintainEventsTable = new PreparedQuery<IMaintainEventsTableParams,IMaintainEventsTableResult>(maintainEventsTableIR);


