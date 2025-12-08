/** Types generated for queries found in "sql/queries.sql" */
import { PreparedQuery } from '@pgtyped/runtime';

export type subscription_type = 'custom' | 'http' | 'webhook';

export type DateOrString = Date | string;

export type DateOrStringArray = (DateOrString)[];

export type stringArray = (string)[];

export type unknownArray = (unknown)[];

/** 'AssertSubscription' parameters type */
export interface IAssertSubscriptionParams {
  conditionsSql?: string | null | void;
  groupId?: string | null | void;
  metadata?: unknown | null | void;
  type?: subscription_type | null | void;
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

const assertSubscriptionIR: any = {"usedParamSet":{"groupId":true,"conditionsSql":true,"metadata":true,"type":true},"params":[{"name":"groupId","required":false,"transform":{"type":"scalar"},"locs":[{"a":85,"b":92}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":105,"b":118}]},{"name":"metadata","required":false,"transform":{"type":"scalar"},"locs":[{"a":140,"b":148}]},{"name":"type","required":false,"transform":{"type":"scalar"},"locs":[{"a":175,"b":179}]}],"statement":"INSERT INTO pgmb2.subscriptions (group_id, conditions_sql, metadata, type)\nVALUES (\n\t:groupId,\n\tCOALESCE(:conditionsSql, 'TRUE'),\n\tCOALESCE(:metadata::jsonb, '{}'),\n\tCOALESCE(:type::pgmb2.subscription_type, 'custom')\n)\nON CONFLICT (id) DO UPDATE\nSET\n\tconditions_sql = EXCLUDED.conditions_sql,\n\tmetadata = EXCLUDED.metadata\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscriptions (group_id, conditions_sql, metadata, type)
 * VALUES (
 * 	:groupId,
 * 	COALESCE(:conditionsSql, 'TRUE'),
 * 	COALESCE(:metadata::jsonb, '{}'),
 * 	COALESCE(:type::pgmb2.subscription_type, 'custom')
 * )
 * ON CONFLICT (id) DO UPDATE
 * SET
 * 	conditions_sql = EXCLUDED.conditions_sql,
 * 	metadata = EXCLUDED.metadata
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
  fetchId: string;
}

/** 'ReadNextEvents' return type */
export interface IReadNextEventsResult {
  id: string;
  metadata: unknown;
  payload: unknown;
  subscriptionIds: stringArray;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"fetchId":true,"chunkSize":true},"params":[{"name":"fetchId","required":true,"transform":{"type":"scalar"},"locs":[{"a":167,"b":175}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":178,"b":188}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\",\n\tsubscription_ids::text[] AS \"subscriptionIds!\"\nFROM pgmb2.read_next_events(:fetchId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!",
 * 	subscription_ids::text[] AS "subscriptionIds!"
 * FROM pgmb2.read_next_events(:fetchId!, :chunkSize!)
 * ```
 */
export const readNextEvents = new PreparedQuery<IReadNextEventsParams,IReadNextEventsResult>(readNextEventsIR);


/** 'ReadNextEventsText' parameters type */
export interface IReadNextEventsTextParams {
  chunkSize: number;
  fetchId: string;
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

const readNextEventsTextIR: any = {"usedParamSet":{"fetchId":true,"chunkSize":true},"params":[{"name":"fetchId","required":true,"transform":{"type":"scalar"},"locs":[{"a":98,"b":106}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":109,"b":119}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\"\nFROM pgmb2.read_next_events(:fetchId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!"
 * FROM pgmb2.read_next_events(:fetchId!, :chunkSize!)
 * ```
 */
export const readNextEventsText = new PreparedQuery<IReadNextEventsTextParams,IReadNextEventsTextResult>(readNextEventsTextIR);


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


/** 'RemoveHttpSubscriptionsInGroup' parameters type */
export interface IRemoveHttpSubscriptionsInGroupParams {
  groupId: string;
}

/** 'RemoveHttpSubscriptionsInGroup' return type */
export type IRemoveHttpSubscriptionsInGroupResult = void;

/** 'RemoveHttpSubscriptionsInGroup' query type */
export interface IRemoveHttpSubscriptionsInGroupQuery {
  params: IRemoveHttpSubscriptionsInGroupParams;
  result: IRemoveHttpSubscriptionsInGroupResult;
}

const removeHttpSubscriptionsInGroupIR: any = {"usedParamSet":{"groupId":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":49,"b":57}]}],"statement":"DELETE FROM pgmb2.subscriptions\nWHERE group_id = :groupId! AND type = 'http'"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb2.subscriptions
 * WHERE group_id = :groupId! AND type = 'http'
 * ```
 */
export const removeHttpSubscriptionsInGroup = new PreparedQuery<IRemoveHttpSubscriptionsInGroupParams,IRemoveHttpSubscriptionsInGroupResult>(removeHttpSubscriptionsInGroupIR);


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


