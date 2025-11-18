/** Types generated for queries found in "sql/queries.sql" */
import { PreparedQuery } from '@pgtyped/runtime';

export type DateOrString = Date | string;

export type DateOrStringArray = (DateOrString)[];

export type stringArray = (string)[];

export type unknownArray = (unknown)[];

/** 'CreateSubscription' parameters type */
export interface ICreateSubscriptionParams {
  conditionsSql?: string | null | void;
  groupId?: string | null | void;
  id?: string | null | void;
  metadata?: unknown | null | void;
}

/** 'CreateSubscription' return type */
export interface ICreateSubscriptionResult {
  id: string;
}

/** 'CreateSubscription' query type */
export interface ICreateSubscriptionQuery {
  params: ICreateSubscriptionParams;
  result: ICreateSubscriptionResult;
}

const createSubscriptionIR: any = {"usedParamSet":{"id":true,"groupId":true,"conditionsSql":true,"metadata":true},"params":[{"name":"id","required":false,"transform":{"type":"scalar"},"locs":[{"a":92,"b":94}]},{"name":"groupId","required":false,"transform":{"type":"scalar"},"locs":[{"a":130,"b":137}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":150,"b":163}]},{"name":"metadata","required":false,"transform":{"type":"scalar"},"locs":[{"a":185,"b":193}]}],"statement":"INSERT INTO pgmb2.subscriptions (id, group_id, conditions_sql, metadata)\nVALUES (\n\tCOALESCE(:id::text, gen_random_uuid()::text),\n\t:groupId,\n\tCOALESCE(:conditionsSql, 'TRUE'),\n\tCOALESCE(:metadata::jsonb, '{}')\n)\nON CONFLICT (id) DO UPDATE\nSET\n\tconditions_sql = EXCLUDED.conditions_sql,\n\tmetadata = EXCLUDED.metadata\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscriptions (id, group_id, conditions_sql, metadata)
 * VALUES (
 * 	COALESCE(:id::text, gen_random_uuid()::text),
 * 	:groupId,
 * 	COALESCE(:conditionsSql, 'TRUE'),
 * 	COALESCE(:metadata::jsonb, '{}')
 * )
 * ON CONFLICT (id) DO UPDATE
 * SET
 * 	conditions_sql = EXCLUDED.conditions_sql,
 * 	metadata = EXCLUDED.metadata
 * RETURNING id AS "id!"
 * ```
 */
export const createSubscription = new PreparedQuery<ICreateSubscriptionParams,ICreateSubscriptionResult>(createSubscriptionIR);


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
  subscriptionId: string;
}

/** 'ReadNextEvents' return type */
export interface IReadNextEventsResult {
  id: string;
  metadata: unknown;
  payload: unknown;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"subscriptionId":true,"chunkSize":true},"params":[{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":118,"b":133}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":136,"b":146}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\"\nFROM pgmb2.read_next_events(:subscriptionId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!"
 * FROM pgmb2.read_next_events(:subscriptionId!, :chunkSize!)
 * ```
 */
export const readNextEvents = new PreparedQuery<IReadNextEventsParams,IReadNextEventsResult>(readNextEventsIR);


/** 'ReadNextEventsText' parameters type */
export interface IReadNextEventsTextParams {
  chunkSize: number;
  subscriptionId: string;
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

const readNextEventsTextIR: any = {"usedParamSet":{"subscriptionId":true,"chunkSize":true},"params":[{"name":"subscriptionId","required":true,"transform":{"type":"scalar"},"locs":[{"a":98,"b":113}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":116,"b":126}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\"\nFROM pgmb2.read_next_events(:subscriptionId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!"
 * FROM pgmb2.read_next_events(:subscriptionId!, :chunkSize!)
 * ```
 */
export const readNextEventsText = new PreparedQuery<IReadNextEventsTextParams,IReadNextEventsTextResult>(readNextEventsTextIR);


/** 'ReadNextEventsForGroup' parameters type */
export interface IReadNextEventsForGroupParams {
  chunkSize: number;
  groupId: string;
}

/** 'ReadNextEventsForGroup' return type */
export interface IReadNextEventsForGroupResult {
  id: string;
  metadata: unknown;
  payload: unknown;
  subscriptionIds: stringArray;
  topic: string;
}

/** 'ReadNextEventsForGroup' query type */
export interface IReadNextEventsForGroupQuery {
  params: IReadNextEventsForGroupParams;
  result: IReadNextEventsForGroupResult;
}

const readNextEventsForGroupIR: any = {"usedParamSet":{"groupId":true,"chunkSize":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":169,"b":177}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":180,"b":190}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\",\n\tsubscription_ids AS \"subscriptionIds!\"\nFROM pgmb2.read_next_events_for_group(:groupId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!",
 * 	subscription_ids AS "subscriptionIds!"
 * FROM pgmb2.read_next_events_for_group(:groupId!, :chunkSize!)
 * ```
 */
export const readNextEventsForGroup = new PreparedQuery<IReadNextEventsForGroupParams,IReadNextEventsForGroupResult>(readNextEventsForGroupIR);


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


/** 'RemoveTemporarySubscriptions' parameters type */
export interface IRemoveTemporarySubscriptionsParams {
  groupId: string;
}

/** 'RemoveTemporarySubscriptions' return type */
export type IRemoveTemporarySubscriptionsResult = void;

/** 'RemoveTemporarySubscriptions' query type */
export interface IRemoveTemporarySubscriptionsQuery {
  params: IRemoveTemporarySubscriptionsParams;
  result: IRemoveTemporarySubscriptionsResult;
}

const removeTemporarySubscriptionsIR: any = {"usedParamSet":{"groupId":true},"params":[{"name":"groupId","required":true,"transform":{"type":"scalar"},"locs":[{"a":49,"b":57}]}],"statement":"DELETE FROM pgmb2.subscriptions\nWHERE group_id = :groupId! AND is_temporary"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb2.subscriptions
 * WHERE group_id = :groupId! AND is_temporary
 * ```
 */
export const removeTemporarySubscriptions = new PreparedQuery<IRemoveTemporarySubscriptionsParams,IRemoveTemporarySubscriptionsResult>(removeTemporarySubscriptionsIR);


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


