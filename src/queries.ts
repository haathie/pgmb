/** Types generated for queries found in "sql/queries.sql" */
import { PreparedQuery } from '@pgtyped/runtime';

export type DateOrString = Date | string;

export type DateOrStringArray = (DateOrString)[];

export type stringArray = (string)[];

export type unknownArray = (unknown)[];

/** 'CreateReader' parameters type */
export interface ICreateReaderParams {
  readerId: string;
}

/** 'CreateReader' return type */
export type ICreateReaderResult = void;

/** 'CreateReader' query type */
export interface ICreateReaderQuery {
  params: ICreateReaderParams;
  result: ICreateReaderResult;
}

const createReaderIR: any = {"usedParamSet":{"readerId":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":39,"b":48}]}],"statement":"INSERT INTO pgmb2.readers (id)\nVALUES (:readerId!)"};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.readers (id)
 * VALUES (:readerId!)
 * ```
 */
export const createReader = new PreparedQuery<ICreateReaderParams,ICreateReaderResult>(createReaderIR);


/** 'CreateSubscription' parameters type */
export interface ICreateSubscriptionParams {
  conditionsSql?: string | null | void;
  id?: string | null | void;
  metadata?: unknown | null | void;
  readerId: string;
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

const createSubscriptionIR: any = {"usedParamSet":{"id":true,"readerId":true,"conditionsSql":true,"metadata":true},"params":[{"name":"id","required":false,"transform":{"type":"scalar"},"locs":[{"a":93,"b":95}]},{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":131,"b":140}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":153,"b":166}]},{"name":"metadata","required":false,"transform":{"type":"scalar"},"locs":[{"a":188,"b":196}]}],"statement":"INSERT INTO pgmb2.subscriptions (id, reader_id, conditions_sql, metadata)\nVALUES (\n\tCOALESCE(:id::text, gen_random_uuid()::text),\n\t:readerId!,\n\tCOALESCE(:conditionsSql, 'TRUE'),\n\tCOALESCE(:metadata::jsonb, '{}')\n)\nON CONFLICT (id) DO UPDATE\nSET\n\tconditions_sql = EXCLUDED.conditions_sql,\n\tmetadata = EXCLUDED.metadata\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscriptions (id, reader_id, conditions_sql, metadata)
 * VALUES (
 * 	COALESCE(:id::text, gen_random_uuid()::text),
 * 	:readerId!,
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


/** 'ReadReaderXidStates' parameters type */
export interface IReadReaderXidStatesParams {
  readerId: string;
}

/** 'ReadReaderXidStates' return type */
export interface IReadReaderXidStatesResult {
  completedAt: Date;
  readerId: string;
  xid: string;
}

/** 'ReadReaderXidStates' query type */
export interface IReadReaderXidStatesQuery {
  params: IReadReaderXidStatesParams;
  result: IReadReaderXidStatesResult;
}

const readReaderXidStatesIR: any = {"usedParamSet":{"readerId":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":128,"b":137}]}],"statement":"SELECT\n\treader_id AS \"readerId!\",\n\txid AS \"xid!\",\n\tcompleted_at AS \"completedAt!\"\nFROM pgmb2.reader_xid_state\nWHERE reader_id = :readerId!"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	reader_id AS "readerId!",
 * 	xid AS "xid!",
 * 	completed_at AS "completedAt!"
 * FROM pgmb2.reader_xid_state
 * WHERE reader_id = :readerId!
 * ```
 */
export const readReaderXidStates = new PreparedQuery<IReadReaderXidStatesParams,IReadReaderXidStatesResult>(readReaderXidStatesIR);


/** 'ReadNextEventsForSubscriptions' parameters type */
export interface IReadNextEventsForSubscriptionsParams {
  chunkSize: number;
  readerId: string;
}

/** 'ReadNextEventsForSubscriptions' return type */
export interface IReadNextEventsForSubscriptionsResult {
  id: string;
  metadata: unknown | null;
  payload: string;
  subscriptionIds: stringArray;
  topic: string;
}

/** 'ReadNextEventsForSubscriptions' query type */
export interface IReadNextEventsForSubscriptionsQuery {
  params: IReadNextEventsForSubscriptionsParams;
  result: IReadNextEventsForSubscriptionsResult;
}

const readNextEventsForSubscriptionsIR: any = {"usedParamSet":{"readerId":true,"chunkSize":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":182,"b":191}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":194,"b":204}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\",\n\tmetadata AS \"metadata\",\n\tsubscription_ids AS \"subscriptionIds!\"\nFROM pgmb2.read_next_events_for_subscriptions(:readerId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!",
 * 	metadata AS "metadata",
 * 	subscription_ids AS "subscriptionIds!"
 * FROM pgmb2.read_next_events_for_subscriptions(:readerId!, :chunkSize!)
 * ```
 */
export const readNextEventsForSubscriptions = new PreparedQuery<IReadNextEventsForSubscriptionsParams,IReadNextEventsForSubscriptionsResult>(readNextEventsForSubscriptionsIR);


/** 'ReadNextEvents' parameters type */
export interface IReadNextEventsParams {
  chunkSize: number;
  readerId: string;
}

/** 'ReadNextEvents' return type */
export interface IReadNextEventsResult {
  id: string;
  metadata: string;
  payload: string;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"readerId":true,"chunkSize":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":118,"b":127}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":130,"b":140}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload::text AS \"payload!\",\n\t'' AS \"metadata!\"\nFROM pgmb2.read_next_events(:readerId!, :chunkSize!)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload::text AS "payload!",
 * 	'' AS "metadata!"
 * FROM pgmb2.read_next_events(:readerId!, :chunkSize!)
 * ```
 */
export const readNextEvents = new PreparedQuery<IReadNextEventsParams,IReadNextEventsResult>(readNextEventsIR);


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

const writeScheduledEventsIR: any = {"usedParamSet":{"ts":true,"topics":true,"payloads":true,"metadatas":true},"params":[{"name":"ts","required":true,"transform":{"type":"scalar"},"locs":[{"a":164,"b":167}]},{"name":"topics","required":true,"transform":{"type":"scalar"},"locs":[{"a":186,"b":193}]},{"name":"payloads","required":true,"transform":{"type":"scalar"},"locs":[{"a":205,"b":214}]},{"name":"metadatas","required":true,"transform":{"type":"scalar"},"locs":[{"a":227,"b":237}]}],"statement":"INSERT INTO pgmb2.events (id, topic, payload, metadata)\nSELECT\n\tpgmb2.create_event_id( COALESCE(ts, clock_timestamp()) ),\n\ttopic,\n\tpayload,\n\tmetadata\nFROM unnest(\n\t:ts!::TIMESTAMPTZ[],\n\t:topics!::TEXT[],\n\t:payloads!::JSONB[],\n\t:metadatas!::JSONB[]\n) AS t(ts, topic, payload, metadata)\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.events (id, topic, payload, metadata)
 * SELECT
 * 	pgmb2.create_event_id( COALESCE(ts, clock_timestamp()) ),
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
  readerId: string;
}

/** 'RemoveTemporarySubscriptions' return type */
export type IRemoveTemporarySubscriptionsResult = void;

/** 'RemoveTemporarySubscriptions' query type */
export interface IRemoveTemporarySubscriptionsQuery {
  params: IRemoveTemporarySubscriptionsParams;
  result: IRemoveTemporarySubscriptionsResult;
}

const removeTemporarySubscriptionsIR: any = {"usedParamSet":{"readerId":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":50,"b":59}]}],"statement":"DELETE FROM pgmb2.subscriptions\nWHERE reader_id = :readerId! AND is_temporary"};

/**
 * Query generated from SQL:
 * ```
 * DELETE FROM pgmb2.subscriptions
 * WHERE reader_id = :readerId! AND is_temporary
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


