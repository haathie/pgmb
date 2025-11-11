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

const createSubscriptionIR: any = {"usedParamSet":{"readerId":true,"conditionsSql":true,"metadata":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":78,"b":87}]},{"name":"conditionsSql","required":false,"transform":{"type":"scalar"},"locs":[{"a":99,"b":112}]},{"name":"metadata","required":false,"transform":{"type":"scalar"},"locs":[{"a":133,"b":141}]}],"statement":"INSERT INTO pgmb2.subscriptions (reader_id, conditions_sql, metadata)\nVALUES (:readerId!, COALESCE(:conditionsSql, 'TRUE'), COALESCE(:metadata::jsonb, '{}'))\nRETURNING id AS \"id!\""};

/**
 * Query generated from SQL:
 * ```
 * INSERT INTO pgmb2.subscriptions (reader_id, conditions_sql, metadata)
 * VALUES (:readerId!, COALESCE(:conditionsSql, 'TRUE'), COALESCE(:metadata::jsonb, '{}'))
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
  metadata: unknown;
  payload: unknown;
  subscriptionIds: stringArray;
  topic: string;
}

/** 'ReadNextEventsForSubscriptions' query type */
export interface IReadNextEventsForSubscriptionsQuery {
  params: IReadNextEventsForSubscriptionsParams;
  result: IReadNextEventsForSubscriptionsResult;
}

const readNextEventsForSubscriptionsIR: any = {"usedParamSet":{"readerId":true,"chunkSize":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":179,"b":188}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":192,"b":202}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\",\n\tsubscription_ids AS \"subscriptionIds!\"\nFROM pgmb2.read_next_events_for_subscriptions(\n\t:readerId!,\n\t:chunkSize!\n)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!",
 * 	subscription_ids AS "subscriptionIds!"
 * FROM pgmb2.read_next_events_for_subscriptions(
 * 	:readerId!,
 * 	:chunkSize!
 * )
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
  metadata: unknown;
  payload: unknown;
  topic: string;
}

/** 'ReadNextEvents' query type */
export interface IReadNextEventsQuery {
  params: IReadNextEventsParams;
  result: IReadNextEventsResult;
}

const readNextEventsIR: any = {"usedParamSet":{"readerId":true,"chunkSize":true},"params":[{"name":"readerId","required":true,"transform":{"type":"scalar"},"locs":[{"a":120,"b":129}]},{"name":"chunkSize","required":true,"transform":{"type":"scalar"},"locs":[{"a":133,"b":143}]}],"statement":"SELECT\n\tid AS \"id!\",\n\ttopic AS \"topic!\",\n\tpayload AS \"payload!\",\n\tmetadata AS \"metadata!\"\nFROM pgmb2.read_next_events(\n\t:readerId!,\n\t:chunkSize!\n)"};

/**
 * Query generated from SQL:
 * ```
 * SELECT
 * 	id AS "id!",
 * 	topic AS "topic!",
 * 	payload AS "payload!",
 * 	metadata AS "metadata!"
 * FROM pgmb2.read_next_events(
 * 	:readerId!,
 * 	:chunkSize!
 * )
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


