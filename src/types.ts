import type { IncomingMessage } from 'node:http'
import type { Logger } from 'pino'
import type { HeaderRecord } from 'undici-types/header.js'
import type { AbortableAsyncIterator } from './abortable-async-iterator.ts'
import type { IAssertSubscriptionParams } from './queries.ts'
import type { PgClientLike } from './query-types.ts'

export type SerialisedEvent = {
	body: Buffer | string
	contentType: string
}

export type WebhookInfo = {
	id: string
	url: string | URL
}

export type GetWebhookInfoFn = (
	subscriptionIds: string[]
) => Promise<{ [id: string]: WebhookInfo[] }> | { [id: string]: WebhookInfo[] }

export type PgmbWebhookOpts = {
	/**
	 * Maximum time to wait for webhook request to complete
	 * @default 5 seconds
	 */
	timeoutMs?: number
	headers?: HeaderRecord
	/**
	 * Configure retry intervals in seconds for failed webhook requests.
	 * If null, a failed handler will fail the event processor. Use carefully.
	 */
	retryOpts?: IRetryHandlerOpts | null
	jsonifier?: JSONifier
	serialiseEvent?(ev: IReadEvent): SerialisedEvent
}

export interface IEventData {
	topic: string
	payload: unknown
	metadata?: unknown
}

export type IEvent<T extends IEventData> = (T & { id: string })

export type PGMBEventBatcherOpts<T extends IEventData> = {
	/**
	 * Whether a particular published message should be logged.
	 * By default, all messages are logged -- in case of certain
	 * failures, the logs can be used to replay the messages.
	 */
	shouldLog?(msg: T): boolean

	publish(...msgs: T[]): Promise<{ id: string }[]>

	logger?: Logger
	/**
	 * Automatically flush after this interval.
	 * Set to undefined or 0 to disable. Will need to
	 * manually call `flush()` to publish messages.
	 * @default undefined
	 */
	flushIntervalMs?: number
	/**
	 * Max number of messages to send in a batch
	 * @default 2500
	 */
	maxBatchSize?: number
}

export type Pgmb2ClientOpts = {
	client: PgClientLike
	/**
	 * Globally unique identifier for this Pgmb2Client instance. All subs
	 * registered with this client will use this groupId.
	 */
	groupId: string
	logger?: Logger
	/** How long to sleep between polls & read fn calls */
	sleepDurationMs?: number
	/**
	 * How often to mark subscriptions as active,
	 * and remove expired ones.
	 * @default 1 minute
	 */
	subscriptionMaintenanceMs?: number
	/** How often to maintain the events tables
	 * (drop old partitions, create new ones, etc)
	 * @default 5 minutes
	 */
	tableMaintainanceMs?: number

	readChunkSize?: number
	/**
	 * As we process in batches, a single handler taking time to finish
	 * can lead to buildup of unprocessed checkpoints. To avoid this,
	 * we keep moving forward while handlers run in the background, but
	 * to avoid an unbounded number of items being backlogged, we limit
	 * how much further we can go ahead from the earliest uncompleted checkpoint.
	 * @default 10
	 */
	maxActiveCheckpoints?: number
	/**
	 * Should this client poll for new events?
	 * @default true
	 */
	poll?: boolean

	webhookHandlerOpts?: Partial<PgmbWebhookOpts>
	getWebhookInfo?: GetWebhookInfoFn
} & Pick<
	PGMBEventBatcherOpts<IEventData>,
	'flushIntervalMs' | 'maxBatchSize' | 'shouldLog'
>

export type IReadEvent<T extends IEventData = IEventData> = {
	items: IEvent<T>[]
	retry?: IRetryEventPayload
}

export type RegisterSubscriptionParams
	= Omit<IAssertSubscriptionParams, 'groupId'>

export type registerReliableHandlerParams = RegisterSubscriptionParams & {
	/**
	 * Name for the retry handler, used to ensure retries for a particular
	 * handler are not mixed with another handler. This name need only be
	 * unique for a particular subscription.
	*/
	name?: string
	retryOpts?: IRetryHandlerOpts
}

export type CreateTopicalSubscriptionOpts<T extends IEventData> = {
	/**
	 * The topics to subscribe to.
	 */
	topics: T['topic'][]
	/**
	 * To scale out processing, you can partition the subscriptions.
	 * For example, with `current: 0, total: 3`, only messages
	 * where `hashtext(e.id) % 3 == 0` will be received by this subscription.
	 */
	partition?: {
		current: number
		total: number
	}
	/**
	 * Add any additional params to filter by.
	 * i.e "s.params @> jsonb_build_object(...additionalFilters)"
	 * The value should be a valid SQL snippet.
	 */
	additionalFilters?: Record<string, string>
	/** JSON to populate params */
	additionalParams?: Record<string, any>

	expiryInterval?: RegisterSubscriptionParams['expiryInterval']
}

export interface IEphemeralListener<T extends IEventData>
	extends AbortableAsyncIterator<IReadEvent<T>> {
	id: string
}

export type IEventHandlerContext = {
	logger: Logger
	client: PgClientLike
	subscriptionId: string
	/** registered name of the handler */
	name: string
	extra?: unknown
}

export type IEventHandler<T extends IEventData = IEventData>
	= (item: IReadEvent<T>, ctx: IEventHandlerContext) => Promise<void>

export type IRetryEventPayload = {
	ids: string[]
	handlerName: string
	retryNumber: number
}

type SSESubscriptionOpts
	= Pick<RegisterSubscriptionParams, 'conditionsSql' | 'params'>

export type SSERequestHandlerOpts = {
	getSubscriptionOpts(req: IncomingMessage):
		Promise<SSESubscriptionOpts> | SSESubscriptionOpts
	/**
	 * Maximum interval to replay events for an SSE subscription.
	 * @default 5 minutes
	 */
	maxReplayIntervalMs?: number
	/**
	 * Max number of events to replay for an SSE subscription.
	 * Set to 0 to disable replaying events.
	 * @default 1000
	 */
	maxReplayEvents?: number

	jsonifier?: JSONifier
}

export type IRetryHandlerOpts = {
	retriesS: number[]
}

export interface JSONifier {
	stringify(data: unknown): string
	parse(data: string): unknown
}

export type ITableMutationEventData<T, N extends string> = {
	topic: `${N}.insert`
	payload: T
	metadata: {}
} | {
	topic: `${N}.delete`
	payload: T
	metadata: {}
} | {
	topic: `${N}.update`
	/**
	 * The fields that were updated in the row
	 */
	payload: Partial<T>
	metadata: {
		old: T
	}
}
