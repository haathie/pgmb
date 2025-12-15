import type { IDatabaseConnection } from '@pgtyped/runtime'
import type { IncomingMessage } from 'node:http'
import type { Logger } from 'pino'
import type { HeaderRecord } from 'undici-types/header.js'
import type { AbortableAsyncIterator } from './abortable-async-iterator.ts'
import type { IAssertSubscriptionParams, IFindEventsResult } from './queries.ts'

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

export type Pgmb2ClientOpts = {
	client: IDatabaseConnection
	logger: Logger
	groupId: string
	/** How long to sleep between polls & read fn calls */
	sleepDurationMs?: number
	/**
	 * How often to mark subscriptions as active,
	 * and remove expired ones.
	 * @default 1 minute
	 */
	subscriptionMaintenanceMs?: number

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
	poll?: boolean

	webhookHandlerOpts?: Partial<PgmbWebhookOpts>
	getWebhookInfo?: GetWebhookInfoFn
}

export type IReadEvent = {
	items: IFindEventsResult[]
}

export type RegisterSubscriptionParams
	= Omit<IAssertSubscriptionParams, 'groupId'>

export interface IEphemeralListener
	extends AbortableAsyncIterator<IReadEvent> {
	id: string
}

export type IEventHandlerContext = {
	logger: Logger
	client: IDatabaseConnection
	subscriptionId: string
	extra?: unknown
	signal?: AbortSignal
}

export type IEventHandler = (
	item: IReadEvent,
	ctx: IEventHandlerContext
) => Promise<void>

export type RetryEventPayload = {
	ids: string[]
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
