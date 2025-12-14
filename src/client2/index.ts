import type { IDatabaseConnection } from '@pgtyped/runtime'
import assert, { AssertionError } from 'assert'
import type { IncomingMessage, ServerResponse } from 'http'
import type { Logger } from 'pino'
import { PassThrough, type Writable } from 'stream'
import { setTimeout } from 'timers/promises'
import type { HeaderRecord } from 'undici-types/header.js'
import type { IAssertSubscriptionParams, IReplayEventsResult } from '../queries.ts'
import { assertGroup, assertSubscription, deleteSubscriptions, type IReadNextEventsResult, pollForEvents, readNextEvents, removeExpiredSubscriptions, replayEvents, setGroupCursor, writeEvents } from '../queries.ts'
import { getCreateDateFromSubscriptionId, getDateFromMessageId } from '../utils.ts'

type SerialisedEvent = {
	body: Buffer | string
	contentType: string
}

type WebhookMetadata = {
	url: string
	/** Specify what duration after which to retry failed events */
	retriesS?: number[]
}

type Pgmb2WebhookOpts = {
	/**
	 * Size of each chunk of events to send in a single webhook request
	 * @default null // (all events in one request)
	 */
	chunkSize: number | null
	/** Maximum time to wait for webhook request to complete */
	timeoutMs: number
	headers?: HeaderRecord
	serialiseEvent(ev: IReadEvent): SerialisedEvent
}

export type Pgmb2ClientOpts = {
	client: IDatabaseConnection
	logger: Logger
	groupId: string
	sleepDurationMs?: number
	readChunkSize?: number
	poll?: boolean
	/**
	 * Maximum interval to replay events for an SSE subscription.
	 * @default 5 minutes
	 */
	maxReplayIntervalMs?: number
	/**
	 * Specify a non-null object to automatically handle
	 * webhook subscriptions. By default, webhooks are auto handled.
	 * @default {}
	 */
	webhooks?: Partial<Pgmb2WebhookOpts> | null
}

type IReadEvent = {
	/**
	 * metadata of the subscription
	 */
	metadata?: unknown
	items: IReadNextEventsResult[]
}

type RegisterSubscriptionParams
	= Omit<IAssertSubscriptionParams, 'groupId'>

type CancelFn = () => Promise<void>

type IActiveSubscription = {
	stream: Writable
}

export type IRegisteredSubscription = AsyncIterableIterator<IReadEvent, void> & {
	id: string
}

const WEBHOOK_RETRY_EVENT = 'pgmb2-webhook-retry'

export class Pgmb2Client {

	readonly client: IDatabaseConnection
	readonly logger: Logger
	readonly groupId: string
	readonly sleepDurationMs: number
	readonly readChunkSize: number
	readonly maxReplayIntervalMs: number
	readonly maxReplayEvents: number = 1_000
	readonly webhooks: Pgmb2WebhookOpts | null

	readonly subscribers: { [subId: string]: IActiveSubscription[] } = {}
	#eventsPublished = 0
	#cancelGroupRead?: CancelFn

	readonly #shouldPoll: boolean
	#pollTask?: CancelFn

	constructor({
		client, logger, groupId,
		webhooks = { },
		sleepDurationMs = 500,
		readChunkSize = 1000,
		maxReplayIntervalMs = 5 * 60 * 1000,
		poll
	}: Pgmb2ClientOpts) {
		this.client = client
		this.logger = logger
		this.groupId = groupId
		this.sleepDurationMs = sleepDurationMs
		this.readChunkSize = readChunkSize
		this.#shouldPoll = !!poll
		this.maxReplayIntervalMs = maxReplayIntervalMs
		this.webhooks = {
			chunkSize: null,
			timeoutMs: 30_000,
			serialiseEvent: serialiseJsonEvent,
			...webhooks
		}
	}

	async init() {
		if(this.groupId) {
			await assertGroup.run({ id: this.groupId }, this.client)
			this.logger.debug({ groupId: this.groupId }, 'asserted group exists')
			await removeExpiredSubscriptions
				.run({ groupId: this.groupId }, this.client)
			this.logger.debug(
				{ groupId: this.groupId },
				'removed expired subscriptions in group'
			)
			this.#cancelGroupRead = this.#startReadLoop(this.groupId)
		}

		if(this.#shouldPoll) {
			this.#pollTask = this.#startPollLoop()
		}
	}

	async end() {
		await this.#pollTask?.()

		const tasks: Promise<unknown>[] = []
		for(const [id, subs] of Object.entries(this.subscribers)) {
			for(const sub of subs) {
				sub.stream.end()
			}

			delete this.subscribers[id]
		}

		if(this.#cancelGroupRead) {
			tasks.push(this.#cancelGroupRead())
		}

		await Promise.all(tasks)

		this.#cancelGroupRead = undefined
		this.#pollTask = undefined
	}

	async registerSseSubscription(
		opts: Omit<RegisterSubscriptionParams, 'type'>,
		req: IncomingMessage,
		res: ServerResponse
	) {
		let sub: IRegisteredSubscription | undefined
		let eventsToReplay: IReplayEventsResult[] = []

		try {
			assert(
				req.method?.toLowerCase() === 'get',
				'SSE only supports GET requests'
			)
			// validate last-event-id header
			const fromEventId = req.headers['last-event-id']
			if(fromEventId) {
				assert(this.maxReplayEvents > 0, 'replay disabled on server')
				assert(typeof fromEventId === 'string', 'invalid last-event-id header')
				const fromDt = getDateFromMessageId(fromEventId)
				assert(fromDt, 'invalid last-event-id header value')
				assert(
					fromDt.getTime() >= (Date.now() - this.maxReplayIntervalMs),
					'last-event-id is too old to replay'
				)
			}

			sub = await this.registerSubscription(opts)
			if(fromEventId) {
				const fromDt = getDateFromMessageId(fromEventId)!
				const subDt = getCreateDateFromSubscriptionId(sub.id)
				assert(subDt, 'internal: invalid subscription id format')
				assert(
					fromDt >= subDt,
					'last-event-id is before subscription creation, cannot replay'
				)

				eventsToReplay = await replayEvents.run(
					{
						groupId: this.groupId,
						subscriptionId: sub.id,
						fromEventId: fromEventId,
						maxEvents: this.maxReplayEvents
					},
					this.client
				)

				this.logger.trace(
					{ subId: sub.id, count: eventsToReplay.length },
					'got events to replay'
				)
			}

			if(res.writableEnded) {
				throw new Error('response already ended')
			}
		} catch(err) {
			sub?.return!()
			this.logger.error({ err }, 'error in sse subscription setup')
			if(res.writableEnded) {
				return
			}

			const message = err instanceof Error ? err.message : String(err)
			// if an assertion failed, we cannot connect with these parameters
			// so use 204 No Content
			const code = err instanceof AssertionError ? 204 : 500
			res
				.writeHead(code, message)
				.end()
			return
		}

		res.once('close', () => {
			sub?.return!()
		})
		res.once('error', err => {
			sub?.throw!(err)
		})

		res.writeHead(200, {
			'Content-Type': 'text/event-stream',
			'Cache-Control': 'no-cache',
			'Connection': 'keep-alive'
		})
		res.flushHeaders()

		try {
			// send replayed events first
			this.#writeSseEvents(res, eventsToReplay)

			for await (const { items } of sub) {
				this.#writeSseEvents(res, items)
			}
		} catch(err) {
			this.logger.error({ err }, 'error in sse subscription')
			if(res.writableEnded) {
				return
			}

			// send error event
			const message = err instanceof Error ? err.message : String(err)
			res.write(`event: error\ndata: ${JSON.stringify({ message })}\nretry: 250\n\n`)
			res.end()
		}
	}

	#writeSseEvents(
		res: ServerResponse,
		items: IReadNextEventsResult[] | IReplayEventsResult[]
	) {
		for(const { id, payload, topic } of items) {
			const data = JSON.stringify(payload)
			if(this.maxReplayEvents) {
				res.write(`id: ${id}\nevent: ${topic}\ndata: ${data}\n\n`)
			} else {
				// if replay is disabled, do not send an id field
				res.write(`event: ${topic}\ndata: ${data}\n\n`)
			}
		}
	}

	async registerSubscription(opts: RegisterSubscriptionParams) {
		const [{ id: subId, expiresAt }] = await assertSubscription
			.run({ ...opts, groupId: this.groupId }, this.client)

		this.logger
			.debug({ subId, expiresAt, ...opts }, 'asserted subscription exists')

		return this.#listenForEvents(subId)
	}

	async removeSubscription(subId: string) {
		await deleteSubscriptions.run({ ids: [subId] }, this.client)
		this.logger.debug({ subId }, 'deleted subscription')

		for(const sub of this.subscribers[subId] ?? []) {
			sub.stream.destroy(new Error('subscription removed'))
		}

		delete this.subscribers[subId]
	}

	#listenForEvents(subId: string): IRegisteredSubscription {
		const stream = new PassThrough({ objectMode: true, highWaterMark: 1 })
		this.subscribers[subId] ||= []
		this.subscribers[subId].push({ stream })

		stream.on('close', () => {
			this.subscribers[subId] = this.subscribers[subId]
				?.filter(s => s.stream !== stream)
			if(!this.subscribers[subId]?.length) {
				delete this.subscribers[subId]
				this.logger.debug({ subId }, 'removed all listeners for subscription')
			}
		})

		const asyncIterator = stream[Symbol.asyncIterator](
		) as unknown as IRegisteredSubscription
		asyncIterator.id = subId
		const ogReturn = asyncIterator.return!.bind(asyncIterator)
		const ogThrow = asyncIterator.throw!.bind(asyncIterator)
		asyncIterator.return = async(value) => {
			stream.end()
			return ogReturn(value)
		}

		asyncIterator.throw = async(err) => {
			stream.destroy(err)
			return ogThrow(err)
		}

		return asyncIterator
	}

	#startReadLoop(fetchId: string) {
		const controller = new AbortController()
		const task = this.#executeReadLoop(fetchId, controller.signal)
			.catch(err => {
				if(err instanceof Error && err.name === 'AbortError') {
					return
				}

				if(controller.signal.aborted) {
					this.logger.error({ fetchId, err }, 'read loop error after abort')
					return
				}

				controller.abort(err)
			})
		return () => {
			controller.abort()
			return task
		}
	}

	async #executeReadLoop(fetchId: string, signal: AbortSignal) {
		this.logger.trace({ fetchId }, 'starting read loop')

		while(!signal.aborted && !this.#isClientEnded()) {
			let rowsRead = 0
			try {
				rowsRead = await this.readChanges(fetchId)
			} catch(err) {
				this.logger.error({ fetchId, err }, 'error reading changes')
			}

			// nothing to read, wait before next iteration
			if(!rowsRead) {
				await setTimeout(this.sleepDurationMs, undefined, { signal })
				continue
			}
		}

		this.logger.trace({ fetchId }, 'exited read loop')
	}

	#startPollLoop() {
		const controller = new AbortController()
		const task = this.#executePollLoop(controller.signal)
			.catch(err => {
				if(err instanceof Error && err.name === 'AbortError') {
					return
				}

				if(controller.signal.aborted) {
					this.logger.error({ err }, 'poll loop error after abort')
					return
				}

				controller.abort(err)
			})

		return () => {
			controller.abort()
			return task
		}
	}

	async #executePollLoop(signal: AbortSignal) {
		while(!signal.aborted && !this.#isClientEnded()) {
			try {
				await pollForEvents.run(undefined, this.client)
			} catch(err) {
				this.logger.error({ err }, 'error polling for events')
			}

			await setTimeout(this.sleepDurationMs, undefined, { signal })
		}
	}

	async readChanges(groupId: string, client: IDatabaseConnection = this.client) {
		const now = Date.now()
		const rows = await readNextEvents
			.run({ groupId, chunkSize: this.readChunkSize }, client)

		const subToEventMap:
			{ [subscriptionId: string]: IReadEvent } = {}
		for(const row of rows) {
			for(const [idx, subId] of row.subscriptionIds.entries()) {
				const metadata = row.subscriptionMetadatas?.[idx]
				subToEventMap[subId] ||= { items: [], metadata }
				subToEventMap[subId].items.push(row)
				this.#eventsPublished ++
			}
		}

		const subs = Object.entries(subToEventMap)
		const webhookTasks: Promise<void>[] = []
		for(const [subId, result] of subs) {
			// @ts-expect-error
			if(result.metadata?.['url'] && this.webhooks) {
				webhookTasks.push(this.#sendWebhook(subId, result))
			}

			const sub = this.subscribers[subId]
			if(!sub?.length) {
				this.logger.trace({ subId }, 'subscription not found')
				continue
			}

			for(const { stream } of sub) {
				const event: IReadEvent = result
				stream.write(event, err => {
					if(err) {
						this.logger.warn(
							{ err, subscriptionId: subId },
							'error writing to subscription stream'
						)
					}
				})
			}
		}

		await Promise.all(webhookTasks)

		if(rows.length) {
			const nextCursor = rows[0].nextCursor
			this.logger.debug(
				{
					rowsRead: rows.length,
					subscriptions: subs.length,
					durationMs: Date.now() - now,
					totalEventsPublished: this.#eventsPublished,
					nextCursor
				},
				'read rows'
			)

			await setGroupCursor
				.run({ groupId, cursor: nextCursor }, client)
		}

		return rows.length
	}

	async #sendWebhook(subId: string, event: IReadEvent) {
		const { metadata } = event
		if(!isWebhookMetadata(metadata)) {
			return
		}

		const { chunkSize, timeoutMs, serialiseEvent, headers } = this.webhooks!
		const requests: {
			items: IReadNextEventsResult[]
			serialised: SerialisedEvent
			retryNumber?: number
		}[] = []

		// find previous failed events to retry
		const items = [...event.items]
		for(let i = 0; i < items.length;) {
			const { id, topic, metadata, payload } = items[i]
			if(topic !== WEBHOOK_RETRY_EVENT) {
				i++
				continue
			}

			const { items: failedItems } = payload as { items: IReadNextEventsResult[] }
			if(!Array.isArray(failedItems) || !failedItems.length) {
				this.logger.warn({ subId, id }, 'invalid webhook retry event payload')
				i++
				continue
			}

			requests.push({
				items: failedItems,
				serialised: serialiseEvent({ metadata, items: failedItems }),
				retryNumber: typeof metadata === 'object' && metadata !== null
					&& 'retryNumber' in metadata
					&& typeof metadata.retryNumber === 'number'
					? metadata.retryNumber
					: undefined
			})

			items.splice(i, 1)
		}

		if(chunkSize) {
			for(let i = 0; i < items.length; i += chunkSize) {
				const slice = items.slice(i, i + chunkSize)
				requests.push({
					items: slice,
					serialised: serialiseEvent({ metadata, items: slice })
				})
			}
		} else {
			requests.push({ items, serialised: serialiseEvent(event) })
		}

		const { url, retriesS = [] } = metadata
		const retryRequests: {
			payload: { items: IReadNextEventsResult[] }
			metadata: { retryNumber: number }
		}[] = []
		for(const {
			items, serialised: { body, contentType }, retryNumber = 0
		} of requests) {
			const ids = items.map(i => i.id)
			try {
				const { status, body: res } = await fetch(url, {
					method: 'POST',
					headers: { 'Content-Type': contentType, ...headers },
					body,
					redirect: 'manual',
					signal: AbortSignal.timeout(timeoutMs)
				})
				// don't care about response body
				await res?.cancel().catch(() => { })
				if(status < 200 || status >= 300) {
					throw new Error(`Non-2xx response: ${status}`)
				}

				this.logger.info({ subId, url, status, ids },	'sent webhook request')
			} catch(err) {
				const nextRetry = retriesS[retryNumber]
				this.logger
					.error({ subId, url, err, ids, nextRetry }, 'error in webhook request')
				if(typeof nextRetry !== 'number') {
					continue
				}

				retryRequests
					.push({ payload: { items }, metadata: { retryNumber: retryNumber + 1 } })
			}
		}

		if(retryRequests.length) {
			await writeEvents.run(
				{
					payloads: retryRequests.map(r => r.payload),
					metadatas: retryRequests.map(r => r.metadata),
					topics: Array(retryRequests.length).fill(WEBHOOK_RETRY_EVENT)
				},
				this.client
			)
		}
	}

	#isClientEnded() {
		if('ended' in this.client) {
			return this.client.ended
		}

		return false
	}
}

function isWebhookMetadata(metadata: unknown): metadata is WebhookMetadata {
	if(typeof metadata !== 'object' || metadata === null) {
		return false
	}

	if(!('url' in metadata) || typeof metadata['url'] !== 'string') {
		return false
	}

	if(('retriesS' in metadata) && !Array.isArray(metadata['retriesS'])) {
		return false
	}

	return true
}

function serialiseJsonEvent(ev: IReadEvent): SerialisedEvent {
	return {
		body: JSON.stringify(ev),
		contentType: 'application/json'
	}
}
