import type { IDatabaseConnection } from '@pgtyped/runtime'
import assert, { AssertionError } from 'assert'
import type { IncomingMessage, ServerResponse } from 'http'
import type { Logger } from 'pino'
import { setTimeout } from 'timers/promises'
import type { HeaderRecord } from 'undici-types/header.js'
import { AbortableAsyncIterator } from '../abortable-async-iterator.ts'
import type { IAssertSubscriptionParams, IReplayEventsResult } from '../queries.ts'
import { assertGroup, assertSubscription, deleteSubscriptions, type IReadNextEventsResult, markSubscriptionsActive, pollForEvents, readNextEvents, removeExpiredSubscriptions, replayEvents, setGroupCursor, writeEvents } from '../queries.ts'
import type { JSONifier } from '../types.ts'
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
	/** How long to sleep between polls & read fn calls */
	sleepDurationMs?: number
	/**
	 * How often to mark subscriptions as active,
	 * and remove expired ones.
	 * @default 1 minute
	 */
	subscriptionMaintenanceMs?: number

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
	/**
	 * Custom JSONifier to use
	 * @default JSON
	 */
	jsonifier?: JSONifier
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

type IActiveSubscription = {
	stream: AbortableAsyncIterator<IReadEvent>
}

export type ISubscriptionListener = AbortableAsyncIterator<IReadEvent> & {
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
	readonly subscriptionMaintenanceMs: number
	readonly webhooks: Pgmb2WebhookOpts | null
	readonly jsonifier: JSONifier

	readonly subscribers: { [subId: string]: IActiveSubscription[] } = {}

	#endAc = new AbortController()
	#eventsPublished = 0

	readonly #shouldPoll: boolean
	#readTask?: Promise<void>
	#pollTask?: Promise<void>
	#subMaintainTask?: Promise<void>

	constructor({
		client, logger, groupId,
		webhooks = { },
		sleepDurationMs = 500,
		readChunkSize = 1000,
		maxReplayIntervalMs = 5 * 60 * 1000,
		poll,
		jsonifier = JSON,
		subscriptionMaintenanceMs = 60 * 1000
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
			serialiseEvent: this.#serialiseJsonEvent,
			...webhooks
		}
		this.jsonifier = jsonifier
		this.subscriptionMaintenanceMs = subscriptionMaintenanceMs
	}

	async init() {
		this.#endAc = new AbortController()

		await assertGroup.run({ id: this.groupId }, this.client)
		this.logger.debug({ groupId: this.groupId }, 'asserted group exists')
		// clean up expired subscriptions on start
		const [{ deleted }] = await removeExpiredSubscriptions
			.run({ groupId: this.groupId, activeIds: [] }, this.client)
		this.logger.debug({ deleted }, 'removed expired subscriptions')

		this.#readTask = this.#startLoop(
			this.readChanges.bind(this, this.groupId),
			this.sleepDurationMs
		)

		if(this.#shouldPoll) {
			this.#pollTask = this.#startLoop(
				pollForEvents.run.bind(pollForEvents, undefined, this.client),
				this.sleepDurationMs
			)
		}

		if(this.subscriptionMaintenanceMs) {
			this.#subMaintainTask = this.#startLoop(
				this.#maintainSubscriptions,
				this.subscriptionMaintenanceMs
			)
		}
	}

	async end() {
		this.#endAc.abort()

		for(const id in this.subscribers) {
			delete this.subscribers[id]
		}

		await Promise.all([this.#readTask, this.#pollTask, this.#subMaintainTask])

		this.#readTask = undefined
		this.#pollTask = undefined
		this.#subMaintainTask = undefined
	}

	async registerSseSubscription(
		opts: Omit<RegisterSubscriptionParams, 'type'>,
		req: IncomingMessage,
		res: ServerResponse
	) {
		let sub: ISubscriptionListener | undefined
		let eventsToReplay: IReplayEventsResult[] = []
		if(typeof opts.expiryInterval === 'undefined') {
			opts.expiryInterval = `${this.maxReplayIntervalMs * 2} milliseconds`
		}

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
			this.logger.error({ err }, 'error in sse subscription setup')

			await sub?.throw(err).catch(() => { })

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
			sub?.return()
		})
		res.once('error', err => {
			sub?.throw(err).catch(() => {})
		})

		res.writeHead(200, {
			'content-type': 'text/event-stream',
			'cache-control': 'no-cache',
			'connection': 'keep-alive',
			'transfer-encoding': 'chunked',
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
			const errData	= this.jsonifier.stringify({ message })
			res.write(`event: error\ndata: ${errData}\nretry: 250\n\n`)
			res.end()
		}
	}

	#writeSseEvents(
		res: ServerResponse,
		items: IReadNextEventsResult[] | IReplayEventsResult[]
	) {
		for(const { id, payload, topic } of items) {
			const data = this.jsonifier.stringify(payload)
			if(this.maxReplayEvents) {
				res.write(`id: ${id}\nevent: ${topic}\ndata: ${data}\n\n`)
			} else {
				// if replay is disabled, do not send an id field
				res.write(`event: ${topic}\ndata: ${data}\n\n`)
			}
		}
	}

	async registerSubscription(opts: RegisterSubscriptionParams) {
		const [{ id: subId }] = await assertSubscription
			.run({ ...opts, groupId: this.groupId }, this.client)

		this.logger
			.debug({ subId, ...opts }, 'asserted subscription exists')

		return this.#listenForEvents(subId)
	}

	async removeSubscription(subId: string) {
		await deleteSubscriptions.run({ ids: [subId] }, this.client)
		this.logger.debug({ subId }, 'deleted subscription')

		const existingSubs = this.subscribers[subId]
		delete this.subscribers[subId]
		if(!existingSubs?.length) {
			return
		}

		await Promise.allSettled(existingSubs.map(e => (
			e.stream.throw(new Error('subscription removed'))
		)))
	}

	#listenForEvents(subId: string): ISubscriptionListener {
		const stream = new AbortableAsyncIterator<IReadEvent>(
			this.#endAc.signal,
			() => {
				this.subscribers[subId] = this.subscribers[subId]
					?.filter(s => s.stream !== stream)
				if(!this.subscribers[subId]?.length) {
					delete this.subscribers[subId]
					this.logger.debug({ subId }, 'removed last subscriber for subscription')
				}
			}
		)

		this.subscribers[subId] ||= []
		this.subscribers[subId].push({ stream })

		const lt = stream as unknown as ISubscriptionListener
		lt.id = subId

		return lt
	}

	async #maintainSubscriptions() {
		const activeIds = Object.keys(this.subscribers)
		await markSubscriptionsActive.run({ ids: activeIds }, this.client)

		this.logger.debug(
			{ activeSubscriptions: activeIds.length },
			'marked subscriptions as active'
		)

		const [{ deleted }] = await removeExpiredSubscriptions
			.run({ groupId: this.groupId, activeIds }, this.client)
		this.logger.debug({ deleted }, 'removed expired subscriptions')
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
		const tasks: Promise<void>[] = []
		for(const [subId, result] of subs) {
			// @ts-expect-error
			if(result.metadata?.['url'] && this.webhooks) {
				tasks.push(this.#sendWebhook(subId, result))
			}

			const sub = this.subscribers[subId]
			if(!sub?.length) {
				this.logger.trace({ subId }, 'subscription not found')
				continue
			}

			for(const { stream } of sub) {
				stream.enqueue(result)
			}
		}

		await Promise.all(tasks)

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

	#serialiseJsonEvent = (ev: IReadEvent): SerialisedEvent => {
		return {
			body: this.jsonifier.stringify(ev),
			contentType: 'application/json'
		}
	}

	async #startLoop(fn: Function, sleepDurationMs: number) {
		const signal = this.#endAc.signal
		while(!signal.aborted) {
			try {
				await setTimeout(sleepDurationMs, undefined, { signal })
				await fn.call(this)
			} catch(err) {
				if(err instanceof Error && err.name === 'AbortError') {
					return
				}

				this.logger.error({ err, fn: fn.name }, 'error in task')
			}
		}
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
