import type { IDatabaseConnection } from '@pgtyped/runtime'
import assert, { AssertionError } from 'assert'
import { createHash } from 'crypto'
import type { IncomingMessage, ServerResponse } from 'http'
import type { Logger } from 'pino'
import { setTimeout } from 'timers/promises'
import type { HeaderRecord } from 'undici-types/header.js'
import { AbortableAsyncIterator } from '../abortable-async-iterator.ts'
import type { IAssertSubscriptionParams, IFindEventsResult, IReplayEventsResult } from '../queries.ts'
import { assertGroup, assertSubscription, deleteSubscriptions, findEvents, markSubscriptionsActive, pollForEvents, readNextEvents, removeExpiredSubscriptions, replayEvents, scheduleEventRetry, setGroupCursor } from '../queries.ts'
import type { JSONifier } from '../types.ts'
import { getCreateDateFromSubscriptionId, getDateFromMessageId } from '../utils.ts'

type SerialisedEvent = {
	body: Buffer | string
	contentType: string
}

export type WebhookInfo = {
	id: string
	url: string | URL
}

type GetWebhookInfo = (
	subscriptionIds: string[]
) => Promise<{ [id: string]: WebhookInfo[] }> | { [id: string]: WebhookInfo[] }

type PgmbWebhookOpts = {
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
	getWebhookInfo?: GetWebhookInfo
}

type FnContext = {
	logger: Logger
	client: IDatabaseConnection
	subscriptionId: string
	extra?: unknown
	signal?: AbortSignal
}

export type IReadEvent = {
	items: IFindEventsResult[]
}

type RegisterSubscriptionParams
	= Omit<IAssertSubscriptionParams, 'groupId'>

export type IEphemeralListener = AbortableAsyncIterator<IReadEvent> & {
	id: string
}

type IEventHandler = (item: IReadEvent, ctx: FnContext) => Promise<void>

type IDurableListener = {
	type: 'durable'
	handler: IEventHandler
	removeOnEmpty?: boolean
	extra?: unknown
	queue: {
		item: IReadEvent
		checkpoint: Checkpoint
	}[]
}

type IListener = {
	type: 'ephemeral'
	stream: IEphemeralListener
} | IDurableListener

type IListenerStore = {
	values: { [id: string]: IListener }
}

type Checkpoint = {
	activeTasks: number
	nextCursor: string
}

type RetryEventPayload = {
	ids: string[]
	retryNumber: number
}

const RETRY_EVENT = 'pgmb-retry'

export class Pgmb2Client {

	readonly client: IDatabaseConnection
	readonly logger: Logger
	readonly groupId: string
	readonly sleepDurationMs: number
	readonly readChunkSize: number
	readonly subscriptionMaintenanceMs: number
	readonly maxActiveCheckpoints: number

	readonly getWebhookInfo: GetWebhookInfo
	readonly webhookHandler: IEventHandler

	readonly listeners: { [subId: string]: IListenerStore } = {}

	#endAc = new AbortController()
	eventsPublished = 0

	readonly #shouldPoll: boolean
	#readTask?: Promise<void>
	#pollTask?: Promise<void>
	#subMaintainTask?: Promise<void>

	#inMemoryCursor: string | null = null
	#activeCheckpoints: Checkpoint[] = []

	constructor({
		client, logger, groupId,
		sleepDurationMs = 500,
		readChunkSize = 1000,
		maxActiveCheckpoints = 100,
		poll,
		subscriptionMaintenanceMs = 60 * 1000,
		webhookHandlerOpts = {},
		getWebhookInfo = () => ({})
	}: Pgmb2ClientOpts) {
		this.client = client
		this.logger = logger
		this.groupId = groupId
		this.sleepDurationMs = sleepDurationMs
		this.readChunkSize = readChunkSize
		this.#shouldPoll = !!poll
		this.subscriptionMaintenanceMs = subscriptionMaintenanceMs
		this.maxActiveCheckpoints = maxActiveCheckpoints
		this.webhookHandler = createWebhookHandler(webhookHandlerOpts)
		this.getWebhookInfo = getWebhookInfo
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
		while(this.#activeCheckpoints.length) {
			await setTimeout(100)
		}

		this.#endAc.abort()

		for(const id in this.listeners) {
			delete this.listeners[id]
		}

		await Promise.all([this.#readTask, this.#pollTask, this.#subMaintainTask])

		this.#readTask = undefined
		this.#pollTask = undefined
		this.#subMaintainTask = undefined
		this.#activeCheckpoints = []
		this.eventsPublished = 0
	}

	async registerSubscription(opts: RegisterSubscriptionParams) {
		const [{ id: subId }] = await assertSubscription
			.run({ ...opts, groupId: this.groupId }, this.client)

		this.logger
			.debug({ subId, ...opts }, 'asserted subscription')

		return this.#listenForEvents(subId)
	}

	async registerDurableSubscription(
		opts: RegisterSubscriptionParams,
		handler: IEventHandler
	) {
		const [{ id: subId }] = await assertSubscription
			.run({ ...opts, groupId: this.groupId }, this.client)
		const listener: IListener = { type: 'durable', handler, queue: [] }

		this.logger
			.debug({ subId, ...opts }, 'asserted subscription')
		this.listeners[subId] ||= { values: {} }
		const lid = createListenerId()
		this.listeners[subId].values[lid] = listener

		return {
			subscriptionId: subId,
			cancel: () => this.#removeListener(subId, lid)
		}
	}

	async removeSubscription(subId: string) {
		await deleteSubscriptions.run({ ids: [subId] }, this.client)
		this.logger.debug({ subId }, 'deleted subscription')

		const existingSubs = this.listeners[subId]?.values
		delete this.listeners[subId]
		if(!existingSubs) {
			return
		}

		await Promise.allSettled(
			Object.values(existingSubs).map(e => (
				e.type === 'ephemeral'
				&& e.stream.throw(new Error('subscription removed'))
			))
		)
	}

	#listenForEvents(subId: string): IEphemeralListener {
		const lid = createListenerId()
		const iterator = new AbortableAsyncIterator<IReadEvent>(
			this.#endAc.signal,
			() => this.#removeListener(subId, lid)
		)

		const stream = iterator as unknown as IEphemeralListener
		stream.id = subId

		this.listeners[subId] ||= { values: {} }
		this.listeners[subId].values[lid] = { type: 'ephemeral', stream }

		return stream
	}

	#removeListener(subId: string, lid: string) {
		const existingSubs = this.listeners[subId]?.values
		delete existingSubs?.[lid]
		if(existingSubs && Object.keys(existingSubs).length) {
			return
		}

		delete this.listeners[subId]
		this.logger.debug({ subId }, 'removed last subscriber for sub')
	}

	async #maintainSubscriptions() {
		const activeIds = Object.keys(this.listeners)
		await markSubscriptionsActive.run({ ids: activeIds }, this.client)

		this.logger.trace(
			{ activeSubscriptions: activeIds.length },
			'marked subscriptions as active'
		)

		const [{ deleted }] = await removeExpiredSubscriptions
			.run({ groupId: this.groupId, activeIds }, this.client)
		this.logger.trace({ deleted }, 'removed expired subscriptions')
	}

	async readChanges(groupId: string, client: IDatabaseConnection = this.client) {
		if(this.#activeCheckpoints.length >= this.maxActiveCheckpoints) {
			return 0
		}

		const now = Date.now()
		const rows = await readNextEvents.run(
			{
				groupId,
				cursor: this.#inMemoryCursor,
				chunkSize: this.readChunkSize
			},
			client
		)
		if(!rows.length) {
			return 0
		}

		const uqSubIds = Array.from(
			new Set(rows.flatMap(r => r.subscriptionIds))
		)
		const webhookSubs = await this.getWebhookInfo(uqSubIds)
		let webhookCount = 0

		for(const sid in webhookSubs) {
			const webhooks = webhookSubs[sid]
			const lts = (this.listeners[sid] ||= { values: {} })
			for(const wh of webhooks) {
				// add durable listener for each webhook
				lts.values[wh.id] ||= {
					type: 'durable',
					queue: [],
					extra: wh,
					removeOnEmpty: true,
					handler: this.webhookHandler
				}

				webhookCount++
			}
		}

		// reverse the map, do subscriptionId -> events
		const subToEventMap:
			{ [subscriptionId: string]: IReadEvent } = {}
		for(const row of rows) {
			for(const subId of row.subscriptionIds) {
				subToEventMap[subId] ||= { items: [] }
				subToEventMap[subId].items.push(row)
			}

			this.eventsPublished ++
		}

		const subs = Object.entries(subToEventMap)
		const checkpoint: Checkpoint
			= { activeTasks: 0, nextCursor: rows[0].nextCursor }
		for(const [subId, ev] of subs) {
			const listeners = this.listeners[subId]?.values
			if(!listeners) {
				continue
			}

			for(const lid in listeners) {
				const lt = listeners[lid]
				if(lt.type === 'ephemeral') {
					lt.stream.enqueue(ev)
					continue
				}

				this.#enqueueEventInDurableListener(subId, lid, ev, checkpoint)
			}
		}

		this.#activeCheckpoints.push(checkpoint)
		this.#inMemoryCursor = checkpoint.nextCursor

		this.logger.debug(
			{
				rowsRead: rows.length,
				subscriptions: subs.length,
				durationMs: Date.now() - now,
				totalEventsPublished: this.eventsPublished,
				checkpoint,
				activeCheckpoints: this.#activeCheckpoints.length,
				webhookCount
			},
			'read rows'
		)

		if(!checkpoint.activeTasks) {
			await this.#updateCursorFromCompletedCheckpoints()
		}

		return rows.length
	}

	/**
	 * Runs the durable listener's handler for each item in its queue,
	 * one after the other, till the queue is empty or the client has ended.
	 * Any errors are logged, swallowed, and processing continues.
	 */
	async #enqueueEventInDurableListener(
		subId: string,
		lid: string,
		item: IReadEvent,
		checkpoint: Checkpoint
	) {
		const lt = this.listeners[subId]?.values?.[lid]
		assert(lt?.type === 'durable', 'invalid listener type: ' + lt.type)

		const { handler, queue, removeOnEmpty, extra } = lt

		queue.push({ item, checkpoint })
		checkpoint.activeTasks ++
		if(queue.length > 1) {
			return
		}

		while(!this.#endAc.signal.aborted && queue.length) {
			const { item, checkpoint } = queue[0]
			const logger = this.logger
				.child({ subId, items: item.items.map(i => i.id) })

			logger.trace(
				{
					cpActiveTasks: checkpoint.activeTasks,
					queue: queue.length,
				},
				'processing durable event handler queue'
			)

			try {
				await handler(
					item,
					{
						signal: this.#endAc.signal,
						client: this.client,
						logger,
						subscriptionId: subId,
						extra,
					}
				)
			} catch(err) {
				logger.error({ err }, 'error in durable event handler')
			} finally {
				checkpoint.activeTasks --
				if(!checkpoint.activeTasks) {
					await this.#updateCursorFromCompletedCheckpoints()
				}

				queue.shift()
				logger.trace(
					{
						cpActiveTasks: checkpoint.activeTasks,
						queue: queue.length,
					},
					'completed durable event handler task'
				)

				assert(checkpoint.activeTasks >= 0, 'internal: checkpoint.activeTasks < 0')
			}
		}

		if(removeOnEmpty) {
			return this.#removeListener(subId, lid)
		}
	}

	/**
	 * Goes through all checkpoints, and sets the group cursor to the latest
	 * completed checkpoint. If a checkpoint has active tasks, stops there.
	 * This ensures that we don't accidentally move the cursor forward while
	 * there are still pending tasks for earlier checkpoints.
	 */
	async #updateCursorFromCompletedCheckpoints() {
		let latestMaxCursor: string | undefined
		while(this.#activeCheckpoints.length) {
			const cp = this.#activeCheckpoints[0]
			if(cp.activeTasks > 0) {
				break
			}

			latestMaxCursor = cp.nextCursor
			this.#activeCheckpoints.shift()
		}

		if(!latestMaxCursor) {
			return
		}

		try {
			await setGroupCursor.run(
				{ groupId: this.groupId, cursor: latestMaxCursor },
				this.client
			)

			this.logger.debug({ cursor: latestMaxCursor }, 'set cursor')
		} catch(err) {
			this.logger
				.error({ err, cursor: latestMaxCursor }, 'error setting cursor')
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

type SSESubscriptionOpts
	= Pick<RegisterSubscriptionParams, 'conditionsSql' | 'params'>

type SSERequestHandlerOpts = {
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

export function createSSERequestHandler(
	this: Pgmb2Client,
	{
		getSubscriptionOpts,
		maxReplayEvents = 1000,
		maxReplayIntervalMs = 5 * 60 * 1000,
		jsonifier = JSON
	}: SSERequestHandlerOpts,
) {

	return handleSSERequest.bind(this)

	async function handleSSERequest(
		this: Pgmb2Client,
		req: IncomingMessage,
		res: ServerResponse
	) {
		let sub: IEphemeralListener | undefined
		let eventsToReplay: IReplayEventsResult[] = []

		try {
			assert(
				req.method?.toLowerCase() === 'get',
				'SSE only supports GET requests'
			)
			// validate last-event-id header
			const fromEventId = req.headers['last-event-id']
			if(fromEventId) {
				assert(maxReplayEvents > 0, 'replay disabled on server')
				assert(typeof fromEventId === 'string', 'invalid last-event-id header')
				const fromDt = getDateFromMessageId(fromEventId)
				assert(fromDt, 'invalid last-event-id header value')
				assert(
					fromDt.getTime() >= (Date.now() - maxReplayIntervalMs),
					'last-event-id is too old to replay'
				)
			}

			sub = await this.registerSubscription({
				...await getSubscriptionOpts(req),
				expiryInterval: `${maxReplayIntervalMs * 2} milliseconds`
			})

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
						maxEvents: maxReplayEvents
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
			this.logger
				.error({ subId: sub?.id, err }, 'error in sse subscription setup')

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
			writeSseEvents(res, eventsToReplay)

			for await (const { items } of sub) {
				writeSseEvents(res, items)
			}
		} catch(err) {
			this.logger.error({ err }, 'error in sse subscription')
			if(res.writableEnded) {
				return
			}

			// send error event
			const message = err instanceof Error ? err.message : String(err)
			const errData	= jsonifier.stringify({ message })
			res.write(`event: error\ndata: ${errData}\nretry: 250\n\n`)
			res.end()
		}
	}

	function writeSseEvents(
		res: ServerResponse,
		items: IFindEventsResult[] | IReplayEventsResult[]
	) {
		for(const { id, payload, topic } of items) {
			const data = jsonifier.stringify(payload)
			if(!maxReplayEvents) {
				// if replay is disabled, do not send an id field
				res.write(`event: ${topic}\ndata: ${data}\n\n`)
				continue
			}

			res.write(`id: ${id}\nevent: ${topic}\ndata: ${data}\n\n`)
		}
	}
}

type IMaybeRetryEvent = {
	items: IFindEventsResult[]
	retryPayload?: RetryEventPayload
}

type IRetryHandlerOpts = {
	retriesS: number[]
}

export function createRetryHandler(
	handler: IEventHandler, { retriesS }: IRetryHandlerOpts
): IEventHandler {
	return async(ev: IReadEvent, ctx: FnContext) => {
		const { client, subscriptionId } = ctx
		const evs: IMaybeRetryEvent[] = []
		const idsToLoad: string[] = []

		const items = [...ev.items]

		for(let i = 0; i < items.length;) {
			const { topic, payload: _p } = items[i]
			if(topic !== RETRY_EVENT) {
				i++
				continue
			}

			const retryPayload = _p as RetryEventPayload
			if(retryPayload.ids?.length) {
				evs.push({ items: [], retryPayload })
				idsToLoad.push(...retryPayload.ids)
			}

			items.splice(i, 1)
		}

		if(items.length) {
			evs.push({ items })
		}

		if(idsToLoad) {
			const fetchedEvents = await findEvents.run({ ids: idsToLoad }, client)
			const fetchedEventMap = fetchedEvents.reduce(
				(map, ev) => {
					map[ev.id] = ev
					return map
				},
				{} as { [id: string]: IFindEventsResult }
			)

			ctx.logger.debug(
				{
					idsToLoad: idsToLoad.length,
					fetchedCount: fetchedEvents.length
				},
				'loaded events for retry'
			)

			// populate the events
			for(const { items, retryPayload } of evs) {
				if(!retryPayload) {
					continue
				}

				for(const id of retryPayload.ids) {
					const ev = fetchedEventMap[id]
					if(!ev) {
						ctx.logger.warn({ id }, 'event to retry not found')
						continue
					}

					items.push(ev)
				}
			}
		}

		for(const ev of evs) {
			const logger = ctx.logger.child({
				retryNumber: ev.retryPayload?.retryNumber,
				ids: ev.items.map(i => i.id)
			})

			try {
				await handler(ev,	{ ...ctx, logger })
			} catch(err) {
				const retryNumber = (ev.retryPayload?.retryNumber ?? 0)
				const nextRetryGapS = retriesS[retryNumber]
				logger.error({ err, nextRetryGapS }, 'error in event handler')

				if(!nextRetryGapS) {
					return
				}

				await scheduleEventRetry.run(
					{
						subscriptionId,
						ids: ev.items.map(i => i.id),
						retryNumber: retryNumber + 1,
						delayInterval: `${nextRetryGapS} seconds`
					},
					client
				)
			}
		}
	}
}

/**
 * Create a handler that sends events to a webhook URL via HTTP POST.
 * @param url Where to send the webhook requests
 */
function createWebhookHandler(
	{
		timeoutMs = 5_000,
		headers,
		retryOpts = { retriesS: [1 * 60, 10 * 60] },
		jsonifier = JSON,
		serialiseEvent = createSimpleSerialiser(jsonifier)
	}: Partial<PgmbWebhookOpts>
) {
	const handler: IEventHandler = async(ev, { extra }) => {
		assert(
			typeof extra === 'object'
			&& extra !== null
			&& 'url' in extra
			&& (
				typeof extra.url === 'string'
				|| extra.url instanceof URL
			),
			'webhook handler requires extra.url parameter'
		)
		const { url } = extra
		const { body, contentType } = serialiseEvent(ev)
		const { status, body: res } = await fetch(url, {
			method: 'POST',
			headers: {
				'content-type': contentType,
				'x-idempotency-key': getIdempotencyKeyHeader(ev),
				...headers
			},
			body,
			redirect: 'manual',
			signal: AbortSignal.timeout(timeoutMs)
		})
		// don't care about response body
		await res?.cancel().catch(() => { })
		if(status < 200 || status >= 300) {
			throw new Error(`Non-2xx response: ${status}`)
		}
	}

	if(!retryOpts) {
		return handler
	}

	return createRetryHandler(handler, retryOpts)
}

function getIdempotencyKeyHeader(ev: IReadEvent) {
	const hasher = createHash('sha256')
	for(const item of ev.items) {
		hasher.update(item.id)
	}

	return hasher.digest('hex').slice(0, 16)
}

function createSimpleSerialiser(
	jsonifier: JSONifier
): ((ev: IReadEvent) => SerialisedEvent) {
	return ev => ({
		body: jsonifier.stringify({
			items: ev.items
				.map(({ id, payload, topic }) => ({ id, payload, topic	}))
		}),
		contentType: 'application/json'
	})
}

function createListenerId() {
	return Math.random().toString(16).slice(2, 10)
}
