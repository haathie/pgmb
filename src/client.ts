import assert from 'assert'
import { type Logger, pino } from 'pino'
import { setTimeout } from 'timers/promises'
import { AbortableAsyncIterator } from './abortable-async-iterator.ts'
import { PGMBEventBatcher } from './batcher.ts'
import {
	assertGroup,
	assertSubscription,
	deleteSubscriptions,
	getConfigValue,
	maintainEventsTable,
	markSubscriptionsActive,
	pollForEvents,
	readNextEvents as defaultReadNextEvents,
	releaseGroupLock,
	removeExpiredSubscriptions,
	setGroupCursor,
	writeEvents,
} from './queries.ts'
import type { PgClientLike, PgReleasableClient } from './query-types.ts'
import {
	createRetryHandler,
	normaliseRetryEventsInReadEventMap,
} from './retry-handler.ts'
import type {
	GetWebhookInfoFn,
	IEphemeralListener,
	IEventData,
	IEventHandler,
	IFindEventsFn,
	IReadEvent,
	IReadNextEventsFn,
	ISplitFn,
	Pgmb2ClientOpts,
	registerReliableHandlerParams,
	RegisterSubscriptionParams,
} from './types.ts'
import { getEnvNumber } from './utils.ts'
import { createWebhookHandler } from './webhook-handler.ts'

type IReliableListener<T extends IEventData> = {
 	type: 'reliable'
 	handler: IEventHandler<T>
 	removeOnEmpty?: boolean
 	extra?: unknown
 	splitBy?: ISplitFn<T>
 	queue: {
 		item: IReadEvent<T>
 		checkpoint: Checkpoint
	}[]
};

type IFireAndForgetListener<T extends IEventData> = {
	type: 'fire-and-forget'
	stream: IEphemeralListener<T>
};

type IListener<T extends IEventData> =
	| IFireAndForgetListener<T>
	| IReliableListener<T>;

type Checkpoint = {
	activeTasks: number
	nextCursor: string
	cancelled?: boolean
};

export type IListenerStore<T extends IEventData> = {
	values: { [id: string]: IListener<T> }
};

export class PgmbClient<
	T extends IEventData = IEventData,
> extends PGMBEventBatcher<T> {
	readonly client: PgClientLike
	readonly logger: Logger
	readonly groupId: string
	readonly readEventsIntervalMs: number
	readonly pollEventsIntervalMs: number
	readonly readChunkSize: number
	readonly subscriptionMaintenanceMs: number
	readonly tableMaintenanceMs: number
	readonly maxActiveCheckpoints: number
	readonly readNextEvents: IReadNextEventsFn
	readonly findEvents?: IFindEventsFn

	readonly getWebhookInfo: GetWebhookInfoFn
	readonly webhookHandler: IEventHandler<T>

	readonly listeners: { [subId: string]: IListenerStore<T> } = {}

	readonly #webhookHandlerOpts: Partial<{ splitBy?: ISplitFn<T> }>

	#readClient?: PgReleasableClient

	#endAc = new AbortController()

	#readTask?: Promise<void>
	#pollTask?: Promise<void>
	#subMaintainTask?: Promise<void>
	#tableMaintainTask?: Promise<void>

	#inMemoryCursor: string | null = null
	#activeCheckpoints: Checkpoint[] = []

	constructor({
		client,
		groupId,
		logger = pino(),
		readEventsIntervalMs = getEnvNumber('PGMB_READ_EVENTS_INTERVAL_MS', 1000),
		readChunkSize = getEnvNumber('PGMB_READ_CHUNK_SIZE', 1000),
		maxActiveCheckpoints = getEnvNumber('PGMB_MAX_ACTIVE_CHECKPOINTS', 10),
		pollEventsIntervalMs = getEnvNumber('PGMB_POLL_EVENTS_INTERVAL_MS', 1000),
		subscriptionMaintenanceMs
		= getEnvNumber('PGMB_SUBSCRIPTION_MAINTENANCE_S', 60) * 1000,
		tableMaintainanceMs
		= getEnvNumber('PGMB_TABLE_MAINTENANCE_M', 15) * 60 * 1000,
		webhookHandlerOpts: {
			splitBy: whSplitBy,
			...whHandlerOpts
		} = {},
		getWebhookInfo = () => ({}),
		readNextEvents = defaultReadNextEvents.run.bind(defaultReadNextEvents),
		findEvents,
		...batcherOpts
	}: Pgmb2ClientOpts<T>) {
		super({
			...batcherOpts,
			logger,
			publish: (...e) => this.publish(e),
		})
		this.client = client
		this.logger = logger
		this.groupId = groupId
		this.readEventsIntervalMs = readEventsIntervalMs
		this.readChunkSize = readChunkSize
		this.pollEventsIntervalMs = pollEventsIntervalMs
		this.subscriptionMaintenanceMs = subscriptionMaintenanceMs
		this.maxActiveCheckpoints = maxActiveCheckpoints
		this.webhookHandler = createWebhookHandler<T>(whHandlerOpts)
		this.#webhookHandlerOpts = { splitBy: whSplitBy }
		this.getWebhookInfo = getWebhookInfo
		this.tableMaintenanceMs = tableMaintainanceMs
		this.readNextEvents = readNextEvents
		this.findEvents = findEvents
	}

	async init() {
		this.#endAc = new AbortController()

		if('connect' in this.client) {
			this.client.on('remove', this.#onPoolClientRemoved)
		}

		const [pgCronRslt] = await getConfigValue
			.run({ key: 'use_pg_cron' }, this.client)
		const isPgCronEnabled = pgCronRslt?.value === 'true'
		if(!isPgCronEnabled) {
			// maintain event table
			await maintainEventsTable.run(undefined, this.client)
			this.logger.debug('maintained events table')

			if(this.pollEventsIntervalMs) {
				this.#pollTask = this.#startLoop(
					pollForEvents.run.bind(pollForEvents, undefined, this.client),
					this.pollEventsIntervalMs,
				)
			}

			if(this.tableMaintenanceMs) {
				this.#tableMaintainTask = this.#startLoop(
					maintainEventsTable.run
						.bind(maintainEventsTable, undefined, this.client),
					this.tableMaintenanceMs,
				)
			}
		}

		await assertGroup.run({ id: this.groupId }, this.client)
		this.logger.debug({ groupId: this.groupId }, 'asserted group exists')
		// clean up expired subscriptions on start
		const [{ deleted }] = await removeExpiredSubscriptions.run(
			{ groupId: this.groupId, activeIds: [] },
			this.client,
		)
		this.logger.debug({ deleted }, 'removed expired subscriptions')

		this.#readTask = this.#startLoop(
			this.readChanges.bind(this),
			this.readEventsIntervalMs,
		)

		if(this.subscriptionMaintenanceMs) {
			this.#subMaintainTask = this.#startLoop(
				this.#maintainSubscriptions,
				this.subscriptionMaintenanceMs,
			)
		}

		this.logger.info({ isPgCronEnabled }, 'pgmb client initialised')
	}

	async end() {
		await super.end()

		this.#endAc.abort()

		while(this.#activeCheckpoints.length) {
			await setTimeout(100)
		}

		for(const id in this.listeners) {
			delete this.listeners[id]
		}

		await Promise.all([
			this.#readTask,
			this.#pollTask,
			this.#subMaintainTask,
			this.#tableMaintainTask,
		])

		await this.#unlockAndReleaseReadClient()
		this.#readTask = undefined
		this.#pollTask = undefined
		this.#subMaintainTask = undefined
		this.#activeCheckpoints = []
	}

	publish(events: T[], client = this.client) {
		return writeEvents.run(
			{
				topics: events.map((e) => e.topic),
				payloads: events.map((e) => e.payload),
				metadatas: events.map((e) => e.metadata || null),
			},
			client,
		)
	}

	async assertSubscription(
		opts: RegisterSubscriptionParams,
		client = this.client,
	) {
		const [rslt] = await assertSubscription.run(
			{ ...opts, groupId: this.groupId },
			client,
		)

		this.logger.debug({ ...opts, ...rslt }, 'asserted subscription')
		return rslt
	}

	/**
	 * Registers a fire-and-forget handler, returning an async iterator
	 * that yields events as they arrive. The client does not wait for event
	 * processing acknowledgements. Useful for cases where data is eventually
	 * consistent, or when event delivery isn't critical
	 * (eg. http SSE, websockets).
	 */
	async registerFireAndForgetHandler(opts: RegisterSubscriptionParams) {
		const { id: subId } = await this.assertSubscription(opts)
		return this.#listenForEvents(subId)
	}

	/**
	 * Registers a reliable handler for the given subscription params.
	 * If the handler throws an error, client will rollback to the last known
	 * good cursor, and re-deliver events.
	 * To avoid a full redelivery of a batch, a retry strategy can be provided
	 * to retry failed events by the handler itself, allowing for delayed retries
	 * with backoff, and without disrupting the overall event flow.
	 */
	async registerReliableHandler(
		{
			retryOpts,
			name = createListenerId(),
			splitBy,
			...opts
		}: registerReliableHandlerParams<T>,
		handler: IEventHandler<T>,
	) {
		const { id: subId } = await this.assertSubscription(opts)
		if(retryOpts) {
			handler = createRetryHandler(retryOpts, handler)
		}

		const lts = (this.listeners[subId] ||= { values: {} })
		assert(
			!lts.values[name],
			`Handler with id ${name} already registered for subscription ${subId}.` +
				' Cancel the existing one or use a different id.',
		)
		this.listeners[subId].values[name] = {
			type: 'reliable',
			handler,
			splitBy,
			queue: [],
		}

		return {
			subscriptionId: subId,
			cancel: () => this.#removeListener(subId, name),
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
			Object.values(existingSubs).map(
				(e) => e.type === 'fire-and-forget' &&
					e.stream.throw(new Error('subscription removed')),
			),
		)
	}

	#listenForEvents(subId: string): IEphemeralListener<T> {
		const lid = createListenerId()
		const iterator = new AbortableAsyncIterator<IReadEvent<T>>(
			this.#endAc.signal,
			() => this.#removeListener(subId, lid),
		)

		const stream = iterator as IEphemeralListener<T>
		stream.id = subId

		this.listeners[subId] ||= { values: {} }
		this.listeners[subId].values[lid] = { type: 'fire-and-forget', stream }

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
			'marked subscriptions as active',
		)

		const [{ deleted }] = await removeExpiredSubscriptions.run(
			{ groupId: this.groupId, activeIds },
			this.client,
		)
		this.logger.trace({ deleted }, 'removed expired subscriptions')
	}

	async readChanges() {
		if(this.#activeCheckpoints.length >= this.maxActiveCheckpoints) {
			return 0
		}

		const now = Date.now()
		await this.#connectReadClient()
		const rows = await this.readNextEvents(
			{
				groupId: this.groupId,
				cursor: this.#inMemoryCursor,
				chunkSize: this.readChunkSize,
			},
			this.#readClient || this.client,
		)
			.catch(async(err) => {
				if(err instanceof Error && err.message.includes('connection error')) {
					await this.#unlockAndReleaseReadClient()
				}

				throw err
			})
		if(!rows.length) {
			// if nothing is happening and there are no active checkpoints,
			// we can just let the read client go
			if(!this.#activeCheckpoints.length) {
				await this.#unlockAndReleaseReadClient()
			}

			return 0
		}

		const uqSubIds = Array.from(
			new Set(rows.flatMap((r) => r.subscriptionIds)),
		)
		const webhookSubs = await this.getWebhookInfo(uqSubIds)
		let webhookCount = 0

		for(const sid in webhookSubs) {
			const webhooks = webhookSubs[sid]
			const lts = (this.listeners[sid] ||= { values: {} })
			for(const wh of webhooks) {
				// add reliable listener for each webhook
				lts.values[wh.id] ||= {
					type: 'reliable',
					queue: [],
					extra: wh,
					removeOnEmpty: true,
					handler: this.webhookHandler,
					...this.#webhookHandlerOpts
				}

				webhookCount++
			}
		}

		const { map: subToEventMap, retryEvents, retryItemCount }
			= await normaliseRetryEventsInReadEventMap<T>(
				rows, this.client, this.findEvents
			)

		const subs = Object.entries(subToEventMap)
		const checkpoint: Checkpoint = {
			activeTasks: 0,
			nextCursor: rows[0].nextCursor,
		}
		for(const [subId, evs] of subs) {
			const listeners = this.listeners[subId]?.values
			if(!listeners) {
				continue
			}

			for(const ev of evs) {
				for(const lid in listeners) {
					if(ev.retry?.handlerName && lid !== ev.retry.handlerName) {
						continue
					}

					const lt = listeners[lid]
					if(lt.type === 'fire-and-forget') {
						lt.stream.enqueue(ev)
						continue
					}

					this.#enqueueEventInReliableListener(subId, lid, ev, checkpoint)
				}
			}
		}

		this.#activeCheckpoints.push(checkpoint)
		this.#inMemoryCursor = checkpoint.nextCursor

		this.logger.debug(
			{
				rowsRead: rows.length,
				subscriptions: subs.length,
				durationMs: Date.now() - now,
				checkpoint,
				activeCheckpoints: this.#activeCheckpoints.length,
				webhookCount,
				retryEvents,
				retryItemCount,
			},
			'read rows',
		)

		if(!checkpoint.activeTasks && this.#activeCheckpoints.length === 1) {
			await this.#updateCursorFromCompletedCheckpoints()
		}

		return rows.length
	}

	/**
	 * Runs the reliable listener's handler for each item in its queue,
	 * one after the other, till the queue is empty or the client has ended.
	 * Any errors are logged, swallowed, and processing continues.
	 */
	async #enqueueEventInReliableListener(
		subId: string,
		lid: string,
		item: IReadEvent<T>,
		checkpoint: Checkpoint,
	) {
		const lt = this.listeners[subId]?.values?.[lid]
		assert(lt?.type === 'reliable', 'invalid listener type: ' + lt.type)

		const {
			handler, queue, removeOnEmpty, extra,
			splitBy = defaultSplitBy
		} = lt

		queue.push({ item, checkpoint })
		checkpoint.activeTasks++
		if(queue.length > 1) {
			return
		}

		while(queue.length) {
			const { item, checkpoint } = queue[0]
			if(checkpoint.cancelled) {
				queue.shift()
				continue
			}

			const logger = this.logger.child({
				subId,
				items: item.items.map((i) => i.id),
				extra,
				retryNumber: item.retry?.retryNumber,
			})

			logger.trace(
				{
					cpActiveTasks: checkpoint.activeTasks,
					queue: queue.length,
				},
				'processing handler queue',
			)

			try {
				for(const batch of splitBy(item)) {
					await handler(batch, {
						client: this.client,
						logger: this.logger.child({
							subId,
							items: batch.items.map(i => i.id),
							extra,
							retryNumber: item.retry?.retryNumber,
						}),
						subscriptionId: subId,
						extra,
						name: lid,
					})
				}

				checkpoint.activeTasks--
				assert(
					checkpoint.activeTasks >= 0,
					'internal: checkpoint.activeTasks < 0',
				)
				if(!checkpoint.activeTasks) {
					await this.#updateCursorFromCompletedCheckpoints()
				}

				logger.trace(
					{
						cpActiveTasks: checkpoint.activeTasks,
						queue: queue.length,
					},
					'completed handler task',
				)
			} catch(err) {
				logger.error(
					{ err },
					'error in handler,' +
						'cancelling all active checkpoints' +
						'. Restarting from last known good cursor.',
				)
				this.#cancelAllActiveCheckpoints()
			} finally {
				queue.shift()
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

		const releaseLock = !this.#activeCheckpoints.length
		await setGroupCursor.run(
			{
				groupId: this.groupId,
				cursor: latestMaxCursor,
				releaseLock: releaseLock,
			},
			this.#readClient || this.client,
		)
		this.logger.debug(
			{
				cursor: latestMaxCursor,
				activeCheckpoints: this.#activeCheckpoints.length,
			},
			'set cursor',
		)

		// if there are no more active checkpoints,
		// clear in-memory cursor, so in case another process takes
		// over, if & when we start reading again, we read from the DB cursor
		if(releaseLock) {
			this.#inMemoryCursor = null
			this.#releaseReadClient()
		}
	}

	#cancelAllActiveCheckpoints() {
		for(const cp of this.#activeCheckpoints) {
			cp.cancelled = true
		}

		this.#activeCheckpoints = []
		this.#inMemoryCursor = null
	}

	async #unlockAndReleaseReadClient() {
		if(!this.#readClient) {
			return
		}

		try {
			await releaseGroupLock.run({ groupId: this.groupId }, this.#readClient)
		} catch(err) {
			this.logger.error({ err }, 'error releasing read client')
		} finally {
			this.#releaseReadClient()
		}
	}

	async #connectReadClient() {
		if(!('connect' in this.client)) {
			return false
		}

		if(this.#readClient) {
			return true
		}

		this.#readClient = await this.client.connect()
		this.logger.trace('acquired dedicated read client')
		return true
	}

	#onPoolClientRemoved = async(cl: PgReleasableClient) => {
		if(cl !== this.#readClient) {
			return
		}

		this.logger.info(
			'dedicated read client disconnected, may have dup event processing',
		)
	}

	#releaseReadClient() {
		try {
			this.#readClient?.release()
		} catch{}

		this.#readClient = undefined
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

function createListenerId() {
	return Math.random().toString(16).slice(2, 10)
}

function defaultSplitBy<T extends IEventData>(e: IReadEvent<T>) {
	return [e]
}
