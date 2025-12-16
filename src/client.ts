import type { IDatabaseConnection } from '@pgtyped/runtime'
import assert from 'assert'
import { type Logger, pino } from 'pino'
import { setTimeout } from 'timers/promises'
import { AbortableAsyncIterator } from './abortable-async-iterator.ts'
import { PGMBEventBatcher } from './batcher.ts'
import { assertGroup, assertSubscription, deleteSubscriptions, markSubscriptionsActive, pollForEvents, readNextEvents, removeExpiredSubscriptions, setGroupCursor, writeEvents } from './queries.ts'
import type { GetWebhookInfoFn, IEphemeralListener, IEvent, IEventData, IEventHandler, IReadEvent, Pgmb2ClientOpts, RegisterSubscriptionParams } from './types.ts'
import { createWebhookHandler } from './webhook-handler.ts'

type IDurableListener<T extends IEventData> = {
	type: 'durable'
	handler: IEventHandler<T>
	removeOnEmpty?: boolean
	extra?: unknown
	queue: {
		item: IReadEvent<T>
		checkpoint: Checkpoint
	}[]
}

type IListener<T extends IEventData> = {
	type: 'ephemeral'
	stream: IEphemeralListener<T>
} | IDurableListener<T>

type Checkpoint = {
	activeTasks: number
	nextCursor: string
}

export type IListenerStore<T extends IEventData> = {
	values: { [id: string]: IListener<T> }
}

export class PgmbClient<T extends IEventData = IEventData>
	extends PGMBEventBatcher<T> {

	readonly client: IDatabaseConnection
	readonly logger: Logger
	readonly groupId: string
	readonly sleepDurationMs: number
	readonly readChunkSize: number
	readonly subscriptionMaintenanceMs: number
	readonly maxActiveCheckpoints: number

	readonly getWebhookInfo: GetWebhookInfoFn
	readonly webhookHandler: IEventHandler

	readonly listeners: { [subId: string]: IListenerStore<T> } = {}

	#endAc = new AbortController()
	eventsPublished = 0

	readonly #shouldPoll: boolean
	#readTask?: Promise<void>
	#pollTask?: Promise<void>
	#subMaintainTask?: Promise<void>

	#inMemoryCursor: string | null = null
	#activeCheckpoints: Checkpoint[] = []

	constructor({
		client,
		groupId,
		logger = pino(),
		sleepDurationMs = 500,
		readChunkSize = 1000,
		maxActiveCheckpoints = 100,
		poll,
		subscriptionMaintenanceMs = 60 * 1000,
		webhookHandlerOpts = {},
		getWebhookInfo = () => ({}),
		...batcherOpts
	}: Pgmb2ClientOpts) {
		super({
			...batcherOpts,
			logger,
			publish: (...e) => this.publish(e)
		})
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

		this.#readTask
			= this.#startLoop(this.readChanges.bind(this), this.sleepDurationMs)

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
		await super.end()
		this.#endAc.abort()

		while(this.#activeCheckpoints.length) {
			await setTimeout(100)
		}

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

	publish(events: T[], client: IDatabaseConnection = this.client) {
		return writeEvents.run(
			{
				topics: events.map(e => e.topic),
				payloads: events.map(e => e.payload),
				metadatas: events.map(e => e.metadata || null),
			},
			client
		)
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
		handler: IEventHandler<T>
	) {
		const [{ id: subId }] = await assertSubscription
			.run({ ...opts, groupId: this.groupId }, this.client)
		const listener: IListener<T> = { type: 'durable', handler, queue: [] }

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

	#listenForEvents(subId: string): IEphemeralListener<T> {
		const lid = createListenerId()
		const iterator = new AbortableAsyncIterator<IReadEvent<T>>(
			this.#endAc.signal,
			() => this.#removeListener(subId, lid)
		)

		const stream = iterator as IEphemeralListener<T>
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

	async readChanges(client: IDatabaseConnection = this.client) {
		if(this.#activeCheckpoints.length >= this.maxActiveCheckpoints) {
			return 0
		}

		const now = Date.now()
		const rows = await readNextEvents.run(
			{
				groupId: this.groupId,
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
			{ [subscriptionId: string]: IReadEvent<T> } = {}
		for(const row of rows) {
			for(const subId of row.subscriptionIds) {
				subToEventMap[subId] ||= { items: [] }
				subToEventMap[subId].items.push(row as unknown as IEvent<T>)
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
		item: IReadEvent<T>,
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

		while(queue.length) {
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

function createListenerId() {
	return Math.random().toString(16).slice(2, 10)
}
