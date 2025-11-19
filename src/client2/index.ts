import type { IDatabaseConnection } from '@pgtyped/runtime'
import assert from 'assert'
import type { Logger } from 'pino'
import { PassThrough, type Writable } from 'stream'
import { setTimeout } from 'timers/promises'
import type { IAssertSubscriptionParams } from '../queries.ts'
import { assertSubscription, deleteSubscriptions, type IReadNextEventsResult, pollForEvents, readNextEvents, removeHttpSubscriptionsInGroup } from '../queries.ts'

type Pgmb2ClientOpts = {
	client: IDatabaseConnection
	logger: Logger
	groupId?: string
	sleepDurationMs?: number
	readChunkSize?: number
	poll?: boolean
}

type IReadEvent = {
	items: IReadNextEventsResult[]
}

type CancelFn = () => Promise<void>

type IActiveSubscription = {
	stream: Writable
	deleteOnClose: boolean
	cancelRead?: CancelFn
}

export class Pgmb2Client {

	readonly client: IDatabaseConnection
	readonly logger: Logger
	readonly groupId: string | undefined
	readonly sleepDurationMs: number
	readonly readChunkSize: number

	#subscribers: { [topic: string]: IActiveSubscription } = {}
	#eventsPublished = 0
	#cancelGroupRead?: CancelFn
	#pollTask?: CancelFn

	constructor({
		client, logger, groupId,
		sleepDurationMs = 500,
		readChunkSize = 1000,
		poll
	}: Pgmb2ClientOpts) {
		this.client = client
		this.logger = logger
		this.groupId = groupId
		this.sleepDurationMs = sleepDurationMs
		this.readChunkSize = readChunkSize
		if(poll) {
			this.#pollTask = this.#startPollLoop()
		}
	}

	async initGroup() {
		if(!this.groupId) {
			return
		}

		await removeHttpSubscriptionsInGroup
			.run({ groupId: this.groupId }, this.client)
		this.#cancelGroupRead = this.#startReadLoop(this.groupId)
	}

	async end() {
		const tasks: Promise<unknown>[] = []
		const subsToDel: string[] = []
		for(const [id, sub] of Object.entries(this.#subscribers)) {
			if(sub.cancelRead) {
				tasks.push(sub.cancelRead())
				sub.cancelRead = undefined
			}

			if(sub.deleteOnClose) {
				subsToDel.push(id)
				sub.deleteOnClose = false
			}

			delete this.#subscribers[id]
			sub.stream.end()
		}

		if(this.#cancelGroupRead) {
			tasks.push(this.#cancelGroupRead())
		}

		if(subsToDel.length) {
			tasks.push(
				deleteSubscriptions.run({ ids: subsToDel }, this.client)
			)
		}

		await Promise.all(tasks)

		this.#subscribers = {}
		this.#cancelGroupRead = undefined
	}

	async registerSubscription(
		params: IAssertSubscriptionParams,
		deleteOnClose: boolean
	) {
		assert(
			params.groupId === this.groupId || !params.groupId,
			'Cannot register subscription with different groupId than client'
		)

		const [{ id: subId }] = await assertSubscription.run(params, this.client)
		const cancelRead = params.groupId
			? undefined
			: this.#startReadLoop(subId)
		return this.#listenForEvents(subId, deleteOnClose, cancelRead)
	}

	#listenForEvents(
		subId: string, deleteOnClose: boolean, cancelRead?: CancelFn,
	): AsyncIterableIterator<IReadEvent, void> {
		const stream = new PassThrough({ objectMode: true, highWaterMark: 1 })
		this.#subscribers[subId] = { stream, deleteOnClose, cancelRead }

		stream.on('close', this.#onSubscriptionEnd.bind(this, subId))

		const asyncIterator = stream[Symbol.asyncIterator]()
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

	async #onSubscriptionEnd(subId: string) {
		const sub = this.#subscribers[subId]
		if(!sub) {
			return
		}

		this.logger.debug({ subId }, 'subscription stream closed, cleaning up')
		delete this.#subscribers[subId]

		if(sub.deleteOnClose) {
			try {
				await deleteSubscriptions.run({ ids: [subId] }, this.client)
			} catch(err) {
				this.logger.error({ subId, err }, 'error deleting subscription')
			}
		}

		if(sub.cancelRead) {
			try {
				await sub.cancelRead()
			} catch(err) {
				this.logger.error({ subId, err }, 'error cancelling read loop')
			}
		}
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

	async readChanges(fetchId: string) {
		const now = Date.now()
		const rows = await readNextEvents.run(
			{ fetchId, chunkSize: this.readChunkSize },
			this.client
		)

		const subToEventMap:
			{ [subscriptionId: string]: IReadNextEventsResult[] } = {}
		for(const row of rows) {
			for(const subId of row.subscriptionIds) {
				subToEventMap[subId] ||= []
				subToEventMap[subId].push(row)
				this.#eventsPublished ++
			}
		}

		const subs = Object.entries(subToEventMap)
		for(const [subId, items] of subs) {
			const sub = this.#subscribers[subId]
			if(!sub) {
				this.logger.trace({ subId }, 'subscription not found')
				continue
			}

			const { stream } = sub
			const event: IReadEvent = { items }
			stream.write(event, err => {
				if(err) {
					this.logger.warn(
						{ fetchId, err, subscriptionId: subId },
						'error writing to subscription stream'
					)
				}
			})
		}

		if(rows.length) {
			this.logger.debug(
				{
					fetchId,
					rowsRead: rows.length,
					subscriptions: subs.length,
					durationMs: Date.now() - now,
					totalEventsPublished: this.#eventsPublished,
				},
				'read rows'
			)
		}

		return rows.length
	}

	#isClientEnded() {
		if('ended' in this.client) {
			return this.client.ended
		}

		return false
	}
}