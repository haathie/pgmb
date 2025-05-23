import { PGMBMakeEventBatcherOpts, PgPublishMsg } from '../types'

type Batch<M> = {
	messages: PgPublishMsg<M>[]
}

export class PGMBEventBatcher<M> {

	#publish: PGMBMakeEventBatcherOpts<M>['publish']
	#flushIntervalMs: number | undefined
	#maxEventsPerBatch: number
	#currentBatch: Batch<M> = { messages: [] }
	#flushTimeout: NodeJS.Timeout | undefined
	#flushTask: Promise<void> | undefined
	#logger: PGMBMakeEventBatcherOpts<M>['logger']

	constructor({
		publish,
		flushIntervalMs,
		maxEventsPerBatch = 2500,
		logger
	}: PGMBMakeEventBatcherOpts<M>) {
		this.#publish = publish
		this.#flushIntervalMs = flushIntervalMs
		this.#maxEventsPerBatch = maxEventsPerBatch
		this.#logger = logger
	}

	async close() {
		clearTimeout(this.#flushTimeout)
		await this.#flushTask
		await this.flush()
	}

	enqueue(msg: PgPublishMsg<M>) {
		this.#currentBatch.messages.push(msg)
		if(this.#currentBatch.messages.length >= this.#maxEventsPerBatch) {
			this.flush()
			return
		}

		if(this.#flushTimeout || !this.#flushIntervalMs) {
			return
		}

		this.#flushTimeout = setTimeout(() => this.flush(), this.#flushIntervalMs)
	}

	async flush() {
		const batch = this.#currentBatch
		this.#currentBatch = { messages: [] }
		clearTimeout(this.#flushTimeout)

		if(!batch.messages.length) {
			return
		}

		await this.#flushTask

		this.#flushTask = this.#publishBatch(batch)
		return this.#flushTask
	}

	async #publishBatch(batch: Batch<M>) {
		try {
			const msgIds = await this.#publish(...batch.messages)
			const consolidatedMsgs = batch.messages.map((msg, i) => ({
				...msg,
				id: msgIds[i].id
			}))
			this.#logger.info(
				{ total: consolidatedMsgs.length, msgs: consolidatedMsgs },
				'published messages'
			)
		} catch(err) {
			this.#logger.error(
				{ err, msgs: batch.messages },
				'failed to publish messages'
			)
		}
	}
}