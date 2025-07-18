import { PGMBMakeEventBatcherOpts, PgPublishMsg } from '../types'

type Batch<M> = {
	messages: PgPublishMsg<M>[]
}

export class PGMBEventBatcher<M> {

	#publish: PGMBMakeEventBatcherOpts<M>['publish']
	#flushIntervalMs: number | undefined
	#maxBatchSize: number
	#currentBatch: Batch<M> = { messages: [] }
	#flushTimeout: NodeJS.Timeout | undefined
	#flushTask: Promise<void> | undefined
	#logger: PGMBMakeEventBatcherOpts<M>['logger']
	#shouldLog: PGMBMakeEventBatcherOpts<M>['shouldLog'] | undefined
	#batch = 0

	constructor({
		shouldLog,
		publish,
		flushIntervalMs,
		maxBatchSize = 2500,
		logger
	}: PGMBMakeEventBatcherOpts<M>) {
		this.#publish = publish
		this.#flushIntervalMs = flushIntervalMs
		this.#maxBatchSize = maxBatchSize
		this.#logger = logger
		this.#shouldLog = shouldLog
	}

	async close() {
		clearTimeout(this.#flushTimeout)
		await this.#flushTask
		await this.flush()
	}

	enqueue(msg: PgPublishMsg<M>) {
		this.#currentBatch.messages.push(msg)
		if(this.#currentBatch.messages.length >= this.#maxBatchSize) {
			this.flush()
			return
		}

		if(this.#flushTimeout || !this.#flushIntervalMs) {
			return
		}

		this.#flushTimeout = setTimeout(() => this.flush(), this.#flushIntervalMs)
	}

	async flush() {
		if(!this.#currentBatch.messages.length) {
			return
		}

		const batch = this.#currentBatch
		this.#currentBatch = { messages: [] }
		clearTimeout(this.#flushTimeout)
		this.#flushTimeout = undefined

		await this.#flushTask

		this.#flushTask = this.#publishBatch(batch)
		return this.#flushTask
	}

	async #publishBatch({ messages }: Batch<M>) {
		const batch = ++this.#batch
		try {
			const ids = await this.#publish(...messages)
			for(const [i, { id }] of ids.entries()) {
				if(this.#shouldLog && !this.#shouldLog(messages[i])) {
					continue
				}

				this.#logger.info(
					{ batch, id, message: messages[i] },
					'published message'
				)
			}
		} catch(err) {
			this.#logger.error(
				{ batch, err, msgs: messages },
				'failed to publish messages'
			)
		}
	}
}