import type { IEventData, PGMBEventBatcherOpts } from './types.ts'

type Batch<T> = {
	messages: T[]
}

export class PGMBEventBatcher<T extends IEventData> {

	#publish: PGMBEventBatcherOpts<T>['publish']
	#flushIntervalMs: number | undefined
	#maxBatchSize: number
	#currentBatch: Batch<T> = { messages: [] }
	#flushTimeout: NodeJS.Timeout | undefined
	#flushTask: Promise<void> | undefined
	#logger: PGMBEventBatcherOpts<T>['logger']
	#shouldLog?: PGMBEventBatcherOpts<T>['shouldLog']
	#batch = 0

	constructor({
		shouldLog,
		publish,
		flushIntervalMs,
		maxBatchSize = 2500,
		logger
	}: PGMBEventBatcherOpts<T>) {
		this.#publish = publish
		this.#flushIntervalMs = flushIntervalMs
		this.#maxBatchSize = maxBatchSize
		this.#logger = logger
		this.#shouldLog = shouldLog
	}

	async end() {
		clearTimeout(this.#flushTimeout)
		await this.#flushTask
		await this.flush()
	}

	/**
	 * Enqueue a message to be published, will be flushed to the database
	 * when flush() is called (either manually or via interval)
	 */
	enqueue(msg: T) {
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

	async #publishBatch({ messages }: Batch<T>) {
		const batch = ++this.#batch
		try {
			const ids = await this.#publish(...messages)
			for(const [i, { id }] of ids.entries()) {
				if(this.#shouldLog && !this.#shouldLog(messages[i])) {
					continue
				}

				this.#logger
					?.info({ batch, id, message: messages[i] }, 'published message')
			}
		} catch(err) {
			this.#logger
				?.error({ batch, err, msgs: messages }, 'failed to publish messages')
		}
	}
}
