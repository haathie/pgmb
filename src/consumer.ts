import type { Pool, PoolClient } from 'pg'
import type { Logger } from 'pino'
import type { PGMBConsumerOpts } from './types'

export class PGMBConsumer {

	#consuming = false
	#pendingCount = 0
	#consumeDebounce: NodeJS.Timeout | undefined

	constructor(
		private pool: Pool,
		private opts: PGMBConsumerOpts,
		private logger: Logger,
	) {}

	getOpts() {
		return this.opts
	}

	onMessage(count: number) {
		this.#pendingCount += count
		if(!this.opts.debounceIntervalMs) {
			return this.consume()
		}

		if(this.#pendingCount >= this.opts.batchSize) {
			return this.consume()
		}

		if(this.#consumeDebounce) {
			return
		}

		this.#consumeDebounce = setTimeout(
			() => this.consume(),
			this.opts.debounceIntervalMs
		)
		this.logger.trace({ count }, 'scheduled consume')
	}

	close() {
		this.#consuming = false
		this.#pendingCount = 0
		if(this.#consumeDebounce) {
			clearTimeout(this.#consumeDebounce)
		}
	}

	async consume() {
		if(this.#consuming) {
			this.logger.trace('already consuming, ignored')
			return
		}

		if(this.#consumeDebounce) {
			clearTimeout(this.#consumeDebounce)
		}

		this.#consuming = true
		let client: PoolClient | undefined

		try {
			client = await this.pool.connect()

			this.logger.debug('got client, starting consumption')
			let rowsDone = 0
			for(;;) {
				if(!this.#consuming) {
					throw new Error('aborted consumption')
				}

				const _rows = await this.#consumeBatch(client)
				rowsDone += _rows
				if(_rows < this.opts.batchSize) {
					break
				}
			}

			this.#consuming = false
			if(rowsDone) {
				this.logger.info({ rowsDone }, 'done consuming')
			}
		} catch(err) {
			this.logger.error({ err }, 'error consuming messages')
		} finally {
			this.#consuming = false
			this.#pendingCount = 0
			client?.release()
		}
	}

	async #consumeBatch(client: PoolClient) {
		await client.query('BEGIN')

		const { rows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, $2)',
			[this.opts.queueName, this.opts.batchSize]
		)
		const msgIds = rows.map((row) => row.id)
		if(!rows.length) {
			await client.query('COMMIT')
			return 0
		}

		let success = false
		try {
			await this.opts.onMessage(this.opts.queueName, rows)
			success = true
		} catch(err) {
			this.logger.error({ err }, 'error processing messages')
		}

		await client.query(
			'SELECT pgmb.ack_msgs($1, $2, $3)',
			[this.opts.queueName, success, `{${msgIds.join(',')}}`]
		)
		await client.query('COMMIT')

		this.logger.debug({ success, msgIds }, 'acked messages')

		return rows.length
	}
}