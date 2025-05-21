import type { Pool, PoolClient } from 'pg'
import type { Logger } from 'pino'
import type { PGMBConsumerOpts, PGMBHeaders, PgTypedIncomingMessage, Serialiser } from '../types'

type PGMBMessageRecord = {
	id: string
	message: Uint8Array | Buffer
	headers: PGMBHeaders
}

export class PGMBConsumer<Q, M, Default> {

	#consuming = false
	#pendingCount = 0
	#consumeDebounce: NodeJS.Timeout | undefined

	constructor(
		private pool: Pool,
		private opts: PGMBConsumerOpts<Q, M, Default>,
		private serialiser: Serialiser | undefined,
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
			[this.opts.name, this.opts.batchSize]
		) as unknown as { rows: PGMBMessageRecord[] }
		const msgIds = rows.map((row) => row.id)
		if(!rows.length) {
			await client.query('COMMIT')
			return 0
		}

		const successMsgs: string[] = []
		const failMsgs: string[] = []
		const msgs: PgTypedIncomingMessage<M, Default>[] = []
		for(const row of rows) {
			let message: unknown = row.message
			if(this.serialiser) {
				try {
					message = this.serialiser.decode(row.message)
				} catch(err) {
					this.logger.error({ err, id: row.id }, 'error decoding message')
					failMsgs.push(row.id)
					continue
				}
			}

			msgs.push({
				id: row.id,
				message,
				headers: row.headers,
				rawMessage: row.message,
				exchange: row.headers.exchange
			} as PgTypedIncomingMessage<M, Default>)
		}

		try {
			await this.opts.onMessage(this.opts.name, msgs)
			successMsgs.push(...msgs.map(m => m.id))
		} catch(err) {
			this.logger.error({ err }, 'error processing messages')
			failMsgs.push(...msgs.map(m => m.id))
		}

		if(successMsgs.length) {
			await client.query(
				'SELECT pgmb.ack_msgs($1, true, $2)',
				[this.opts.name, `{${successMsgs.join(',')}}`]
			)
		}

		if(failMsgs.length) {
			await client.query(
				'SELECT pgmb.ack_msgs($1, false, $2)',
				[this.opts.name, `{${failMsgs.join(',')}}`]
			)
		}

		await client.query('COMMIT')

		this.logger.debug(
			{ success: successMsgs.length, fail: failMsgs.length, msgIds },
			'acked messages'
		)

		return rows.length
	}
}