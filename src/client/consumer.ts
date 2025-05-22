import type { Pool, PoolClient } from 'pg'
import type { Logger } from 'pino'
import type { PGMBConsumerOpts, PGMBHeaders, PgTypedIncomingMessage, Serialiser } from '../types'

type PGMBMessageRecord = {
	id: string
	message: Uint8Array | Buffer
	headers: PGMBHeaders
}

export class PGMBConsumer<Q, M, Default> {

	#pendingCount = 0
	#consumeDebounce: NodeJS.Timeout | undefined
	#abortedConsuming = false
	#currentConsumeTask: Promise<void> | undefined

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

	async close() {
		clearTimeout(this.#consumeDebounce)
		this.#abortedConsuming = true
		this.#pendingCount = 0
		await this.#currentConsumeTask
	}

	async consume() {
		if(this.#currentConsumeTask) {
			this.logger.trace('already consuming, ignored')
			return
		}

		if(this.#consumeDebounce) {
			clearTimeout(this.#consumeDebounce)
		}

		this.#currentConsumeTask = this.#consume()
			.finally(() => {
				this.#currentConsumeTask = undefined
			})
		return this.#currentConsumeTask
	}

	async #consume() {
		this.#abortedConsuming = false
		let client: PoolClient | undefined

		try {
			client = await this.pool.connect()

			this.logger.debug('got client, starting consumption')
			let rowsDone = 0
			for(;;) {
				if(this.#abortedConsuming) {
					throw new Error('Aborted consumption task')
				}

				const _rows = await this.#consumeBatch(client)
				rowsDone += _rows
				if(_rows < this.opts.batchSize) {
					break
				}
			}

			if(rowsDone) {
				this.logger.info({ rowsDone }, 'done consuming')
			}
		} catch(err) {
			this.logger.error({ err }, 'error consuming messages')
		} finally {
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
		if(!rows.length) {
			await client.query('COMMIT')
			return 0
		}

		const pendingMsgSet = new Set<string>()
		const successMsgs: string[] = []
		const failMsgs: string[] = []
		const msgs: PgTypedIncomingMessage<M, Default>[] = []
		for(const row of rows) {
			let message: unknown = row.message
			if(this.serialiser) {
				try {
					message = this.serialiser.decode(row.message)
				} catch(err) {
					this.logger.error(
						{ err, id: row.id, serialiser: this.serialiser.id },
						'error decoding message'
					)
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
			pendingMsgSet.add(row.id)
		}

		if(pendingMsgSet.size) {
			try {
				await this.opts.onMessage({
					queueName: this.opts.name,
					msgs,
					ack(success, ...msgIds) {
						for(const id of msgIds) {
							if(!pendingMsgSet.has(id)) {
								throw new Error(`Message ${id} not in batch, or already marked`)
							}

							pendingMsgSet.delete(id)
						}

						if(success) {
							successMsgs.push(...msgIds)
						} else {
							failMsgs.push(...msgIds)
						}
					},
				})

				successMsgs.push(...Array.from(pendingMsgSet))
			} catch(err) {
				this.logger.error(
					{ err, failed: pendingMsgSet.size },
					'error processing messages'
				)
				failMsgs.push(...Array.from(pendingMsgSet))
			}
		}

		await Promise.all(
			[
				!!successMsgs.length && client.query(
					'SELECT pgmb.ack_msgs($1, true, $2)',
					[this.opts.name, `{${successMsgs.join(',')}}`]
				),
				!!failMsgs.length && client.query(
					'SELECT pgmb.ack_msgs($1, false, $2)',
					[this.opts.name, `{${failMsgs.join(',')}}`]
				)
			]
		)

		await client.query('COMMIT')

		this.logger.debug(
			{ success: successMsgs, fail: failMsgs },
			'acked messages'
		)

		return rows.length
	}
}