import type { Notification, Pool, PoolClient } from 'pg'
import type { Logger } from 'pino'
import type { PgEnqueueMsg, PGMBClientOpts, PGMBConsumerOpts, PGMBNotification, PGMBNotificationData, PgPublishMsg } from './types'
import { delay, getChannelNameForQueue, getQueueNameFromChannel, serialisePgMsgConstructorsIntoSql } from './utils'

export class PGMBClient {

	#pool: Pool
	#consumers: PGMBConsumer[]
	#logger: Logger
	#listener: PGMBListener | undefined

	constructor({
		pool,
		logger,
		consumers = [],
	}: PGMBClientOpts) {
		this.#pool = pool
		this.#consumers = consumers.map(s => (
			new PGMBConsumer(
				this.#pool,	s, logger.child({ queue: s.queueName }),
			)
		))
		this.#logger = logger
		this.#onNotification = this.#onNotification.bind(this)
	}

	async open() {
		const subQueues = this.#consumers
			.map(s => s.getSubscription().queueName)
		if(!this.#listener && subQueues.length) {
			this.#listener = new PGMBListener(
				this.#pool,
				this.#onNotification,
				this.#logger,
			)
			await this.#listener.setSubscriptions(...subQueues)
		}

		this.#logger.info({ queues: subQueues.length }, 'opened client')
	}

	async close() {
		await this.#listener?.close()
		await Promise.all(
			this.#consumers.map(s => s.close())
		)

		this.#listener = undefined
	}

	async assertQueue(name: string) {
		const { rows: [{ created }] } = await this.#pool.query(
			'SELECT pgmb.assert_queue($1) AS "created"', [name]
		)
		return created as boolean
	}

	async sendToQueue(queueName: string, ...messages: PgEnqueueMsg[]) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params]
			= serialisePgMsgConstructorsIntoSql(messages, [queueName])
		const { rows } = await this.#pool
			.query(`SELECT pgmb.send_to_queue($1, ${arraySql})`, params)
		return rows
	}

	async publishToExchange(...messages: PgPublishMsg[]) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params] = serialisePgMsgConstructorsIntoSql(messages, [])
		const { rows } = await this.#pool
			.query(`SELECT pgmb.publish_to_exchange(${arraySql})`, params)
		return rows
	}

	#onNotification = (notif: PGMBNotification) => {
		if(notif.type === 'message') {
			for(const sub of this.#consumers) {
				if(sub.getSubscription().queueName === notif.queueName) {
					sub.onMessage(notif.data.count)
				}
			}
		} else {
			Promise.all(this.#consumers.map(s => s.consume()))
		}
	}
}

class PGMBConsumer {

	#consuming = false
	#pendingCount = 0
	#consumeDebounce: NodeJS.Timeout | undefined

	constructor(
		private pool: Pool,
		private opts: PGMBConsumerOpts,
		private logger: Logger,
	) {}

	getSubscription() {
		return this.opts
	}

	onMessage(count: number) {
		this.#pendingCount += count
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
		} finally {
			this.#consuming = false
			this.#pendingCount = 0
			client?.release()
		}
	}

	async #consumeBatch(client: PoolClient) {
		await client.query('BEGIN')

		const { rows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, $2) AS id',
			[this.opts.queueName, this.opts.batchSize]
		)
		if(!rows.length) {
			await client.query('COMMIT')
			return 0
		}

		let success = false
		try {
			await this.opts.onMessage(rows)
			success = true
		} catch(err) {
			this.logger.error({ err }, 'error processing messages')
		}

		await client.query(
			'SELECT pgmb.ack_msgs($1, $2, $3)',
			[
				this.opts.queueName,
				success,
				`{${rows.map((row) => row.id).join(',')}}`
			]
		)
		await client.query('COMMIT')

		this.logger.debug({ rows: rows.map(r => r.id) }, 'acknowledged messages')

		return rows.length
	}
}

class PGMBListener {

	static RECONNECT_DELAY_MS = 2500

	#subscribedQueues: string[] = []
	#reconnectAttempt = 0
	#client: PoolClient | undefined

	constructor(
		private pool: Pool,
		private onNotification: (msg: PGMBNotification) => void,
		private logger: Logger,
	) {
		this.#onNotification = this.#onNotification.bind(this)
		this.#onListenerError = this.#onListenerError.bind(this)
	}

	async setSubscriptions(...queueNames: string[]) {
		queueNames = Array.from(new Set(queueNames))
		this.#subscribedQueues = queueNames
		this.#client = await this.#assertConnection()

		await this.#client.query('UNLISTEN *')
		for(const queueName of queueNames) {
			await this.#client.query(`LISTEN ${getChannelNameForQueue(queueName)}`)
		}
	}

	async close() {
		this.#reconnectAttempt = 0
		if(!this.#client) {
			return
		}

		this.#client.off('error', this.#onListenerError)
		this.#client.off('notification', this.#onNotification)
		try {
			await this.#client.query('UNLISTEN *')
			this.#client?.release()
		} catch(err) {
			if(err.message.includes('already been released')) {
				return
			}

			throw err
		} finally {
			this.#client = undefined
		}
	}

	#onNotification = (notif: Notification) => {
		const queueName = getQueueNameFromChannel(notif.channel)
		if(!queueName || !this.#subscribedQueues.includes(queueName)) {
			return
		}

		let data: PGMBNotificationData
		try {
			data = JSON.parse(notif.payload!)
		} catch(err) {
			this.logger.error({ err, notif }, 'error parsing notification payload')
			data = { count: -1 }
		}

		this.logger.trace({ count: data?.count, queueName }, 'got notification')
		this.onNotification({ type: 'message', queueName, data })
	}

	#onListenerError = (err: Error) => {
		if(this.#reconnectAttempt) {
			this.logger.debug('already reconnecting, ignoring error')
			return
		}

		this.logger.error({ err }, 'listener error, will reconnect...')
		this.#client = undefined
		this.#reconnectAttempt ++

		this.setSubscriptions(...this.#subscribedQueues)
			// swallow errors
			.catch(() => { })
	}

	async #assertConnection() {
		if(this.#client) {
			return this.#client
		}

		this.#reconnectAttempt ++

		while(this.#reconnectAttempt) {
			try {
				this.#client = await this.pool.connect()
				this.#client.on('error', this.#onListenerError)
				this.#client.on('notification', this.#onNotification)
				// if we reconnected & the connection was closed
				// mark as connection aborted
				if(!this.#reconnectAttempt) {
					break
				}

				this.#reconnectAttempt = 0
				this.logger.info('connected to PG')
				this.onNotification({ type: 'connection' })
				return this.#client
			} catch(err) {
				this.logger.error(
					{ err, attempt: this.#reconnectAttempt },
					'error reconnecting listener'
				)
				this.#reconnectAttempt ++
				// wait some time before retrying
				await delay(PGMBListener.RECONNECT_DELAY_MS)
			}
		}

		throw new Error('aborted reconnect')
	}
}