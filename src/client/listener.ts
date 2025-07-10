import type { Notification, Pool, PoolClient } from 'pg'
import type { Logger } from 'pino'
import type { PGMBNotification, PGMBNotificationData } from '../types'
import { delay, getChannelNameForQueue, getQueueNameFromChannel } from '../utils'

const CLIENT_REMOVED_ERR = 'Listener client removed from pool'

/**
 * PGMBListener is a class that listens for notifications on multiple
 * PostgreSQL channels. It uses a connection pool to obtain a connection
 * for listening to notifications and handles connection failures
 * automatically.
 */
export class PGMBListener {

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
		this.#onClientRemoved = this.#onClientRemoved.bind(this)
	}

	async subscribe(...queueNames: string[]) {
		queueNames = Array.from(new Set(queueNames))
		this.#subscribedQueues = queueNames
		this.#client = await this.#assertConnection()

		const queueListenStmts = queueNames
			.map(queueName => `LISTEN ${getChannelNameForQueue(queueName)}`)
			.join(';')
		await this.#client.query(
			`BEGIN; UNLISTEN *; ${queueListenStmts}; END;`
		)

		// release the client back to the pool -- so it can be used for
		// queries
		this.#client.release()
	}

	async close() {
		this.pool.off('remove', this.#onClientRemoved)
		this.#reconnectAttempt = 0
		if(!this.#client) {
			return
		}

		this.#client.off('error', this.#onListenerError)
		this.#client.off('notification', this.#onNotification)
		try {
			await this.#client.query('UNLISTEN *')
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

		const level = err.message === CLIENT_REMOVED_ERR ? 'debug' : 'warn'
		this.logger[level]({ err }, 'listener error, will reconnect...')
		this.#client = undefined
		this.#reconnectAttempt ++

		this.subscribe(...this.#subscribedQueues)
			// swallow errors
			.catch(() => { })
	}

	#onClientRemoved = (client: PoolClient) => {
		if(client !== this.#client) {
			return
		}

		this.#onListenerError(new Error(CLIENT_REMOVED_ERR))
	}

	async #assertConnection() {
		if(this.#client) {
			return this.#client
		}

		// ensure onClientRemoved is only added once
		this.pool.off('remove', this.#onClientRemoved)

		this.#reconnectAttempt ++

		while(this.#reconnectAttempt) {
			try {
				this.#client = await this.pool.connect()
				this.#client.on('error', this.#onListenerError)
				this.#client.on('notification', this.#onNotification)
				this.pool.on('remove', this.#onClientRemoved)
				// if we reconnected & the connection was closed
				// mark as connection aborted
				if(!this.#reconnectAttempt) {
					break
				}

				this.#reconnectAttempt = 0
				this.logger.trace('connected to PG')
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