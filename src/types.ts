import type { Pool } from 'pg'
import type { Logger } from 'pino'

export type PGMBClientOpts = {
	/**
	 * Provide a connection pool to use.
	 */
	pool: Pool

	logger: Logger
	/**
	 * Add consumers to the client. This will automatically
	 * start consuming messages.
	 * Note: Ensure the queues set here are created before
	 * calling `client.listen()`
	 */
	consumers?: PGMBConsumerOpts[]
}

export type PGMBConsumerOpts = {
	queueName: string
	/**
	 * Number of messages to consume at once.
	 */
	batchSize: number
	/**
	 * Number of milliseconds to wait after receiving a message
	 * before processing the next batch. This is useful to
	 * process messages in batches
	 */
	debounceIntervalMs?: number
	/**
	 * Process messages in the queue. Rejecting a message will
	 * automatically nack it, resolving it will ack it.
	 */
	onMessage(queueName: string, msgs: PgIncomingMessage[]): Promise<void> | void
}

export type PgIncomingMessage = {
	id: string
	message: Uint8Array
	headers: Record<string, any>
}

export type PgEnqueueMsg = {
	message: Uint8Array | string
	headers?: Record<string, any>
	consumeAt?: Date
}

export type PgPublishMsg = PgEnqueueMsg & {
	/**
	 * Queue or exchange name to send the message to.
	 */
	route: string
}

export type PGMBNotificationData = { count: number }

export type PGMBNotification = {
	type: 'message'
	queueName: string
	data: PGMBNotificationData
} | {
	type: 'connection'
}

export type PGMBAssertQueueOpts = {
	name: string
	ackSetting?: 'archive' | 'delete'
	defaultHeaders?: { [key: string]: any }
}