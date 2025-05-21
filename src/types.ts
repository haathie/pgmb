import type { Pool } from 'pg'
import type { Logger } from 'pino'

export type PGMBClientOpts = {
	/**
	 * Provide a connection pool to use.
	 */
	pool: Pool

	logger?: Logger
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

export type PGMBHeaders = {
	/**
	 * Specifys the number of times this message should be retried.
	 * Each element in the array is a retry delay in seconds.
	 * Eg. [5, 10, 15] will retry the message after 5 seconds,
	 * then 10 seconds, then 15 seconds.
	 */
	retriesLeftS?: number[]
	/**
	 * If this message is a retried message, this will be the ID of the
	 * original n-acked message.
	 */
	originalMessageId?: string
	/**
	 * The number of times this message has been retried.
	 */
	tries?: number
} & { [key: string]: any }

export type PgIncomingMessage = {
	id: string
	message: Uint8Array
	headers: PGMBHeaders
}

export type PgEnqueueMsg = {
	message: Uint8Array | string
	headers?: PGMBHeaders
	consumeAt?: Date
}

export type PgPublishMsg = PgEnqueueMsg & {
	/**
	 * Exchange name to send the message to.
	 */
	exchange: string
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
	defaultHeaders?: PGMBHeaders
	type?: 'logged' | 'unlogged'
}

export type PGMBAssertExchangeOpts = {
	name: string
}

export type PGSentMessage = { id: string }