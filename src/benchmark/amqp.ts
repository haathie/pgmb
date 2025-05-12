import amqp from 'amqplib'
import type { BenchmarkClient, BenchmarkClientOpts, MakeBenchmarkClient } from './types'

const makeAmqpBenchmarkClient: MakeBenchmarkClient = async({
	assertQueues, batchSize, publishers: publisherCount, consumers, logger
}: BenchmarkClientOpts): Promise<BenchmarkClient> => {
	// Connect to RabbitMQ
	const uri = 'amqp://user:password@localhost:5672'
	const connection = await amqp.connect(uri)

	// Create a single channel for queue assertions
	const assertChannel = await connection.createConfirmChannel()

	// Assert all queues exist
	for(const queueName of assertQueues) {
		await assertChannel.assertQueue(
			queueName,
			{
				durable: true,
				arguments: {
					// quorum queues allow for delivery limits
					// and retry count tracking
					'x-queue-type': 'quorum',
				}
			}
		)
		logger.info({ queueName }, 'queue asserted')
	}

	// Close the assertion channel as we don't need it anymore
	await assertChannel.close()

	// Create publisher channels and interfaces
	const publisherChannels: amqp.Channel[] = []
	const publisherInterfaces: { publish: (queueName: string, msgs: Uint8Array[]) => Promise<void> }[] = []

	for(let i = 0; i < publisherCount; i++) {
		const channel = await connection.createConfirmChannel()
		publisherChannels.push(channel)

		// Create publisher interface
		publisherInterfaces.push({
			publish: async(queueName: string, msgs: Uint8Array[]) => {
				await Promise.all(msgs.map(async(msg) => (
					new Promise<void>((resolve, reject) => {
						channel.sendToQueue(
							queueName, Buffer.from(msg), { persistent: true },
							(err) => {
								if(err) {
									reject(err)
									return
								}

								resolve()
							}
						)
					})
				)))
			}
		})

		logger.info({ publisherId: i }, 'Publisher created')
	}

	// Create consumer channels
	const consumerChannels: amqp.Channel[] = []

	for(const consumer of consumers) {
		const channel = await connection.createConfirmChannel()
		consumerChannels.push(channel)

		// Set prefetch to batch size to control how many messages are delivered at once
		await channel.prefetch(batchSize)

		// Buffer to accumulate messages for batching
		let messageBuffer: amqp.ConsumeMessage[] = []

		// Start consuming
		await channel.consume(consumer.queueName, async(msg) => {
			if(!msg) {
				return
			}

			// Add message to buffer
			messageBuffer.push(msg)

			// When buffer reaches batch size, process it
			if(messageBuffer.length >= batchSize) {
				const currentBatch = messageBuffer
				messageBuffer = []

				try {
					// Call the consumer's onMessage handler
					await consumer.onMessage(currentBatch.map(item => item.content))

					// Acknowledge all messages in the batch
					for(const item of currentBatch) {
						channel.ack(item)
					}
				} catch(error) {
					logger.error({ error, queueName: consumer.queueName }, 'Error processing message batch')

					// Reject all messages in the batch
					for(const item of currentBatch) {
						channel.nack(item, false, false)
					}
				}
			}
		})

		logger.info({ queueName: consumer.queueName }, 'Consumer started')
	}

	// Return the benchmark client interface
	return {
		publishers: publisherInterfaces,
		close: async() => {
			logger.info('Closing benchmark client')

			// Close all publisher channels
			for(const channel of publisherChannels) {
				await channel.close()
			}

			// Close all consumer channels
			for(const channel of consumerChannels) {
				await channel.close()
			}

			// Close the connection
			await connection.close()

			logger.info('Benchmark client closed')
		}
	}
}

export default makeAmqpBenchmarkClient