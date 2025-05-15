import dotenv from 'dotenv'
dotenv.config({ })

import { threadId, Worker, workerData } from 'worker_threads'
import makeAmqpBenchmarkClient from './amqp'
import { benchmarkConsumption, benchmarkPublishing } from './base'
import makePgmbBenchmarkClient from './pgmb'
import makePgmqBenchmarkClient from './pgmq'
import { MakeBenchmarkClient } from './types'

const CLIENTS: { [client: string]: MakeBenchmarkClient } = {
	'pgmb': makePgmbBenchmarkClient,
	'pgmq': makePgmqBenchmarkClient,
	'amqp': makeAmqpBenchmarkClient
}

function getArg(name: string) {
	const index = process.argv.indexOf('--' + name)
	if(index === -1) {
		return undefined
	}

	const value = process.argv[index + 1]
	if(!value) {
		return null
	}

	return value
}

if(!workerData) {
	const clientId = getArg('client')
	if(!clientId) {
		throw new Error('Please specify --client <client>')
	}

	const method = getArg('consume') ? 'consume' : getArg('publish') ? 'publish' : null
	if(!method) {
		throw new Error('Please specify --consume or --publish')
	}

	const queues = getArg('queues') || '1'
	const testQueues = [...Array.from({ length: Number(queues) })]
		.map((_, i) => `test_queue_${i}`)

	testQueues.map(queueName => (
		new Worker(__filename, {
			workerData: { queueName, method, clientId }
		})
	))
} else {
	const { queueName, clientId, method } = workerData
	const makeClient = CLIENTS[clientId]
	if(!makeClient) {
		throw new Error(
			`Client ${clientId} not found. `
			+ `Available clients: ${Object.keys(CLIENTS).join(', ')}`
		)
	}

	const opts = {
		makeClient,
		id: `${clientId}-${threadId}`,
		queueName,
	}

	if(method === 'consume') {
		benchmarkConsumption(opts)
	} else if(method === 'publish') {
		benchmarkPublishing(opts)
	} else {
		throw new Error('INTERNAL: unknown method ' + method)
	}
}