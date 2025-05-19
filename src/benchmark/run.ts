import dotenv from 'dotenv'
dotenv.config({ })

import { threadId, Worker, workerData } from 'worker_threads'
import makeAmqpBenchmarkClient from './amqp'
import { benchmarkConsumption, benchmarkPublishing } from './base'
import makePgmbBenchmarkClient, { install as installPgmb } from './pgmb'
import makePgmqBenchmarkClient, { install as installPgmq } from './pgmq'
import { MakeBenchmarkClient } from './types'

type Client = {
	make: MakeBenchmarkClient
	install?: () => Promise<boolean>
}

const CLIENTS: { [client: string]: Client } = {
	'pgmb': {
		make: makePgmbBenchmarkClient,
		install: installPgmb
	},
	'pgmq': {
		make: makePgmqBenchmarkClient,
		install: installPgmq
	},
	'amqp': {
		make: makeAmqpBenchmarkClient,
	}
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

	const batchSize = getArg('batch')

	const replicas = getArg('replicas') || '1'

	const queues = getArg('queues') || '1'
	const testQueues = [...Array.from({ length: Number(queues) })]
		.flatMap((_, i) => (
			[...Array.from({ length: Number(replicas) })]
				.map(() => `test_queue_${i}`)
		))
	const runWorkers = () => (
		testQueues.map(queueName => (
			new Worker(__filename, {
				workerData: {
					queueName,
					method,
					clientId,
					batchSize: batchSize ? Number(batchSize) : undefined,
				}
			})
		))
	)

	const install = CLIENTS[clientId]?.install
	if(install) {
		install()
			.then(installed => {
				console.log(
					installed
						? `Client ${clientId} installed successfully`
						: `Client ${clientId} already installed`
				)

				return runWorkers()
			})
			.catch(err => {
				console.error('Error installing client', err)
				process.exit(1)
			})
	} else {
		runWorkers()
	}
} else {
	const { queueName, clientId, method, batchSize } = workerData
	const client = CLIENTS[clientId]
	if(!client) {
		throw new Error(
			`Client ${clientId} not found. `
			+ `Available clients: ${Object.keys(CLIENTS).join(', ')}`
		)
	}

	const opts = {
		makeClient: client.make,
		id: `${clientId}-${threadId}`,
		queueName,
		batchSize,
	}

	if(method === 'consume') {
		benchmarkConsumption(opts)
	} else if(method === 'publish') {
		benchmarkPublishing(opts)
	} else {
		throw new Error('INTERNAL: unknown method ' + method)
	}
}