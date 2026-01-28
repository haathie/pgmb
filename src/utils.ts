import assert from 'node:assert'
import type { CreateTopicalSubscriptionOpts, IEventData, RegisterSubscriptionParams } from './types'

/**
 * Extract the date from a message ID, same as the PG function
 */
export function getDateFromMessageId(messageId: string) {
	if(!messageId.startsWith('pm')) {
		return undefined
	}

	const micros = parseInt(messageId.slice(2, 15), 16)
	if(isNaN(micros)) {
		return undefined
	}

	const date = new Date(micros / 1000)
	return date
}

/**
 * Extract the date from a subscription ID
 */
export function getCreateDateFromSubscriptionId(id: string) {
	if(!id.startsWith('su')) {
		return undefined
	}

	return getDateFromMessageId('pm' + id.slice(2))
}

/**
 * Creates subscription params for a subscription that matches
 * 1 or more topics. Also supports partitioning the subscription
 * such that only a subset of messages are received.
 */
export function createTopicalSubscriptionParams<T extends IEventData>({
	topics,
	partition,
	additionalFilters = {},
	additionalParams = {},
	...rest
}: CreateTopicalSubscriptionOpts<T>): RegisterSubscriptionParams {
	assert(topics.length > 0, 'At least one topic must be provided')

	const filters = { ...additionalFilters }
	filters['topics'] ||= 'ARRAY[e.topic]'
	if(partition) {
		filters['partition'] = `hashtext(e.id) % ${partition.total}`
	}

	const strs = Object.entries(filters)
		.map(([k, v]) => `'${k}',${v}`)
	return {
		conditionsSql: `s.params @> jsonb_build_object(${strs.join(',')})`,
		params: { topics, partition: partition?.current, ...additionalParams },
		...rest
	}
}

/**
 * Get an environment variable as a number
 */
export function getEnvNumber(key: string, defaultValue = 0) {
	const num = +(process.env[key] || defaultValue)
	if(isNaN(num) || !isFinite(num)) {
		return defaultValue
	}

	return num
}
