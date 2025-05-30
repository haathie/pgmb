import type { PgEnqueueMsg, PgPublishMsg } from './types'

export function getChannelNameForQueue(queueName: string) {
	return `chn_${queueName}`
}

export function getQueueNameFromChannel(channelName: string) {
	if(!channelName.startsWith('chn_')) {
		return undefined
	}

	return channelName.slice(4)
}

export function delay(ms: number) {
	return new Promise(resolve => setTimeout(resolve, ms))
}

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
 * Serialise the messages into a SQL array of pgmb.msg_constructor
 */
export function serialisePgMsgConstructorsIntoSql(
	messages: (PgEnqueueMsg | PgPublishMsg)[],
	params: unknown[] = []
) {
	const type = 'exchange' in messages[0] ? 'publish_msg' : 'enqueue_msg'
	const queryComps = messages.map(({ message, headers, consumeAt, ...rt }) => {
		let str = '('
		if('exchange' in rt) {
			params.push(rt.exchange)
			str += `$${params.length}, `
		}

		params.push(message)
		str += `$${params.length}::bytea, `
		if(headers) {
			params.push(JSON.stringify(headers))
			str += `$${params.length}::jsonb, `
		} else {
			str += 'NULL, '
		}

		if(consumeAt) {
			params.push(consumeAt)
			str += `$${params.length}::timestamptz)`
		} else {
			str += 'NULL)'
		}

		return str
	})

	return [
		`ARRAY[${queryComps.join(',')}]::pgmb.${type}[]`,
		params
	] as const
}