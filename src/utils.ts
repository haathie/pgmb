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
