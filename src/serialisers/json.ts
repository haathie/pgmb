import type { Serialiser } from '../types.ts'

export const JSONSerialiser: Serialiser = {
	encode: obj => Buffer.from(JSON.stringify(obj)),
	decode: buff => JSON.parse(buff.toString()),
	contentType: 'application/json',
	id: 'json'
}