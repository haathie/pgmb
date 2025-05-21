import { deserialize as V8Decode, serialize as V8Encode } from 'v8'
import type { Serialiser } from '../types'

export const V8Serialiser: Serialiser = {
	id: 'v8',
	contentType: 'nodejs/v8',
	encode: obj => {
		try {
			return V8Encode(obj)
		} catch(error) {
			obj = JSON.parse(JSON.stringify(obj))
			return V8Encode(obj)
		}
	},
	decode: buff => V8Decode(buff)
}