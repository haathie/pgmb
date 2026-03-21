import { PGlite } from '@electric-sql/pglite'
import { btree_gin } from '@electric-sql/pglite/contrib/btree_gin'
import assert from 'node:assert'
import { readFile } from 'node:fs/promises'
import { after, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { pino } from 'pino'
import { type PgClientLike, PgmbClient } from '../src/index.ts'

const LOGGER = pino()

// incomplete tests for pglite. Just making sure the basics work
describe('PGLite Tests', () => {
	// eslint-disable-next-line camelcase
	const pglite = new PGlite({ extensions: { btree_gin } })

	let client: PgmbClient
	let groupId: string

	before(async() => {
		const sql = await readFile('./sql/pgmb.sql', 'utf-8')
		await pglite.exec(sql)
	})

	after(async() => {
		await client.end()
		await pglite.close()
	})

	beforeEach(async() => {
		groupId = `grp${Math.random().toString(36).substring(2, 15)}`

		client = new PgmbClient({
			client: pglite as unknown as PgClientLike,
			logger: LOGGER,
			groupId,
			readEventsIntervalMs: 250,
			subscriptionMaintenanceMs: 1000,
			maxActiveCheckpoints: 3,
		})
		await client.init()
	})

	it('should remove expired subs', async() => {
		const sub = await client
			.registerFireAndForgetHandler({ expiryInterval: '1 second' })
		sub.return()

		await setTimeout(1500)

		const {
			rows: [{ count }],
		} = await pglite.query<{ count: string }>(
			'select count(*) as count from pgmb.subscriptions where id = $1',
			[sub.id],
		)
		assert.equal(count, '0')
	})
})
