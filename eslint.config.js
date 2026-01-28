import config from '@adiwajshing/eslint-config'
import { defineConfig } from 'eslint/config'

export default defineConfig(
	[
		{
			files: [
				'**/*.ts'
			],
		},
		{
			ignores: [
				'lib',
				'src/queries.ts',
				'node_modules',
				'dev',
				'sql'
			],
		},
		{
			extends: config
		}
	]
)
