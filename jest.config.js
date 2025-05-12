module.exports = {
	'roots': [
		'<rootDir>/src'
	],
	modulePathIgnorePatterns: [
		'<rootDir>/node_modules'
	],
	'testMatch': [
		'**/tests/*.test.ts',
	],
	'transform': {
		'^.+\\.(ts|tsx)$': '@swc/jest'
	},
	'setupFiles': [
		'./src/tests/setup.ts'
	]
}