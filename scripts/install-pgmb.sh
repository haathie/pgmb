# script to install pgmb, if not already installed

PG_URI=$1
if [ -z "$PG_URI" ]; then
	echo "Usage: $0 <postgres_uri>"
	exit 1
fi

# wait for PostgreSQL to be ready
until psql $PG_URI -c '\q' 2>/dev/null; do
	echo "Waiting for PostgreSQL to be ready..."
	sleep 1
done

# check if schema exists
if ! psql $PG_URI -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pgmb';" | grep -q pgmb; then
	echo "Installing pgmb..."
	psql $PG_URI -f "../sql/pgmb.sql" -1
	echo "pgmb installed successfully."
else
	echo "pgmb schema already exists, skipping installation."
fi