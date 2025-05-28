PG_URI='postgres://postgres:@localhost:5432/test'

psql $PG_URI -c "select pgmb.uninstall();" -1
echo "Uninstalled pgmb schema."
psql $PG_URI -f sql/pgmb.sql -1
echo "Recreated pgmb schema."