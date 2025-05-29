PG_URI='postgres://postgres:@localhost:5432/test'

psql $PG_URI -c "select pgmb.uninstall();" -f sql/pgmb.sql -1
echo "Recreated pgmb schema."