import happybase

class HbaseDriver(object):
    def __init__(self):
        self._connection = None

    def connection(self):
        if not self._connection:
            self._connection = happybase.Connection('localhost')
        return self._connection

    def table_exists(self, table):
        return table in self.connection().tables()

    def create_table(self, table, schema):
        if not self.table_exists(table):
            self.connection().create_table(table, schema)

    def drop_table(self, table):
        if self.table_exists(table):
            self.connection().delete_table(table, disable=True)


# Cassandra
#################
# keyspace_exists
# tables_exists
# create_keyspace
# drop_keyspace
# set_keyspace
