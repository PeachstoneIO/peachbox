import cassandra.cluster

class CassandraDriver(object):
    def __init__(self):
        self._session = None
        
    def session(self):
        if not self._session:
            self._session = cassandra.cluster.Cluster().connect()
        return self._session

    def keyspace_exists(self, ks):
        return (ks in self.list_keyspaces())

    def list_keyspaces(self):
        s = self.session()
        current_keyspace = s.keyspace
        s.set_keyspace('system')
        keyspaces = map(lambda k: k.keyspace_name, s.execute('SELECT * FROM schema_keyspaces'))
        if current_keyspace and (current_keyspace in keyspaces): 
            s.set_keyspace(current_keyspace)
        return keyspaces

    def table_exists(self, table):
        tables = self.session().execute('select columnfamily_name from system.schema_columnfamilies \
                where keyspace_name = \'' + self.session().keyspace + '\'')
        tables = map(lambda t: t.columnfamily_name, tables)
        return table in tables

    def create_keyspace(self, ks):
        if not self.keyspace_exists(ks):
            self.session().execute("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy',\
                    'replication_factor':1}" % (ks,))
        self.session().set_keyspace(ks)

    def drop_keyspace(self, ks):
        if self.keyspace_exists(ks):
            self.execute("DROP KEYSPACE %s" % (ks,))
    
    def execute(self, cql):
        return self.session().execute(cql)

    def set_keyspace(self, ks):
        if self.keyspace_exists(ks):
            self.session().set_keyspace(ks)
        else:
            raise ValueError("%s keyspace does not exist" % (ks,))

    def shutdown(self):
        if self._session:
            self._session.shutdown()
            self._session = None

