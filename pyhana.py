try:
    from hdbcli import dbapi
except ImportError:
    dbapi = None

HOST = '127.0.0.1'
PORT = 30015
USER = 'SYSTEM'
PASSWORD = '*****'

class MetaTable(object):
    __type__ = 'COLUMN'
    __schema__ = None
    __table__ = None
    __dry__ = dbapi is None

    @property
    def identifier(self):
        return '"{schema}"."{table}"'.format(
            schema=self.__schema__,
            table=self.__table__)

    def __init__(self, schema=None, table_name=None, connection=None):
        self.schema = schema or self.__schema__
        self.table_name = table_name or self.__table__
        self._connection = connection

    def connect(self, address=HOST, port=PORT, user=USER, password=PASSWORD):
        self._connection = dbapi.Connection(address=address, port=port, user=user, password=password)

    def use_connection(self, connection):
        self._connection = connection

    def disconnect(self):
        self._connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def execute(self, *args, **kwargs):
        assert self.__table__ and self.__schema__
        #TODO: really needed?
        if self.__dry__:
            print args[0]
        else:
            assert self._connection
            with self._connection.cursor() as cursor:
                return cursor.execute(*args, **kwargs)

    def create_table_sql(self, type=None):
        if type is None:
            type = self.__type__ or 'COLUMN'

        parts = ['''CREATE {type} TABLE "{schema}"."{table_name}"'''.format(
            schema=self.schema,
            table_name=self.table_name,
            type=type)
        ]

        columns = []
        for name, ctype in self.__columns__:
            columns.append(
                '"{name}" {type}'.format(name=name, type=ctype)
            )
        parts.append('(\t%s\n)' %',\n\t'.join(columns))

        return '\n'.join(parts)

    def create_table(self, *args, **kwargs):
        self.execute(self.create_table_sql(*args, **kwargs))

    def drop_table_sql(self):
        return '''DROP TABLE %s''' % self.identifier

    def drop_table(self, *args, **kwargs):
        self.execute(self.drop_table_sql(*args, **kwargs))

    def import_from_sql(self, path, sep=';', record_delimiter='\n', threads=None, 
        batch=None, logpath=None):
        
        sql = ['''IMPORT FROM CSV FILE '{path}' INTO "{schema}"."{table}"'''.format(
            path=path,
            schema=self.__schema__,
            table=self.__table__)]
        
        options = []
        if threads:
            options.append('THREADS {threads}'.format(threads=threads))
        if batch:
            options.append('BATH {batch}'.format(batch=batch))
        if record_delimiter:
            options.append("RECORD DELIMITED BY '%s'" %record_delimiter)
        if sep:
            options.append("FIELD DELIMITED BY '%s'" %sep)
        if logpath:
            options.append("ERROR LOG '%s'" %logpath)

        if options:
            sql.append('WITH ' + '\n'.join(options))

        return '\n'.join(sql)

    def import_from(*args, **kwargs):
        self.execute(self.import_from_sql(*args, **kwargs))

    def merge_delta(self):
        self.execute('MERGE DELTA OF "{schema}"."{table_name}"'''.format(
            schema=self.__schema__,
            table_name=self.__table__)
        )
