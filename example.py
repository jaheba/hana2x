from pyhana import MetaTable

class WorldPopulation(MetaTable):
    __schema__ = 'WP'
    __table__ = 'WorldPopulation'
    __columns__ = (
        ('id', 'INTEGER'),
        ('firstname', 'NVARCHAR(49)'),
        ('lastname', 'NVARCHAR(49)'),
        ('gender', 'NVARCHAR(1)'),
        ('city', 'NVARCHAR(49)'),
        ('country', 'NVARCHAR(2)'),
        ('date', 'DATE')
        )

table = WorldPopulation()
print table.create_table_sql()
print table.import_from_sql('/mnt/share/mitarbeiter.csv', sep=',')
#table.merge_delta_sql()
