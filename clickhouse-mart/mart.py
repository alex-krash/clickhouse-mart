from clickhouse_driver import Client
from abc import ABC, abstractmethod
import hashlib, re, copy


class Table:
    def __init__(self, database: str, table: str):
        self.database = database
        self.table = table

    def __str__(self):
        return self.database + "." + self.table

    def hash(self):
        return self.database + "_" + self.table


# Base class for operating marts
class AbstractMartProcessor(ABC):
    def __init__(self, client: Client, cluster_name: str):
        # we assume, that cluster_name is a name of "normal" cluster, where all the hosts are
        # normally installed as parts of shards
        self.client = client
        self.cluster = cluster_name

    @property
    @abstractmethod
    def table(self) -> Table:
        # Returns name of physical MergeTree table, storing the data
        pass

    @abstractmethod
    def population_query(self, partition, shard_number: int) -> str:
        # Returns query for populating single partition of mart
        pass

    # number of shards to use for this mart population
    def shards_count(self) -> int:
        return 1

        # runs population of a particular partition
    def run(self, partition_object):

        temp_table_name = self.table.hash() + "_" + hashlib.md5(str(partition_object).encode()).hexdigest()

        self.prepare_temp_table(temp_table_name)

        shard_count = self.shards_count()

        if shard_count <= 0:
            raise RuntimeError("Bad configuration: shard count should be >= 1, but '{}' is given".format(shard_count))

        for i in range(shard_count):
            self.populate_data_for_shard(temp_table_name, partition_object, i)

        self.clean_partition_on_cluster(partition_object)

        self.move_data_to_permanent_table(temp_table_name, partition_object)

        self.drop_temp_table(temp_table_name)

    # Method drops temporary table and re-creates it according to structure of permanent table
    def prepare_temp_table(self, temp_table_name):
        self.client.execute("DROP TABLE IF EXISTS tmp.{}".format(temp_table_name))
        # this will create a "temporary" MergeTree table with structure, similar to persistent
        self.client.execute(self.get_temp_table_ddl(temp_table_name))

    # Insert part of data, covering some amount of source
    def populate_data_for_shard(self, temp_table_name, partition_object, i):
        population_query = self.population_query(partition_object, i)
        self.client.execute('INSERT INTO tmp.{temp} {population_query}'.format(
            temp=temp_table_name, population_query=population_query))

    # In order to make possible partition manipulations, we need to infer
    # engine DDL, and then cut off Replicated*(*) part, replacing it with MergeTree
    def get_temp_table_ddl(self, temp_table_name):
        engine_ddl = self.client.execute("SELECT engine_full FROM system.tables "
                                         "WHERE database='{database}' AND name='{table}' LIMIT 1"
                                         .format(database=self.table.database, table=self.table.table))
        if not len(engine_ddl):
            raise RuntimeError("Destination table '{}' not found".format(self.table))

        # replace ReplicatedReplacingMergeTree('path', 'replica') -> ReplacingMergeTree
        engine_ddl = engine_ddl[0][0]
        engine_ddl = re.sub(r'Replicated(?P<engine>\w+)\(.*\)', '\g<engine>', engine_ddl)

        return 'CREATE TABLE tmp.{temp} AS {real} ENGINE = {engine_dll}' \
            .format(temp=temp_table_name, real=self.table, engine_dll=engine_ddl)

    def clean_partition_on_cluster(self, partition_object):
        """
            This method cleans out any presence of partition on cluster
            It may be a copy of partition on the whole cluster, may be a well-designed even-spreaded partition
        """
        # here we define, which of hosts in cluster may contain a piece of data
        # assuming, we are picking random host for initial population of table
        alive_replicas = self.client.execute(
            "SELECT hostName() AS host FROM cluster('{}', system.one)".format(self.cluster))
        if len(alive_replicas) == 0:
            raise RuntimeError("Failed to retrieve alive replicas for cluster '{}'".format(self.cluster))

        # closing current client, in order to copy it with another params
        self.client.disconnect()
        for replica in alive_replicas:
            query = "ALTER TABLE {table} DROP PARTITION '{partition}'".format(table=self.table,
                                                                              partition=partition_object)
            client = copy.deepcopy(self.client)
            client.connection.host = replica[0]
            client.execute(query)

    def move_data_to_permanent_table(self, temp_table_name: str, partition_object):
        self.client.execute("ALTER TABLE {real} ATTACH PARTITION '{partition}' FROM tmp.{temp}"
                            .format(real=self.table, partition=partition_object, temp=temp_table_name))

    def drop_temp_table(self, temp_table_name: str):
        self.client.execute("DROP TABLE IF EXISTS tmp.{temp}".format(temp=temp_table_name))


class MartProcessor(AbstractMartProcessor):

    def __init__(self, client, cluster, table, query):

        if isinstance(table, str):
            parts = re.split("\.", table)
            if len(parts) != 2:
                raise RuntimeError("Bad table name '{}'".format(table))
            table = Table(parts[0], parts[1])

        AbstractMartProcessor.__init__(self, client, cluster)
        self.__table = table
        self.__query = query
        self.__shards = 1

    @property
    def table(self) -> Table:
        return self.__table

    def shards_count(self) -> int:
        return self.__shards

    def set_shards(self, shard_count : int):
        self.__shards = shard_count

    def population_query(self, partition, shard_number: int) -> str:
        return self.__query.format(partition=partition, shard=shard_number)