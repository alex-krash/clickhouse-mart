from clickhouse_driver import Client

from mart import MartProcessor

def test_all():

    client = Client("localhost")
    mart = MartProcessor(client,
                         "test_cluster_two_shards_localhost",
                         "tmp.mart",
                         "SELECT '{partition}', rand(), {shard} FROM numbers(1)")
    mart.run("2020-05-25")

