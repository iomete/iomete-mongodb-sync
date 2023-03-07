import os

from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger
from pyhocon import ConfigFactory

JOB_NAME = "mongo_sync"

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)

APPLICATION_CONFIG_DEFAULT_PATH = "/etc/configs/application.conf"


def read_application_config():
    logger.info(f"Reading application config")

    config_path = os.environ.get("APPLICATION_CONFIG_PATH") or APPLICATION_CONFIG_DEFAULT_PATH
    logger.info(f"application config path: {config_path}")

    return ConfigFactory.parse_file(config_path)


def read_mongo_table(connection_string, database_name, collection_name):
    logger.debug(f"Read mongo table {database_name}.{collection_name}")
    return spark.read.format("mongo") \
        .option("uri", connection_string) \
        .option("database", database_name) \
        .option("collection", collection_name) \
        .load()


def sync_mongo_tables(application_config):
    logger.info(f"Sync mongo tables started. Application config: {application_config}")

    connection_string = application_config.get_string("connection_string")
    syncs = application_config.get_list("syncs")

    for sync in syncs:
        source_database = sync.get_string("source_database")
        source_collections = sync.get_list("source_collections")
        destination_database = sync.get_string("destination_database")

        # create destination database automatically if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {destination_database}")

        for collection in source_collections:
            destination_table = collection  # same as the source collection name

            logger.info(f"Syncing table {source_database}.{collection} to {destination_database}.{destination_table}")

            df = read_mongo_table(connection_string, source_database, collection)

            df.printSchema()
            df.show(truncate=False, n=100)

            full_table_name = destination_database + "." + destination_table
            df.writeTo(full_table_name).createOrReplace()


if __name__ == '__main__':
    sync_mongo_tables(application_config=read_application_config())
