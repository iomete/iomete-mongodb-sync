import os

from pyspark.sql import SparkSession
from pyspark_iomete.utils import get_spark_logger
from pyhocon import ConfigFactory

from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType, NullType

JOB_NAME = "mongo_sync"

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)

APPLICATION_CONFIG_DEFAULT_PATH = "/etc/configs/application.conf"


def read_application_config():
    logger.info(f"Reading application config")

    config_path = os.environ.get("APPLICATION_CONFIG_PATH") or APPLICATION_CONFIG_DEFAULT_PATH
    logger.info(f"application config path: {config_path}")

    return ConfigFactory.parse_file(config_path)


def drop_col(df, struct_nm, delete_struct_child_col_nm):
    fields_to_keep = filter(lambda x: x != delete_struct_child_col_nm, df.select("{}.*".format(struct_nm)).columns)
    fields_to_keep = list(map(lambda x: "{}.{}".format(struct_nm, x), fields_to_keep))
    return df.withColumn(struct_nm, struct(fields_to_keep))


def drop_null_type_columns(df):
    null_type_column_names = []

    def find_null_type_columns(df, prefix=""):
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                next_prefix = f"{prefix}.{field.name}" if prefix else field.name
                find_null_type_columns(df.select(col(field.name + ".*")), prefix=next_prefix)
            elif isinstance(field.dataType, NullType):
                null_type_column_names.append(f"{prefix}.{field.name}")

    find_null_type_columns(df)

    if null_type_column_names:
        logger.info("Following columns are null type and will be dropped: {}".format(null_type_column_names))

    struct_name_to_fields = {}
    for field_full_path in null_type_column_names:
        # split the field full path into root struct which is until first dot and field name which is after first dot
        struct_name, field_name = field_full_path.split(".", 1)
        struct_name_to_fields.setdefault(struct_name, []).append(field_name)

    for struct_name, field_names in struct_name_to_fields.items():
        if struct_name == "":
            df = df.drop(*field_names)
        else:
            df = df.withColumn(struct_name, df[struct_name].dropFields(*field_names))

    return df


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
            df.show(truncate=False, n=10)

            df = drop_null_type_columns(df)

            full_table_name = destination_database + "." + destination_table
            df.writeTo(full_table_name).createOrReplace()


if __name__ == '__main__':
    sync_mongo_tables(application_config=read_application_config())
