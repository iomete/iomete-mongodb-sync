from pyspark.sql import SparkSession

import job

spark = SparkSession.builder.appName(job.JOB_NAME + "-test").getOrCreate()

"""
Note: Before running test, make sure loca mongodb docker is running. To start the mongodb docker, run:
make mongodb-start

To stop the mongodb docker, run:
make mongodb-stop
"""


def test_reading_application_config():
    application_config = job.read_application_config()

    assert application_config.get_string("connection_string") == "mongodb://root:rootpassword@localhost:27017"

    syncs = application_config.get_list("syncs")
    assert len(syncs) == 1

    sync = syncs[0]
    assert sync.get_string("source_database") == "local"
    assert sync.get_list("source_collections") == ["startup_log"]
    assert sync.get_string("destination_database") == "mongo_local_db"


def test_sync_mongo_tables():
    application_config = job.read_application_config()
    job.sync_mongo_tables(application_config=application_config)

    df = spark.sql("select * from mongo_local_db.startup_log")
    df.printSchema()
    df.show(truncate=False, n=100)

    assert df.count() == 2
