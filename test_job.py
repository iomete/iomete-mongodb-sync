from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, NullType
import job

spark = SparkSession.builder.appName(job.JOB_NAME + "-test").getOrCreate()

"""
Note: Before running test, make sure loca mongodb docker is running. To start the mongodb docker, run:
make mongodb-start

To stop the mongodb docker, run:
make mongodb-stop
"""


def create_df_with_void_type():
    # Define the list of nested data
    data = [("Alice", 25, {"city": "New York", "state": "NY"}),
            ("Bob", 30, {"city": "San Francisco", "state": "CA"}),
            ("Charlie", 35, {"city": "Seattle", "state": "WA"})]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("score2", NullType(), True)
        ]), True),
        StructField("score1", NullType(), True)
    ])

    # Convert the list of data to a list of tuples with a struct for the location
    rows = [(row[0], row[1], row[2], None) for row in data]

    # Create a DataFrame from the list of tuples using the schema
    df = spark.createDataFrame(rows, schema)

    df.printSchema()
    df.show()

    return df


def test_dropping_null_type_columns():
    df = create_df_with_void_type()

    df = job.drop_null_type_columns(df)

    print("After drop null columns")
    df.printSchema()
    df.show()

    expected_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
        ]), True)
    ])

    assert df.schema == expected_schema


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
