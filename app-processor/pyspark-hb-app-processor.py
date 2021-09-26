# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>
# <markdowncell>
# Markdown cells are embedded in comments, so the file is a valid `python` script.
# <codecell>
from pyspark.sql import SparkSession
import yaml

# Use s3a for accessing the parquet file in case it exceeds 5GB and for perf enhancements over s3n and native s3
S3_URL = "s3a://hb-data-eng-test/events/part-00000-d3813b59-7d2f-429b-80ed-76bddb41e9bb-c000.snappy.parquet"
SPARK_CONF_FILE_LOCATION = "./conf/spark-conf.yml"
# run with 4 cores
SPARK_MASTER_URL = "local[4]"
SPARK_APP_NAME = "PySpark HB Application Processor"
SPARK_SESSION = None


def find_top_n_locations(spark_session, url, n=20):
    df = spark_session.read.parquet(url)
    df.createOrReplaceTempView('T')
    # Count on location_id so that we skip null ones which are probably not too useful anyways
    spark_session.sql(
        'SELECT LOCATION_ID, COUNT(LOCATION_ID) AS TOTAL_COUNT FROM T ' +
        'GROUP BY LOCATION_ID ORDER BY TOTAL_COUNT DESC LIMIT ' + str(n))\
        .show(n, False, False)  # pass in `n` in show as well, in case predicate pushdown of limit is not supported


def parse_configs(conf_location):
    with open(conf_location, 'r') as conf_fd:
        return yaml.safe_load(conf_fd)


# TODO: Note: passing in global variables to make this testable potentially via DI
def get_or_generate_spark_session(conf_location, spark_master_url, spark_app_name):
    conf_map = parse_configs(conf_location)
    spark_session_builder = SparkSession.builder \
        .master(spark_master_url) \
        .appName(spark_app_name)
    keys = list(conf_map)
    for k in keys:
        spark_session_builder.config(k, conf_map[k])
    return spark_session_builder.getOrCreate()


def main():
    # Make this global so we can interact with it in Jupyter
    global SPARK_SESSION
    SPARK_SESSION = get_or_generate_spark_session(SPARK_CONF_FILE_LOCATION, SPARK_MASTER_URL, SPARK_APP_NAME)
    # task b)
    find_top_n_locations(SPARK_SESSION, S3_URL, 20)


if __name__ == "__main__":
    main()

# <rawcell>
# Raw cell contents are not formatted as markdown
# <markdowncell>