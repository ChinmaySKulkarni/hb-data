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


def find_top_n_locations(spark_session, n=20):
    # Skip null locations which are probably not too useful anyways
    return spark_session.sql(
        "SELECT LOCATION_ID, COUNT(*) AS TOTAL_COUNT FROM T " +
        "WHERE LOCATION_ID IS NOT NULL "
        "GROUP BY LOCATION_ID ORDER BY TOTAL_COUNT DESC LIMIT " + str(n))


def find_timesheets_active_first_week(spark_session):
    return spark_session.sql(
        "SELECT COMPANY_ID, IF(TOTAL_INSTANCES > 0, TRUE, FALSE) AS TIMESHEETS_ACTIVE_FIRST_WEEK " +
        "FROM (SELECT COMPANY_ID, SUM(IF(((PRODUCT_AREA='Timesheets' OR PRODUCT_AREA='timesheets') AND " +
        "(CAST(DATE_FORMAT(CREATED_AT, 'd') AS INT) <= 7)), 1, 0)) AS TOTAL_INSTANCES FROM T GROUP BY COMPANY_ID)")


def find_events_grouped_by_location_device(spark_session):
    return spark_session.sql(
        "SELECT LOCATION_ID, DEVICE_OS, COUNT(*) AS TOTAL_EVENTS FROM T " +
        "GROUP BY LOCATION_ID, DEVICE_OS HAVING TOTAL_EVENTS > 0")


def parse_configs(conf_location):
    with open(conf_location, 'r') as conf_fd:
        return yaml.safe_load(conf_fd)


def init_dataframe(spark_session, url):
    spark_session.read.parquet(url).createOrReplaceTempView('T')


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
    # Make these global so we can interact with it in Jupyter
    global SPARK_SESSION
    print("Task A) Reading input Parquet data from S3 into a Dataframe")
    SPARK_SESSION = get_or_generate_spark_session(SPARK_CONF_FILE_LOCATION, SPARK_MASTER_URL, SPARK_APP_NAME)
    init_dataframe(SPARK_SESSION, S3_URL)

    print("Task B) Retrieving the top 20 events grouped by location")
    df_b = find_top_n_locations(SPARK_SESSION, 20)
    # pass in `20` in show as well, in case predicate push-down of limit is not supported
    df_b.show(20, False, False)

    print("Task C) Retrieving if there were active timesheets in the first week for each company")
    df_c = find_timesheets_active_first_week(SPARK_SESSION)
    df_c.show(50, False, False)

    print("Task D) Retrieving events grouped by location_id and device_os")
    df_d = find_events_grouped_by_location_device(SPARK_SESSION)
    df_d.show(50, False, False)


if __name__ == "__main__":
    main()

# <rawcell>
# Raw cell contents are not formatted as markdown
# <markdowncell>