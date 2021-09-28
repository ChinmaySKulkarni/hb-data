# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>
# <markdowncell>
# Markdown cells are embedded in comments, so the file is a valid `python` script.
# <codecell>
from pyspark.sql import SparkSession
import yaml
import great_expectations as ge
import json

# Use s3a for accessing the parquet file in case it exceeds 5GB and for perf enhancements over s3n and native s3
S3_URL = "s3a://hb-data-eng-test/events/part-00000-d3813b59-7d2f-429b-80ed-76bddb41e9bb-c000.snappy.parquet"
HADOOP_CONF_FILE_LOCATION = "conf/hadoop-conf.yml"
EXPECTATIONS_DIR = "expectations"
RUN_WITH_VALIDATION = False
# run with 4 cores
SPARK_MASTER_URL = "local[4]"
SPARK_APP_NAME = "PySpark HB Application Processor"
SPARK_SESSION = None


def find_top_n_locations(n=20):
    # Skip null locations which are probably not too useful anyways
    df = SPARK_SESSION.sql(
        "SELECT LOCATION_ID, COUNT(*) AS TOTAL_COUNT FROM T " +
        "WHERE LOCATION_ID IS NOT NULL "
        "GROUP BY LOCATION_ID ORDER BY TOTAL_COUNT DESC LIMIT " + str(n))

    if RUN_WITH_VALIDATION:
        print("Validating dataframe for task b) via Great Expectations")
        df_ge = ge.dataset.SparkDFDataset(df)
        df_ge.expect_column_to_exist("LOCATION_ID")
        df_ge.expect_column_to_exist("TOTAL_COUNT")
        df_ge.expect_column_values_to_be_of_type("TOTAL_COUNT", "IntegerType")
        df_ge.expect_table_row_count_to_equal(20)
        df_ge.expect_column_values_to_be_unique("LOCATION_ID")
        df_ge.expect_column_values_to_not_be_null("LOCATION_ID")
        df_ge.expect_column_values_to_be_decreasing("TOTAL_COUNT")
        __persist_expectation("task_b_find_top_n_locations", df_ge)
    return df


def find_timesheets_active_first_week():
    df = SPARK_SESSION.sql(
        "SELECT COMPANY_ID, IF(TOTAL_INSTANCES > 0, TRUE, FALSE) AS TIMESHEETS_ACTIVE_FIRST_WEEK " +
        "FROM (SELECT COMPANY_ID, SUM(IF(((PRODUCT_AREA='Timesheets' OR PRODUCT_AREA='timesheets') AND " +
        "(CAST(DATE_FORMAT(CREATED_AT, 'd') AS INT) <= 7)), 1, 0)) AS TOTAL_INSTANCES FROM T GROUP BY COMPANY_ID)")

    if RUN_WITH_VALIDATION:
        print("Validating dataframe for task c) via Great Expectations")
        df_ge = ge.dataset.SparkDFDataset(df)
        df_ge.expect_column_to_exist("COMPANY_ID")
        df_ge.expect_column_to_exist("TIMESHEETS_ACTIVE_FIRST_WEEK")
        df_ge.expect_column_values_to_be_unique("COMPANY_ID")
        df_ge.expect_column_values_to_be_of_type("TIMESHEETS_ACTIVE_FIRST_WEEK", "BooleanType")
        __persist_expectation("task_c_find_timesheets_active_first_week", df_ge)
    return df


def find_events_grouped_by_location_device():
    df = SPARK_SESSION.sql(
        "SELECT LOCATION_ID, DEVICE_OS, COUNT(*) AS TOTAL_EVENTS FROM T " +
        "GROUP BY LOCATION_ID, DEVICE_OS HAVING TOTAL_EVENTS > 0")

    if RUN_WITH_VALIDATION:
        print("Validating dataframe for task d) via Great Expectations")
        df_ge = ge.dataset.SparkDFDataset(df)
        df_ge.expect_column_to_exist("LOCATION_ID")
        df_ge.expect_column_to_exist("DEVICE_OS")
        df_ge.expect_column_to_exist("TOTAL_EVENTS")
        df_ge.expect_compound_columns_to_be_unique(["LOCATION_ID", "DEVICE_OS"])
        df_ge.expect_column_values_to_be_of_type("TOTAL_EVENTS", "IntegerType")
        df_ge.expect_column_values_to_be_between("TOTAL_EVENTS", 0, None, True)
        __persist_expectation("task_d_find_events_grouped_by_location_device", df_ge)
    return df


def parse_configs(conf_location):
    with open(conf_location, 'r') as conf_fd:
        return yaml.safe_load(conf_fd)


def init_dataframe():
    df = SPARK_SESSION.read.parquet(S3_URL)
    df.createOrReplaceTempView('T')

    if RUN_WITH_VALIDATION:
        print("Validating dataframe for task a) via Great Expectations")
        df_ge = ge.dataset.SparkDFDataset(df)
        # validate expected schema
        df_ge.expect_column_to_exist("COMPANY_ID")
        df_ge.expect_column_values_to_be_of_type("COMPANY_ID", "IntegerType")
        df_ge.expect_column_to_exist("LOCATION_ID")
        df_ge.expect_column_values_to_be_of_type("LOCATION_ID", "IntegerType")
        df_ge.expect_column_to_exist("USER_ID")
        df_ge.expect_column_values_to_be_of_type("USER_ID", "IntegerType")
        df_ge.expect_column_to_exist("CREATED_AT")
        df_ge.expect_column_values_to_be_of_type("CREATED_AT", "TimestampType")
        df_ge.expect_column_to_exist("PRODUCT_AREA")
        df_ge.expect_column_values_to_be_of_type("PRODUCT_AREA", "StringType")
        df_ge.expect_column_to_exist("EVENT_ACTION")
        df_ge.expect_column_values_to_be_of_type("EVENT_ACTION", "StringType")
        df_ge.expect_column_to_exist("DEVICE_OS")
        df_ge.expect_column_values_to_be_of_type("DEVICE_OS", "StringType")
        __persist_expectation("task_a_init_dataframe", df_ge)


# Note: passing in global variables to make this easily testable via dependency injection
def get_or_generate_spark_session(spark_session_builder, conf_map, spark_master_url, spark_app_name):
    spark_session_builder = spark_session_builder \
        .master(spark_master_url) \
        .appName(spark_app_name)
    keys = list(conf_map)
    for k in keys:
        spark_session_builder.config(k, conf_map[k])
    return spark_session_builder.getOrCreate()


# persist expectations for each task. We can use these expectations to
# validate our dataframes against new data in the future
def __persist_expectation(task_name, df_ge):
    with open(EXPECTATIONS_DIR + "/expectation_" + task_name + ".json", "w") as fd:
        fd.write(json.dumps(df_ge.get_expectation_suite().to_json_dict()))


def main():
    # Make these global so we can interact with it in Jupyter
    global SPARK_SESSION
    print("******* Task a) Reading input Parquet data from S3 into a Dataframe *******")
    conf_map = parse_configs(HADOOP_CONF_FILE_LOCATION)
    SPARK_SESSION = get_or_generate_spark_session(SparkSession.builder, conf_map, SPARK_MASTER_URL, SPARK_APP_NAME)
    init_dataframe()

    print("******* Task b) Retrieving the top 20 events grouped by location *******")
    df_b = find_top_n_locations(20)
    # pass in `20` in show as well, in case predicate push-down of limit is not supported
    df_b.show(20, False, False)

    print("******* Task c) Retrieving if there were active timesheets in the first week for each company *******")
    df_c = find_timesheets_active_first_week()
    df_c.show(50, False, False)

    print("******* Task d) Retrieving events grouped by location_id and device_os *******")
    df_d = find_events_grouped_by_location_device()
    df_d.show(50, False, False)


if __name__ == "__main__":
    main()

# <rawcell>
# Raw cell contents are not formatted as markdown
# <markdowncell>
