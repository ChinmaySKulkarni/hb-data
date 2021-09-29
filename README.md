# HB-Data Processing Application

## Description

This project uses PySpark to read and query data stored in Parquet files on S3. We use the Spark DataFrame APIs to
interact with the underlying data. The code is converted to an IPYNB file which you can then view, execute and interact 
with via Jupyter notebook.

The application is packaged as a Docker image and designed to be run on a Linux-based machine which runs Spark in local mode.
More information about prerequisites and running the application can be found below.

### Project Structure
```dockerfile
.
├── Dockerfile
├── LICENSE.md
├── Makefile
├── README.md
├── app_processor   --> Main module 
│   ├── __init__.py
│   ├── __pycache__
│   │   ├── __init__.cpython-39.pyc
│   │   └── pyspark_hb_app_processor.cpython-39.pyc
│   ├── conf    --> Contains confs used for running
│   │   └── hadoop-conf.yml
│   ├── expectations    --> Persisted expectations from data validation
│   │   ├── expectation_task_a_init_dataframe.json
│   │   ├── expectation_task_b_find_top_n_locations.json
│   │   ├── expectation_task_c_find_timesheets_active_first_week.json
│   │   └── expectation_task_d_find_events_grouped_by_location_device.json
│   ├── pyspark_hb_app_processor.ipynb  --> Auto-generated iPynb from pyspark_hb_app_processor.py
│   └── pyspark_hb_app_processor.py     --> Main script
├── requirements.txt
└── tests   --> Contains unit tests and test configs
    ├── test-conf.yml
    └── test_pyspark_hb_app_processor.py
```

## Prerequisites

In order to run this application, there are a small set of prerequisites which you must satisfy:
- System requirements: Linux-based system, 6-8 Gb RAM preferred - To support Docker and a browser comfortably
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) - To locally `clone` this code 
- [Make](https://www.gnu.org/software/make/) - To be able to `make` this project
- [Docker](https://docs.docker.com/get-docker/) - To be able to build docker images and run a container
- Your favorite browser to interact with Jupyter Notebook which will be launched in the container


## Building and Running the Application

Getting started by building and running this application is easy. Since it is containerized, all the dependencies
are resolved and installed as required while baking the image and running the container. 

This means you don't have to worry about doing any of that
yourself, or muddying your environment. The docker image bakes in the necessary versions of Spark, Hadoop, etc.
and Python module dependencies are installed via pip using the `requirements.txt` file. 
Here are the steps:
1. Open your terminal and clone the repository locally:  
    `git clone git@github.com:ChinmaySKulkarni/hb-data.git`
2. Checkout various options to make the project: `make help`, which should output something like this:
    ```makefile
    help                           Get help.
    build                          Run unit tests and build the image
    build-no-cache                 Run unit tests and build the image without using the cache
    run                            Run container with ports exposed for Jupyter Notebook
    up                             Build the image and run container
    stop                           Stop and remove the running container
    destroy                        Forcefully remove the image
    clean                          Stop and remove a running container and also remove the image
    ```
3. To get started quickly and run the application, run:     
    `make up`  
    This will download all required dependencies, set necessary environment variables, build the image, run unit tests, 
    download all Python dependency modules and run the container which will start Jupyter Notebook.
4. You will see a final output mentioning the URL at which Jupyter Notebook is running on your localhost, for ex:
    ```
   Or copy and paste one of these URLs:
        http://dba511b9bed1:8888/?token=2d977c1a56a5f2b501bb71e0734146cb07973ca94a25fd60
   or http://127.0.0.1:8888/?token=2d977c1a56a5f2b501bb71e0734146cb07973ca94a25fd60
    ```
   Go ahead and click on this or copy-paste the URL in your browser of choice
5. When you open Jupyter Notebook in your browser, go ahead and click on the `pyspark_hb_app_processor.ipynb` notebook,
   select the cell containing the Python script and run it. Voilà!

You can also interact with the script by running different parts of it in another cell and by interacting
with the `SPARK_SESSION` and created dataframes. By default, data validation via Great Expectations is disabled 
i.e. `RUN_WITH_VALIDATION = False` and validations are not persisted i.e. `PERSIST_EXPECTATIONS = False`. 
More information on testing and data validation can be found in the next section. 


## Testing and Data Validation

The image build process runs a set of unit tests via Python's `unittest` module (see `test_pyspark_hb_app_processor.py`).
For data validation we use [Great Expectations](https://greatexpectations.io/) to generate expectations for PySpark dataframes.

You can enable validation by setting `RUN_WITH_VALIDATION = True` when running the Jupyter Notebook. This will in turn validate
not just the data being input from the given Parquet file in S3, but also intermediary dataframes.   

Expectations are persisted in the `expectations` directory in json format and can be used to validate new datasets. To overwrite
existing expectations, we can set `PERSIST_EXPECTATIONS = True` when running our script in Jupyter Notebook
if we want to change the expectations.


## Implementation Details and Configurations

This project relies on OpenJDK 11, Spark 3.1.2 (without an in-built hadoop dependency) and introduces necessary Hadoop 2.8.4 
libraries on the Spark driver/executor classpaths such as `hadoop-aws-2.8.4`. We also require the `aws-java-sdk` jar to 
read from S3.

Similarly, necessary Python modules are also installed as part of the build process via `pip` and `requirements.txt`.
This includes PySpark v 3.1.2, Jupytext v 1.13.0 (for converting the `.py` script to `ipynb`) and 
Great Expectations v 0.13.35.

Note that all dependencies are handled while building the image (via Dockerfile) and users don't need to worry about them.
Also invisible to users are the following configs that were set:
- `spark.driver.memory 5g` : Increased from the default to improve the performance of `show()` and other memory-intensive operations on the driver-side
- `spark.executor.memory 2g` : Increased from the default to improve executor performance
- `spark.executor.instances 8`: Increased from the default to improve parallelism of our Spark cluster
- `spark.sql.adaptive.enabled true` : Use [adaptive query execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) to be able to choose more efficient query plans using stats.
- `spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem` : Use `s3a://` for accessing the parquet file
in case it exceeds 5GB and for perf enhancements over s3n and native s3.
- `spark.hadoop.fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` : Since the S3 bucket
is public, we must set this to be able to access it anonymously in our scripts. 

We use PySpark's Parquet reader to create a DataFrame and interact with it via Spark SQL using a temporary view `T`. 
This gives us the flexibility of defining our queries in SQL while also being able to push-down supported predicates down to
the executors. Spark lazily and parallely loads our Parquet data directly from S3.

### Why not use `jupyter/pyspark-notebook` directly as a base image and inherit from that?

Initially, I wanted my project to inherit from the docker-stacks notebook i.e. `FROM jupyter/pyspark-notebook` and build on
top of that by adding any other required dependencies and steps. While doing this, I ran into a couple of blockers.

The S3 bucket is public and hence no access and secret keys should be needed. However, to access a public bucket programmatically,
you must use `AnonymousAWSCredentialsProvider`. The `jupyter/pyspark-notebook` image comes with Spark 3.1.2 with a backed in dependency
on Hadoop 3.2 (see [this](https://github.com/jupyter/docker-stacks/blob/a6d0ed456ef8dc155173293cf6d38f0acd404601/pyspark-notebook/Dockerfile#L33)).

Even after adding the corresponding aws-java-sdk version i.e. 1.11.375 as per (this [comptibility matrix](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.2.0)), there were various classpath issues (such as `java.lang.NoClassDefFoundError: com/amazonaws/AmazonClientException`).
Trying other versions of `aws-java-sdk` led to other documented incompatibilities such as a lack of support for `AnonymousAWSCredentialsProvider` 
(which we need to programatically access a public S3 bucket) and/or incompatiblility issues with using S3A, etc. See
this [StackOverflow thread](https://stackoverflow.com/questions/31496152/access-public-available-amazon-s3-file-from-apache-spark) and
[HADOOP-13237](https://issues.apache.org/jira/browse/HADOOP-13237) for more details.

I also wanted to be able to read from S3 rather than manually downloading the file locally to read from that, since that
is more similar to how things would be in a real-life environment (though we wouldn't have public buckets :))

The best way forward at this point was to downgrade the Hadoop version. This however was not possible unless I did not inherit from the
`jupyter/pyspark-notebook` image. Hence, I proceeded with using Spark 3.1.2 without the bundled hadoop jars and instead just 
use separate native hadoop jars along with the same version of `hadoop-aws` and corresponding `aws-java-sdk`. 
This solved all incompatibility issues.

**Note: The assumption here is that we are okay with using Hadoop 2.8.4 libraries**


## Brainstorming steps to scale and take this to production:
This project is intended to be run in a local single-machine setting to play with toy datasets. However, if we were to 
take something like this to production and plan to handle 100-1000x the scale, here are some points we should think about:

### Partitioning, schema, query patterns, etc.
- The current data has 9 partitions i.e. `df = SPARK_SESSION.read.parquet(S3_URL).rdd.getNumPartitions()` --> Returns 9,
so we already have some partitioning. Experiment with increasing the number of partitions. 
- To support a service that regularly queries this data, we would benefit from partitioning the data while writing it to S3 via common query patterns.
This can be done via Hive/Iceberg and would greatly improve our write performance (parallel writes to partitions) and query performance
(use partition stats/path to avoid reading partitions).
- Using a metastore like HMS and table spec like Iceberg for storing partition information is also beneficial in that Spark 
doesn't need to reach out the underlying object store i.e. S3 to list partitions which is pretty slow (see [this](https://stackoverflow.com/questions/66535286/how-to-efficiently-filter-a-dataframe-from-an-s3-bucket)).
- Partitioning would not only depend on query patterns, but also the file sizes generated by workers. Perf experiments would have to be run to see the 
best partitioning scheme for our data.
- We should carry out experiments on caching/persisting certain dataFrames based on query patterns and memory/GC pressure.
Sometimes recomputation may be faster than the price paid by the increased memory pressure, so this needs extensive perf testing.

### Infrastructure improvements
- We could horizontally scale by adding more Spark executors and running this on EC2 with auto-scaling. 
- We could also vertically scale by getting beefier EC2 instances, for example getting workers with higher memory which can be
beneficial for caching dataframes/joins/less spilling to disk, etc.

### REST service to manage client interactions
- Clients could interact with our application via a REST service that is decoupled from the data layer.
- Think about scaling the REST servers horizontally by adding more servers behind a load balancer. 
- Add some logic for fairly scheduling incoming client requests in say a queue (backed by something simple like a MySQL db or something). We could write some service which polls this queue and fires off Spark jobs.
- We could have separate side-car containers to handle common tasks like certificate generation, metrics and logs emissions, etc.
- Using prepared queries and/or a caching layer for queries before issuing them to the backing datastore could also be beneficial.
Here we may trade-off reading stale data for improved performance.
 
### Security
- Add security to the S3 bucket to allow only authorized access via access key and secret key along with cert-based AuthN.
- Add encryption at rest and in transit (can use CMKs via KMS). Think about using MTLS connections.

### CI/CD and orchestration
- Add some CI/CD process to the project to automatically test and deploy (think of using helm for configs, k8s deployment scripts, deployment orchestrated via spinnaker using something like terraform, etc.)
- Add metrics and monitoring for parts of the ETL pipeline
- Orchestrate ETL pipeline using something like Airflow. Airflow can also be used to validate the data via Great Expectations before
even triggering the next step.

### Parameter tuning
- Tune important Spark configs: executor cores, executor/driver memory, number of executors, etc. based on experiments. 
- Tune important JVM configs: GC tuning, heap size, stack space, etc. based on experiments.

### Misc
- If we have many small Parquet files, think of a compaction process which can potentially reduce the number of files by combining them, since this may improve query performance.
- To reduce data storage costs, think of using a better compression algorithm like Z-Standard

## Resources:
- [PySpark Cheatsheet](https://www.datacamp.com/community/blog/pyspark-sql-cheat-sheet)
- [Great Expectations Basics](https://greatexpectations.io/)
- [Great Expecations Pyspark](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/dataset/sparkdf_dataset.py)
- [Great Expecations glossary of expectations](https://legacy.docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html)
- [StackOverflow thread to anonymously access S3 bucket](https://stackoverflow.com/questions/31496152/access-public-available-amazon-s3-file-from-apache-spark)
- [HADOOP-13237](https://issues.apache.org/jira/browse/HADOOP-13237)
- [towardsdatascience.com Spark config settings](https://towardsdatascience.com/basics-of-apache-spark-configuration-settings-ca4faff40d45)
- [towardsdatascience.com Spark perf boosting](https://towardsdatascience.com/apache-spark-performance-boosting-e072a3ec1179)
- [towardsdatascience.com best practices for caching in Spark sql](https://towardsdatascience.com/best-practices-for-caching-in-spark-sql-b22fb0f02d34)