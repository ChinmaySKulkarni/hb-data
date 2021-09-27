# This is a modified version of https://github.com/jupyter/docker-stacks/blob/master/pyspark-notebook/Dockerfile
# Please see the README for information on why modifications were required
ARG OWNER=jupyter
ARG BASE_CONTAINER=$OWNER/scipy-notebook
FROM $BASE_CONTAINER

LABEL maintainer="chinmayskulkarni@gmail.com"

# Fix DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

# Spark dependencies
# Default values can be overridden at build time
# (ARGS are in lower case to distinguish them from ENV)
ARG spark_version="3.1.2"
ARG hadoop_version="2.8.4"
ARG spark_checksum="E7B580BC67356F1B36756B18A95E452307EB3825265616E399B1766124764D6E65AB4C666AD23C93E006D88B6C83A78A7D786BE981B4F20696697860A459879D"
ARG openjdk_version="11"

ENV APACHE_SPARK_VERSION="${spark_version}" \
    HADOOP_VERSION="${hadoop_version}"

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Spark installation
# Note we install Spark without Hadoop (see README for more details)
WORKDIR /tmp
RUN wget -q "https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz" && \
    echo "${spark_checksum} *spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz" | sha512sum -c - && \
    tar xzf "spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz" -C /usr/local --owner root --group root --no-same-owner && \
    rm "spark-${APACHE_SPARK_VERSION}-bin-without-hadoop.tgz"

# Separate Hadoop 2.8.4 install required for setting the SPARK_DIST_CLASSPATH
RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar xzf "hadoop-${HADOOP_VERSION}.tar.gz" -C /usr/local --owner root --group root --no-same-owner && \
    rm "hadoop-${HADOOP_VERSION}.tar.gz"

ENV HADOOP_HOME="/usr/local/hadoop-${HADOOP_VERSION}"
ENV SPARK_DIST_CLASSPATH="/usr/local/hadoop-${HADOOP_VERSION}/etc/hadoop:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/common/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs/lib/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/yarn/lib/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/yarn/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/mapreduce/lib/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/mapreduce/*:/usr/local/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/*"

WORKDIR /usr/local

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms4096M --driver-java-options=-Xmx8192M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH="${PATH}:${SPARK_HOME}/bin"

RUN ln -s "spark-${APACHE_SPARK_VERSION}-bin-without-hadoop" spark && \
    # Add a link in the before_notebook hook in order to source automatically PYTHONPATH
    mkdir -p /usr/local/bin/before-notebook.d && \
    ln -s "${SPARK_HOME}/sbin/spark-config.sh" /usr/local/bin/before-notebook.d/spark-config.sh

# Fix Spark installation for Java 11 and Apache Arrow library
# see: https://github.com/apache/spark/pull/27356, https://spark.apache.org/docs/latest/#downloading
RUN cp -p "${SPARK_HOME}/conf/spark-defaults.conf.template" "${SPARK_HOME}/conf/spark-defaults.conf" && \
    echo 'spark.driver.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> "${SPARK_HOME}/conf/spark-defaults.conf" && \
    echo 'spark.executor.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> "${SPARK_HOME}/conf/spark-defaults.conf"

USER ${NB_UID}
WORKDIR "${HOME}"

# Additional steps on top of modified pyspark-notebook docker image
USER root
# Expose for access to Jupyter Notebooks
EXPOSE 8888:8888
RUN chmod -R 777 /home/jovyan/
# Make sure required Hadoop and S3 SDK libraries are present on the classpath
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.8.4/hadoop-aws-2.8.4.jar -P $SPARK_HOME/jars/
# Find the exact compatible aws-java-sdk version for this hadoop-aws version https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.8.4
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.10.6/aws-java-sdk-1.10.6.jar -P $SPARK_HOME/jars/
RUN wget https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar -P $SPARK_HOME/jars/

RUN export PYSPARK_DRIVER_PYTHON='jupyter'
RUN export PYSPARK_DRIVER_PYTHON_OPTS='notebook --NotebookApp.iopub_data_rate_limit=1.0e10'

COPY ./app-processor $HOME/
COPY ./requirements.txt $HOME/
ADD ./app-processor/conf $HOME/conf
# install requirements and convert the main py file to ipynb
RUN pip install -r requirements.txt && \
    jupytext --to notebook pyspark-hb-app-processor.py && \
    rm *.py requirements.txt
