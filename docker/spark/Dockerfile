FROM ubuntu:18.04

LABEL authors="penghuo@gmail.com"

RUN apt update
RUN apt install default-jdk -y \
&& apt install wget -y \
&& apt install supervisor -y

ENV SPARK_VERSION 3.2.3
ENV HADOOP_VERSION 3.2

# download and extract Spark 
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
&&  tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
&&  mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark

# add configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j.properties /opt/spark/conf/log4j.properties
COPY jars/*.* /opt/spark/jars
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master" \
SPARK_HOME=/opt/spark

EXPOSE 4040 8080 7077 6066 10000

# CMD ["/usr/bin/supervisord"]
RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]
