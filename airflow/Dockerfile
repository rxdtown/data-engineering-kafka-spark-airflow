FROM apache/airflow:2.9.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        procps \
        openjdk-17-jre-headless \
    && ln -sf $(readlink -f /usr/bin/java | sed "s:bin/java::") /usr/lib/jvm/default-java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0