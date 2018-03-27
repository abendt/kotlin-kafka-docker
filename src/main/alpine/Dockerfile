# Kafka and Zookeeper

FROM anapsix/alpine-java:8_server-jre

ARG KAFKA_VERSION

ENV ZK_VERSION 3.4.11
ENV SCALA_VERSION 2.12
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

RUN apk upgrade && \
    apk --no-cache add wget supervisor && \
    wget -q http://apache.mirror.digionline.de/zookeeper/zookeeper-"$ZK_VERSION"/zookeeper-"$ZK_VERSION".tar.gz -O /tmp/zookeeper-"$ZK_VERSION".tgz && \
    tar xzf /tmp/zookeeper-"$ZK_VERSION".tgz -C /opt && \
    rm /tmp/zookeeper-"$ZK_VERSION".tgz && \
    ln -s /opt/zookeeper-"$ZK_VERSION" /opt/zookeeper && \
    wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
    ln -s /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION" /opt/kafka && \
    rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    apk del wget

ADD scripts/start-kafka.sh scripts/kafka-health.sh /usr/bin/

ADD conf/zoo.cfg /opt/zookeeper/conf/zoo.cfg

# Supervisor config
ADD supervisor/kafka.ini supervisor/zookeeper.ini /etc/supervisor.d/

# 2181 is zookeeper, 9092 is kafka
EXPOSE 2181 9092

HEALTHCHECK CMD /usr/bin/kafka-health.sh

CMD ["supervisord", "-n", "-c", "/etc/supervisord.conf"]