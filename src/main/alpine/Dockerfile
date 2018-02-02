# Kafka and Zookeeper

FROM anapsix/alpine-java:8_server-jre

ENV ZK_VERSION 3.4.10
ENV SCALA_VERSION 2.12
ENV KAFKA_VERSION 1.0.0
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

RUN apk -U upgrade && \
    apk --update add wget supervisor && \
    wget -q http://apache.mirror.digionline.de/zookeeper/zookeeper-"$ZK_VERSION"/zookeeper-"$ZK_VERSION".tar.gz -O /tmp/zookeeper-"$ZK_VERSION".tgz && \
    tar xzf /tmp/zookeeper-"$ZK_VERSION".tgz -C /opt && \
    rm /tmp/zookeeper-"$ZK_VERSION".tgz && \
    ln -s /opt/zookeeper-3.4.10 /opt/zookeeper && \
    wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
    rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    apk del wget && \
    rm -rf /var/cache/apk/*

ADD scripts/start-kafka.sh /usr/bin/start-kafka.sh

ADD conf/zoo.cfg /opt/zookeeper/conf/zoo.cfg

# Supervisor config
ADD supervisor/kafka.ini supervisor/zookeeper.ini /etc/supervisor.d/

# 2181 is zookeeper, 9092 is kafka
EXPOSE 2181 9092

CMD ["supervisord", "-n", "-c", "/etc/supervisord.conf"]