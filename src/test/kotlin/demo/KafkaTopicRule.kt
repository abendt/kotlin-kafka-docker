package demo

import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.utils.ZkUtils
import kafka.utils.`ZKStringSerializer$`
import mu.KLogging
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.junit.rules.ExternalResource
import java.util.*

class KafkaTopicRule : ExternalResource() {

    companion object : KLogging() {
        fun zookeeperConnect() = "127.0.0.1:2181"

        private const val DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000
        private const val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000
    }

    fun createTopic(topic: String) {
        createTopic(topic, 1, 1, Properties())
    }

    fun createTopic(topic: String,
                    partitions: Int,
                    replication: Int,
                    topicConfig: Properties) {
        logger.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig)

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        val zkClient = ZkClient(
                zookeeperConnect(),
                DEFAULT_ZK_SESSION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
                `ZKStringSerializer$`.`MODULE$`)
        val isSecure = false
        val zkUtils = ZkUtils(zkClient, ZkConnection(zookeeperConnect()), isSecure)
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.`Enforced$`.`MODULE$`)
        zkClient.close()
    }

    fun bootstrapServers() = "localhost:9092"

}