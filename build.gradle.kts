import com.bmuschko.gradle.docker.tasks.DockerInfo
import com.bmuschko.gradle.docker.tasks.DockerVersion
import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerLogsContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.container.extras.DockerWaitHealthyContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import java.io.IOException

buildscript {
    repositories {
        jcenter()
    }
}

plugins {
    application
    kotlin("jvm") version "1.2.31"
    id("com.bmuschko.docker-remote-api") version "3.2.5"
}

repositories {
    jcenter()
}

application {
    mainClassName = "samples.HelloWorldKt"
}

dependencies {
    val kafkaVersion = "1.1.0"

    compile(kotlin("stdlib-jdk8"))

    compile("org.apache.kafka:kafka-streams:$kafkaVersion")
    compile("org.apache.kafka:kafka_2.12:$kafkaVersion")
    compile("io.github.microutils:kotlin-logging:1.5.4")

    compile("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.5")
    compile("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.9.5")

    runtime("ch.qos.logback:logback-classic:1.2.3")
    runtime("org.slf4j:log4j-over-slf4j:1.7.25")

    testCompile("junit:junit:4.12")
    testCompile("org.assertj:assertj-core:3.9.1")
    testCompile("net.wuerl.kotlin:assertj-core-kotlin:0.2.1")
    testCompile("com.101tec:zkclient:0.10")
    testCompile("org.apache.kafka:kafka-clients:$kafkaVersion")

    testRuntime(kotlin("reflect"))
}

configurations {
    get("compile").exclude(module = "slf4j-log4j12").exclude(module = "log4j")
    get("testCompile").exclude(module = "slf4j-log4j12").exclude(module = "log4j")
}

tasks {

    val testContainerName = "kotlin-kafka-docker"
    val kafkaVersion = "1.1.0"

    val dockerInfo by creating(DockerInfo::class) {}

    val dockerVersion by creating(DockerVersion::class) {}

    val dockerBuild by creating(DockerBuildImage::class) {
        inputDir = projectDir.resolve("src/main/alpine")
        tag = "abendt/kafka-alpine:$kafkaVersion"
        buildArgs.put("KAFKA_VERSION", kafkaVersion)
    }

    val dockerRemove by creating(Exec::class) {
        group = "docker"
        executable = "docker"
        args = listOf("rm", "-f", testContainerName)
        isIgnoreExitValue = true
    }

    val dockerCreate by creating(DockerCreateContainer::class) {
        dependsOn("dockerBuild", "dockerRemove")
        targetImageId { "abendt/kafka-alpine:$kafkaVersion" }
        portBindings = listOf("2181:2181", "9092:9092")
        setEnv("ADVERTISED_HOST=127.0.0.1", "ADVERTISED_PORT=9092")
        containerName = testContainerName
    }

    val dockerStart by creating(DockerStartContainer::class) {
        dependsOn(dockerCreate)

        targetContainerId { dockerCreate.containerId }
    }

    val dockerStop by creating(DockerStopContainer::class) {
        targetContainerId { dockerCreate.containerId }
    }

    val dockerWaitHealthy by creating(DockerWaitHealthyContainer::class) {
        targetContainerId { dockerCreate.containerId }
    }

    val test by getting(Test::class) {
        include("**/*Test.class")
    }

    val integrationTest by creating(Test::class) {
        description = "run Integration tests"

        dependsOn(test, dockerStart, dockerWaitHealthy)
        finalizedBy(dockerStop)
        maxParallelForks = 2

        include("**/*IT.class")
    }
}

tasks.withType(Test::class.java) {
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        showStandardStreams = true
    }
}
