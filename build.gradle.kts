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
    val kafkaVersion = "1.0.1"

    compile(kotlin("stdlib-jdk8"))

    compile("org.apache.kafka:kafka-streams:$kafkaVersion")
    compile("org.apache.kafka:kafka_2.12:$kafkaVersion")
    compile("io.github.microutils:kotlin-logging:1.5.3")

    runtime("ch.qos.logback:logback-classic:1.2.3")
    runtime("org.slf4j:log4j-over-slf4j:1.7.25")

    testCompile("junit:junit:4.12")
    testCompile("org.assertj:assertj-core:3.9.1")
    testCompile("com.101tec:zkclient:0.10")
    testCompile("org.apache.kafka:kafka-clients:$kafkaVersion")
    testCompile("com.natpryce:hamkrest:1.4.2.2")

    testRuntime(kotlin("reflect"))
}

configurations {
    get("compile").exclude(module = "slf4j-log4j12").exclude(module = "log4j")
    get("testCompile").exclude(module = "slf4j-log4j12").exclude(module = "log4j")
}

tasks {

    val testContainerName = "kotlin-kafka-docker"

    createTask("dockerInfo", DockerInfo::class) {}

    createTask("dockerVersion", DockerVersion::class) {}

    createTask("dockerBuild", DockerBuildImage::class) {
        inputDir = projectDir.resolve("src/main/alpine")
        tag = "abendt/kafka-alpine"
    }

    createTask("dockerRemove", Exec::class) {
        group = "docker"
        executable = "docker"
        args = listOf("rm", "-f", testContainerName)
        isIgnoreExitValue = true
    }

    val dockerCreate = createTask("dockerCreate", DockerCreateContainer::class) {
        dependsOn("dockerBuild", "dockerRemove")
        targetImageId { "abendt/kafka-alpine" }
        portBindings = listOf("2181:2181", "9092:9092")
        setEnv("ADVERTISED_HOST=127.0.0.1", "ADVERTISED_PORT=9092")
        containerName = testContainerName
    }

    createTask("dockerStart", DockerStartContainer::class) {
        dependsOn("dockerCreate")

        targetContainerId { dockerCreate.containerId }
    }

    createTask("dockerStop", DockerStopContainer::class) {
        targetContainerId { dockerCreate.containerId }
    }

    createTask("dockerWaitHealthy", DockerWaitHealthyContainer::class) {
        targetContainerId { dockerCreate.containerId }
    }

    "test"(Test::class) {
        include("**/*Test.class")
    }

    createTask("it", Test::class) {
        dependsOn("test", "dockerStart", "dockerWaitHealthy")
        finalizedBy("dockerStop")

        include("**/*IT.class")
    }
}

tasks.withType(Test::class.java) {
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        showStandardStreams = true
    }
}
