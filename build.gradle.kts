plugins {
    kotlin("jvm") version "1.9.0"
    java
}

group = "io.jhegarty14"

repositories {
    mavenCentral()
}

dependencies {
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core:2.18.11") {
            because("CVE-2026-89425 / CVE-2026-68494: _reportInvalidToken lacks the maxErrorTokenLength limit before 2.18.11")
        }
        implementation("com.fasterxml.jackson.core:jackson-databind:2.18.11") {
            because("keep jackson-databind aligned with the patched jackson-core")
        }
    }

    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:3.9.2")
    implementation("org.apache.kafka:connect-api:3.9.2")
    implementation("com.amazonaws:amazon-kinesis-client:1.15.1")
    implementation("com.amazonaws:dynamodb-streams-kinesis-adapter:1.6.0")
    implementation("com.amazonaws:aws-java-sdk-sts:1.12.669")
    implementation("com.amazonaws:aws-java-sdk-resourcegroupstaggingapi:1.12.669")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.apache.logging.log4j:log4j-api:2.23.0")
    implementation("org.apache.logging.log4j:log4j-core:2.23.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.23.0")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}
