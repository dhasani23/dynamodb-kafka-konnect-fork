plugins {
    kotlin("jvm") version "1.9.0"
    java
}

group = "io.jhegarty14"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("org.apache.kafka:connect-api:3.6.1")
    // AWS SDK v2 replacements
    implementation("software.amazon.awssdk:dynamodb:2.21.41")
    implementation("software.amazon.awssdk:dynamodb-enhanced:2.21.41")
    implementation("software.amazon.awssdk:sts:2.21.41")
    implementation("software.amazon.awssdk:resourcegroupstaggingapi:2.21.41")
    // AWS KCL v2
    implementation("software.amazon.kinesis:amazon-kinesis-client:2.5.3")
    // HTTP client is required for AWS SDK v2
    implementation("software.amazon.awssdk:apache-client:2.21.41")
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
