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
    
    // AWS SDK v2 dependencies
    implementation("software.amazon.awssdk:dynamodb:2.20.160")
    implementation("software.amazon.awssdk:dynamodb-enhanced:2.20.160")
    implementation("software.amazon.awssdk:sts:2.20.160")
    implementation("software.amazon.awssdk:resourcegroupstaggingapi:2.20.160")
    implementation("software.amazon.awssdk:auth:2.20.160")
    implementation("software.amazon.awssdk:apache-client:2.20.160")
    
    // We'll replace these in later steps
    implementation("com.amazonaws:amazon-kinesis-client:1.15.1")
    implementation("com.amazonaws:dynamodb-streams-kinesis-adapter:1.6.0")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.12.669") // Keep v1 SDK until full migration
    implementation("com.amazonaws:aws-java-sdk-resourcegroupstaggingapi:1.12.669") // Keep v1 SDK until full migration
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