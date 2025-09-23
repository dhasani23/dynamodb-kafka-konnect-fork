package kcl

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient
import software.amazon.awssdk.services.dynamodb.model.BillingMode

interface KclWorker {
    fun start(
        dynamoDbClient: DynamoDbClient,
        dynamoDbStreamsClient: DynamoDbStreamsClient,
        tableName: String,
        taskId: String,
        endpoint: String,
        kclTableBillingMode: BillingMode
    ): Void

    fun stop(): Void
}