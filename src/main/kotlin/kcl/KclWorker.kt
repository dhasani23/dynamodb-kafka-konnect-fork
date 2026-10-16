package kcl

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
import com.amazonaws.services.dynamodbv2.model.BillingMode

interface KclWorker {
    fun start(
        dynamoDbClient: AmazonDynamoDB,
        dynamoDbStreamsClient: AmazonDynamoDBStreams,
        tableName: String,
        taskId: String,
        endpoint: String,
        kclTableBillingMode: BillingMode
    ): Void?;

    fun stop(): Void?;
}