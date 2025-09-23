package aws

import com.google.common.util.concurrent.RateLimiter
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

class DynamoDbTableScanner
    (private val client: DynamoDbClient, private val tableName: String, readCapacityUnits: Long) : TableScanner {
    private val rateLimiter: RateLimiter?
    private var permitsToConsume = 1

    init {
        if (readCapacityUnits > 0L) {
            // TODO
            this.rateLimiter = RateLimiter.create((readCapacityUnits / 2).toDouble())
        } else {
            this.rateLimiter = null
        }
    }

    override fun getItems(exclusiveStartKey: Map<String, AttributeValue>): ScanResponse {
        rateLimiter?.acquire(permitsToConsume)

        val scanRequest = ScanRequest.builder()
            .tableName(tableName)
            .limit(1000)
            .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .exclusiveStartKey(exclusiveStartKey)
            .build()

        val result = client.scan(scanRequest)

        if (rateLimiter != null) {
            val consumedCapacity = result.consumedCapacity().capacityUnits()
            permitsToConsume = (consumedCapacity - 1.0).toInt()
            permitsToConsume = if (permitsToConsume <= 0) 1 else permitsToConsume
        }

        return result
    }
}