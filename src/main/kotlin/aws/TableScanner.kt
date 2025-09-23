package aws

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

interface TableScanner {
    fun getItems(exclusiveStartKey: Map<String, AttributeValue>): ScanResponse
}