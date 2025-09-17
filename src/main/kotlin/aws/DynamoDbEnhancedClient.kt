package aws

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex

/**
 * Factory class for creating DynamoDB Enhanced Client and related resources.
 */
object DynamoDbEnhancedClient {

    /**
     * Creates a DynamoDbEnhancedClient from a DynamoDbClient.
     * 
     * @param dynamoDbClient The DynamoDbClient to use.
     * @return A DynamoDbEnhancedClient.
     */
    @JvmStatic
    fun create(dynamoDbClient: DynamoDbClient): DynamoDbEnhancedClient {
        return DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dynamoDbClient)
            .build()
    }

    /**
     * Creates a TableSchema for the given document class.
     * This is a simplified version that assumes the class has a primary key and other attributes
     * that can be introspected by the Enhanced Client.
     * 
     * @param documentClass The class to create a schema for.
     * @return A TableSchema for the document class.
     */
    @JvmStatic
    inline fun <reified T> createTableSchema(): TableSchema<T> {
        return TableSchema.fromBean(T::class.java)
    }
}