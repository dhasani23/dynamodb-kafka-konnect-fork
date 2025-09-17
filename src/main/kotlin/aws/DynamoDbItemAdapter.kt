package aws

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import java.util.function.Consumer

/**
 * Generic adapter for working with DynamoDB items using the Enhanced Client.
 * This class provides common operations for DynamoDB tables.
 *
 * @param T the type of items stored in the DynamoDB table
 * @property enhancedClient The enhanced DynamoDB client
 * @property tableSchema The table schema for the item type
 * @property tableName The name of the DynamoDB table
 */
class DynamoDbItemAdapter<T : Any>(
    private val enhancedClient: DynamoDbEnhancedClient,
    private val tableSchema: TableSchema<T>,
    private val tableName: String
) {
    // Get the DynamoDbTable<T> from the enhanced client
    private val table: DynamoDbTable<T> = enhancedClient.table(tableName, tableSchema)

    /**
     * Retrieves an item by its partition key.
     *
     * @param partitionKey The partition key of the item
     * @return The retrieved item or null if not found
     */
    fun getItem(partitionKey: String): T? {
        val key = Key.builder().partitionValue(partitionKey).build()
        return table.getItem { b -> b.key(key) }
    }

    /**
     * Retrieves an item by its partition and sort keys.
     *
     * @param partitionKey The partition key of the item
     * @param sortKey The sort key of the item
     * @return The retrieved item or null if not found
     */
    fun getItem(partitionKey: String, sortKey: String): T? {
        val key = Key.builder()
            .partitionValue(partitionKey)
            .sortValue(sortKey)
            .build()
        return table.getItem { b -> b.key(key) }
    }

    /**
     * Puts an item into the DynamoDB table.
     *
     * @param item The item to put
     */
    fun putItem(item: T) {
        table.putItem(item)
    }

    /**
     * Puts an item with additional options.
     *
     * @param item The item to put
     * @param configurator A lambda to configure the PutItemEnhancedRequest
     */
    fun putItem(item: T, configurator: Consumer<PutItemEnhancedRequest.Builder<T>>) {
        table.putItem { b -> 
            b.item(item)
            configurator.accept(b)
        }
    }

    /**
     * Queries items using a partition key and additional options.
     *
     * @param partitionKey The partition key to query for
     * @param configurator A lambda to configure the QueryEnhancedRequest
     * @return A list of items matching the query
     */
    fun queryItems(partitionKey: String, configurator: Consumer<QueryEnhancedRequest.Builder> = Consumer {}): List<T> {
        // Use QueryConditional to create the key condition
        val response = table.query { builder ->
            // Create a key condition for equality with the partition key
            val keyCondition = QueryConditional.keyEqualTo(Key.builder()
                .partitionValue(partitionKey)
                .build())
                
            builder.queryConditional(keyCondition)
            configurator.accept(builder)
        }
        
        return response.items().toList()
    }
    
    /**
     * Deletes an item by its partition key.
     *
     * @param partitionKey The partition key of the item to delete
     */
    fun deleteItem(partitionKey: String) {
        val key = Key.builder().partitionValue(partitionKey).build()
        table.deleteItem(key)
    }
    
    /**
     * Deletes an item by its partition and sort keys.
     *
     * @param partitionKey The partition key of the item to delete
     * @param sortKey The sort key of the item to delete
     */
    fun deleteItem(partitionKey: String, sortKey: String) {
        val key = Key.builder()
            .partitionValue(partitionKey)
            .sortValue(sortKey)
            .build()
        table.deleteItem(key)
    }
    
    // Batch operations are intentionally omitted for now due to API compatibility issues
}