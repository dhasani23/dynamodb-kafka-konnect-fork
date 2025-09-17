package aws

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesRequest
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.TagFilter
import java.util.*

class DynamoDbTablesProviderV2(
    private val groupsTaggingApi: ResourceGroupsTaggingApiClient,
    private val client: DynamoDbClient,
    private val ingestionTagKey: String,
    private val envTagKey: String,
    private val envTagValue: String,
) : TablesProviderBase() {
    override fun getConsumableTables(): List<String> {
        val consumableTables = LinkedList<String>()
        var paginationToken: String? = null
        
        do {
            val request = buildResourceRequest(paginationToken)
            val result = groupsTaggingApi.getResources(request)

            for (resource in result.resourceTagMappingList()) {
                val tableArn = resource.resourceARN()
                val tableName = tableArn.substring(tableArn.lastIndexOf('/') + 1)

                val tableDesc = try {
                    client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).table()
                } catch (_: Throwable) { continue }
                
                if (hasValidConfig(tableDesc, tableName)) {
                    consumableTables.add(tableName)
                }
            }
            
            paginationToken = result.paginationToken()
        } while (paginationToken != null && paginationToken.isNotEmpty())

        return consumableTables
    }

    private fun buildResourceRequest(paginationToken: String?): GetResourcesRequest {
        val stackTagFilter = TagFilter.builder()
            .key(envTagKey)
            .values(envTagValue)
            .build()

        val ingestionTagFilter = TagFilter.builder()
            .key(ingestionTagKey)
            .build()

        val requestBuilder = GetResourcesRequest.builder()
            .resourceTypeFilters("dynamodb")
            .tagFilters(stackTagFilter, ingestionTagFilter)
            .resourcesPerPage(50)
            
        if (paginationToken != null) {
            requestBuilder.paginationToken(paginationToken)
        }

        return requestBuilder.build()
    }
}