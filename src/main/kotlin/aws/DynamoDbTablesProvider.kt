package aws

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesRequest
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.TagFilter
import java.util.*

class DynamoDbTablesProvider(
    private val groupsTaggingApi: ResourceGroupsTaggingApiClient,
    private val client: DynamoDbClient,
    private val ingestionTagKey: String,
    private val envTagKey: String,
    private val envTagValue: String,
) : TablesProviderBase() {
    override fun getConsumableTables(): List<String> {
        val consumableTables = LinkedList<String>()
        var resourcesRequest = buildResourceRequest()
        var paginationToken: String? = null

        do {
            // If there's a pagination token from previous request, use it
            if (paginationToken != null) {
                resourcesRequest = resourcesRequest.toBuilder()
                    .paginationToken(paginationToken)
                    .build()
            }

            val result = groupsTaggingApi.getResources(resourcesRequest)

            for (resource in result.resourceTagMappingList()) {
                val tableArn = resource.resourceARN()
                val tableName = tableArn.substring(tableArn.lastIndexOf('/') + 1)

                val tableDesc = try {
                    client.describeTable { it.tableName(tableName) }.table()
                } catch (_: Throwable) { continue }
                
                if (hasValidConfig(tableDesc, tableName)) {
                    consumableTables.add(tableName)
                }
            }

            paginationToken = result.paginationToken()
        } while (paginationToken != null && paginationToken.isNotEmpty())

        return consumableTables
    }

    private fun buildResourceRequest(): GetResourcesRequest {
        val stackTagFilter = TagFilter.builder()
            .key(envTagKey)
            .values(envTagValue)
            .build()

        val ingestionTagFilter = TagFilter.builder()
            .key(ingestionTagKey)
            .build()

        return GetResourcesRequest.builder()
            .resourceTypeFilters("dynamodb")
            .resourcesPerPage(50)
            .tagFilters(stackTagFilter, ingestionTagFilter)
            .build()
    }
}