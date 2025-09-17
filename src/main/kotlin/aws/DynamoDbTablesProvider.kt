package aws

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI
import com.amazonaws.services.resourcegroupstaggingapi.model.GetResourcesRequest
import com.amazonaws.services.resourcegroupstaggingapi.model.TagFilter
import java.util.*

class DynamoDbTablesProvider(
    private val groupsTaggingApi: AWSResourceGroupsTaggingAPI,
    private val client: AmazonDynamoDB,
    private val ingestionTagKey: String,
    private val envTagKey: String,
    private val envTagValue: String,
) : TablesProviderBase() {
    override fun getConsumableTables(): List<String> {
        val consumableTables = LinkedList<String>();
        val resourcesRequest = buildResourceRequest();

        while (true) {
            val result = groupsTaggingApi.getResources(resourcesRequest);

            for (resource in result.resourceTagMappingList) {
                val tableArn = resource.resourceARN;
                val tableName = tableArn.substring(tableArn.lastIndexOf('/') + 1);

                val tableDesc = try {
                    client.describeTable(tableName).table
                } catch (_: Throwable) { continue; };
                
                // Use the v1 version of hasValidConfig since we're working with v1 TableDescription
                if (hasValidConfig(tableDesc, tableName)) {
                    consumableTables.add(tableName);
                }
            }

            if (result.paginationToken.isNullOrEmpty()) break;

            resourcesRequest.withPaginationToken(result.paginationToken);
        }

        return consumableTables;
    }

    private fun buildResourceRequest(): GetResourcesRequest {
        val stackTagFilter = TagFilter();
        stackTagFilter.withKey(envTagKey);
        stackTagFilter.setValues(Collections.singletonList(envTagValue));

        val ingestionTagFilter = TagFilter();
        ingestionTagFilter.withKey(ingestionTagKey);

        val tagFilters = LinkedList<TagFilter>();
        tagFilters.add(stackTagFilter);
        tagFilters.add(ingestionTagFilter);

        return GetResourcesRequest()
            .withResourceTypeFilters("dynamodb")
            .withResourcesPerPage(50)
            .withTagFilters(tagFilters);
    }

}