package aws

import software.amazon.awssdk.services.dynamodb.model.StreamViewType
import software.amazon.awssdk.services.dynamodb.model.TableDescription

abstract class TablesProviderBase : TablesProvider {
    protected fun hasValidConfig(tableDesc: TableDescription, tableName: String): Boolean {
        val streamSpec = tableDesc.streamSpecification()
        if (streamSpec == null || !streamSpec.streamEnabled()) {
            return false
        }

        val streamViewType = streamSpec.streamViewType()
        return streamViewType == StreamViewType.NEW_IMAGE || 
               streamViewType == StreamViewType.NEW_AND_OLD_IMAGES
    }
}