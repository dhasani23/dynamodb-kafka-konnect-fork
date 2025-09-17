package aws

import com.amazonaws.services.dynamodbv2.model.StreamViewType
import com.amazonaws.services.dynamodbv2.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableDescription as TableDescriptionV2
import software.amazon.awssdk.services.dynamodb.model.StreamViewType as StreamViewTypeV2

abstract class TablesProviderBase : TablesProvider {
    protected fun hasValidConfig(tableDesc: TableDescription, tableName: String): Boolean {
        val streamSpec = tableDesc.streamSpecification;
        if (!streamSpec.isStreamEnabled) {
            return false;
        }

        val streamViewType = streamSpec.streamViewType;
        return !(!streamViewType.equals(StreamViewType.NEW_IMAGE.name)
                && !streamViewType.equals(StreamViewType.NEW_AND_OLD_IMAGES.name));
    }
    
    protected fun hasValidConfig(tableDesc: TableDescriptionV2, tableName: String): Boolean {
        val streamSpec = tableDesc.streamSpecification();
        if (streamSpec == null || !streamSpec.streamEnabled()) {
            return false;
        }

        val streamViewType = streamSpec.streamViewType();
        return !(!streamViewType.equals(StreamViewTypeV2.NEW_IMAGE)
                && !streamViewType.equals(StreamViewTypeV2.NEW_AND_OLD_IMAGES));
    }
}