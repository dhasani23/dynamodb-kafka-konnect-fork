import org.apache.kafka.common.config.ConfigDef

class DynamoDbSourceTaskConfig(props: Map<String, String>) : DynamoDbSourceConnectorConfig(null, props) {
    companion object {
        val TableNameConfig = "table"
        private val TableNameDoc = "table monitored by this task for changes."

        val TaskIdConfig = "task.id"
        private val TaskIdDoc = "Id of task within the set of tasks per table."
    }

    override var config = baseConfigDef()
    .define(TableNameConfig, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TableNameDoc)
    .define(TaskIdConfig, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TaskIdDoc)

    init {
        super.config = config
    }

    fun getTableName(): String {
        return getString(TableNameConfig)
    }

    fun getTaskId(): String {
        return getString(TaskIdConfig)
    }
}