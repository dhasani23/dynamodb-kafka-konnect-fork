import aws.AwsClients
import aws.ConfigTablesProvider
import aws.DynamoDbTablesProvider
import aws.TablesProvider
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectorContext
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import java.util.*

class DynamoDbSourceConnector(private var tablesProvider: TablesProvider?) : SourceConnector() {
    private lateinit var configProperties: Map<String, String>
    private lateinit var consumableTables: List<String>

    @Volatile
    private lateinit var timer: Timer

    lateinit var connectorConfig: DynamoDbSourceConnectorConfig;

    override fun taskClass(): Class<out Task?>? {
        return DynamoDbSourceTask::class.java
    }

    override fun start(properties: Map<String, String>) {
        configProperties = properties
        connectorConfig = DynamoDbSourceConnectorConfig(
            ConfigDef(),
            configProperties
        )

        val groupsTaggingApiClient = AwsClients.buildAwsResourceGroupsTaggingApiClient(
            connectorConfig.getAwsRegion(),
            connectorConfig.getResourceTaggingServiceEndpoint(),
            connectorConfig.getAwsAccessKeyIdValue(),
            connectorConfig.getAwsSecretKeyValue(),
        )

        val dynamoDbClient = AwsClients.buildDynamoDbClient(
            connectorConfig.getAwsRegion(),
            connectorConfig.getResourceTaggingServiceEndpoint(),
            connectorConfig.getAwsAccessKeyIdValue(),
            connectorConfig.getAwsSecretKeyValue(),
        )

        if (tablesProvider == null) {
            tablesProvider = if (connectorConfig.getWhitelistTables() != null) {
                ConfigTablesProvider(dynamoDbClient, connectorConfig)
            } else {
                DynamoDbTablesProvider(
                    groupsTaggingApiClient,
                    dynamoDbClient,
                    connectorConfig.getSrcDynamoDBIngestionTagKey(),
                    connectorConfig.getSrcDynamoDBEnvTagKey(),
                    connectorConfig.getSrcDynamoDBEnvTagValue()
                )
            }
        }


    }

    private fun startBackgroundReconfigurationTasks(
        connectorCtx: ConnectorContext,
        rediscoveryPeriod: Long,
    ) {
        timer = Timer(true)

        val configurationChangeDetectorTask = object : TimerTask() {
            override fun run() {
                val consumableTablesRefreshed = try {
                    tablesProvider?.getConsumableTables()
                } catch (e: Throwable) { listOf() }
                if (consumableTables != consumableTablesRefreshed) {
                    connectorCtx.requestTaskReconfiguration()
                }
            }
        }

        timer.scheduleAtFixedRate(configurationChangeDetectorTask, rediscoveryPeriod, rediscoveryPeriod)
    }

    override fun stop() {
        timer.cancel()
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        consumableTables = try {
            tablesProvider?.getConsumableTables() ?: listOf()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            consumableTables
        }

        if (consumableTables.size > maxTasks) {
            return listOf()
        }

        val taskCfgs = ArrayList<Map<String, String>>(consumableTables.size)
        for (table in consumableTables) {
            val taskProps = HashMap<String, String>(configProperties)
            taskProps.put(DynamoDbSourceTaskConfig.TableNameConfig, table)
            taskProps.put(DynamoDbSourceTaskConfig.TaskIdConfig, "task-1")
            taskCfgs.add(taskProps)
        }

        return taskCfgs
    }

    override fun config() = connectorConfig.config

    override fun version(): String {
        return "0.1"
    }
}