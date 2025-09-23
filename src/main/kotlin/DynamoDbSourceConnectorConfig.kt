import software.amazon.awssdk.services.dynamodb.model.BillingMode
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.types.Password


open class DynamoDbSourceConnectorConfig(
    inputConfig: ConfigDef?,
    properties: Map<String, String>,
) : AbstractConfig(inputConfig, properties) {
    val AWS_GROUP = "AWS"
    val CONNECTOR_GROUP = "Connector"

    val SRC_INIT_SYNC_DELAY_CONFIG = "init.sync.delay.period"
    val SRC_INIT_SYNC_DELAY_DOC = "Define how long to delay INIT_SYNC start in seconds."
    val SRC_INIT_SYNC_DELAY_DISPLAY = "INIT_SYNC delay"
    val SRC_INIT_SYNC_DELAY_DEFAULT = 60

    val AWS_REGION_CONFIG = "aws.region"
    val AWS_REGION_DOC = "Define AWS region."
    val AWS_REGION_DISPLAY = "Region"
    val AWS_REGION_DEFAULT = "eu-west-1"

    val AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id"
    val AWS_ACCESS_KEY_ID_DOC =
        "Explicit AWS access key ID. Leave empty to utilize the default credential provider chain."
    val AWS_ACCESS_KEY_ID_DISPLAY = "Access key id"
    val AWS_ACCESS_KEY_ID_DEFAULT: Password? = null

    val AWS_SECRET_KEY_CONFIG = "aws.secret.key"
    val AWS_SECRET_KEY_DOC =
        "Explicit AWS secret access key. Leave empty to utilize the default credential provider chain."
    val AWS_SECRET_KEY_DISPLAY = "Secret key"
    val AWS_SECRET_KEY_DEFAULT: Password? = null

    val SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG = "dynamodb.table.ingestion.tag.key"
    val SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DOC =
        "Define DynamoDB table tag name. Only tables with this tag key will be ingested."
    val SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DISPLAY = "Ingestion tag key name"
    val SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DEFAULT = "datalake-ingest"

    val SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG = "dynamodb.table.env.tag.key"
    val SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DOC =
        "Define DynamoDB tables environment tag name. Only tables with dynamodb.table.env.tag.value value in this key will be ingested."
    val SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DISPLAY = "Environment tag key"
    val SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DEFAULT = "environment"

    val SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG = "dynamodb.table.env.tag.value"
    val SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DOC =
        "Define environment name which must be present in dynamodb.table.env.tag.key."
    val SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DISPLAY = "Environment"
    val SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DEFAULT = "dev"

    val SRC_DYNAMODB_TABLE_WHITELIST_CONFIG = "dynamodb.table.whitelist"
    val SRC_DYNAMODB_TABLE_WHITELIST_DOC =
        "Define whitelist of dynamodb table names. This overrides table auto-discovery by ingestion tag."
    val SRC_DYNAMODB_TABLE_WHITELIST_DISPLAY = "Tables whitelist"
    val SRC_DYNAMODB_TABLE_WHITELIST_DEFAULT: String? = null

    val SRC_KCL_TABLE_BILLING_MODE_CONFIG = "kcl.table.billing.mode"
    val SRC_KCL_TABLE_BILLING_MODE_DOC =
        "Define billing mode for internal table created by the KCL library. Default is provisioned."
    val SRC_KCL_TABLE_BILLING_MODE_DISPLAY = "KCL table billing mode"
    val SRC_KCL_TABLE_BILLING_MODE_DEFAULT = "PROVISIONED"

    val DST_TOPIC_PREFIX_CONFIG = "kafka.topic.prefix"
    val DST_TOPIC_PREFIX_DOC = "Define Kafka topic destination prefix. End will be the name of a table."
    val DST_TOPIC_PREFIX_DISPLAY = "Topic prefix"
    val DST_TOPIC_PREFIX_DEFAULT = "dynamodb-"


    val REDISCOVERY_PERIOD_CONFIG = "connect.dynamodb.rediscovery.period"
    val REDISCOVERY_PERIOD_DOC = "Time period in milliseconds to rediscover stream enabled DynamoDB tables"
    val REDISCOVERY_PERIOD_DISPLAY = "Rediscovery period"
    val REDISCOVERY_PERIOD_DEFAULT = (1 * 60 * 1000 // 1 minute
            ).toLong()

    val AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG = "resource.tagging.service.endpoint"
    val AWS_RESOURCE_TAGGING_API_ENDPOINT_DOC = "AWS Resource Group Tag API Endpoint. Will use default AWS if not set."
    val AWS_RESOURCE_TAGGING_API_ENDPOINT_DISPLAY = "AWS Resource Group Tag API Endpoint"
    val AWS_RESOURCE_TAGGING_API_ENDPOINT_DEFAULT: String? = null

    val AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG = "dynamodb.service.endpoint"
    val AWS_DYNAMODB_SERVICE_ENDPOINT_DOC = "AWS DynamoDB API Endpoint. Will use default AWS if not set."
    val AWS_DYNAMODB_SERVICE_ENDPOINT_DISPLAY = "AWS DynamoDB API Endpoint"
    val AWS_DYNAMODB_SERVICE_ENDPOINT_DEFAULT: String? = null

    open lateinit var config: ConfigDef

    init {
        config = inputConfig ?: baseConfigDef();
    }

    fun baseConfigDef(): ConfigDef {
        return ConfigDef()
            .define(AWS_REGION_CONFIG,
                ConfigDef.Type.STRING,
                AWS_REGION_DEFAULT,
                ConfigDef.Importance.LOW,
                AWS_REGION_DOC,
                AWS_GROUP, 1,
                ConfigDef.Width.SHORT,
                AWS_REGION_DISPLAY)

            .define(AWS_ACCESS_KEY_ID_CONFIG,
                ConfigDef.Type.PASSWORD,
                AWS_ACCESS_KEY_ID_DEFAULT,
                ConfigDef.Importance.LOW,
                AWS_ACCESS_KEY_ID_DOC,
                AWS_GROUP, 2,
                ConfigDef.Width.LONG,
                AWS_ACCESS_KEY_ID_DISPLAY)

            .define(AWS_SECRET_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                AWS_SECRET_KEY_DEFAULT,
                ConfigDef.Importance.LOW,
                AWS_SECRET_KEY_DOC,
                AWS_GROUP, 3,
                ConfigDef.Width.LONG,
                AWS_SECRET_KEY_DISPLAY)

            .define(SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG,
                ConfigDef.Type.STRING,
                SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DEFAULT,
                ConfigDef.Importance.HIGH,
                SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DOC,
                AWS_GROUP, 4,
                ConfigDef.Width.MEDIUM,
                SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DISPLAY)

            .define(SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG,
                ConfigDef.Type.STRING,
                SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DEFAULT,
                ConfigDef.Importance.HIGH,
                SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DOC,
                AWS_GROUP, 5,
                ConfigDef.Width.MEDIUM,
                SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DISPLAY)

            .define(SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG,
                ConfigDef.Type.STRING,
                SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DEFAULT,
                ConfigDef.Importance.HIGH,
                SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DOC,
                AWS_GROUP, 6,
                ConfigDef.Width.MEDIUM,
                SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DISPLAY)

            .define(AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG,
                ConfigDef.Type.STRING,
                AWS_DYNAMODB_SERVICE_ENDPOINT_DEFAULT,
                ConfigDef.Importance.LOW,
                AWS_DYNAMODB_SERVICE_ENDPOINT_DOC,
                AWS_GROUP, 7,
                ConfigDef.Width.MEDIUM,
                AWS_DYNAMODB_SERVICE_ENDPOINT_DISPLAY)

            .define(AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG,
                ConfigDef.Type.STRING,
                AWS_RESOURCE_TAGGING_API_ENDPOINT_DEFAULT,
                ConfigDef.Importance.LOW,
                AWS_RESOURCE_TAGGING_API_ENDPOINT_DOC,
                AWS_GROUP, 8,
                ConfigDef.Width.MEDIUM,
                AWS_RESOURCE_TAGGING_API_ENDPOINT_DISPLAY)

            .define(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG,
                ConfigDef.Type.LIST,
                SRC_DYNAMODB_TABLE_WHITELIST_DEFAULT,
                ConfigDef.Importance.LOW,
                SRC_DYNAMODB_TABLE_WHITELIST_DOC,
                AWS_GROUP, 8,
                ConfigDef.Width.MEDIUM,
                SRC_DYNAMODB_TABLE_WHITELIST_DISPLAY)

            .define(SRC_KCL_TABLE_BILLING_MODE_CONFIG,
                ConfigDef.Type.STRING,
                SRC_KCL_TABLE_BILLING_MODE_DEFAULT,
                ConfigDef.Importance.LOW,
                SRC_KCL_TABLE_BILLING_MODE_DOC,
                AWS_GROUP, 9,
                ConfigDef.Width.MEDIUM,
                SRC_KCL_TABLE_BILLING_MODE_DISPLAY)

            .define(DST_TOPIC_PREFIX_CONFIG,
                ConfigDef.Type.STRING,
                DST_TOPIC_PREFIX_DEFAULT,
                ConfigDef.Importance.HIGH,
                DST_TOPIC_PREFIX_DOC,
                CONNECTOR_GROUP, 1,
                ConfigDef.Width.MEDIUM,
                DST_TOPIC_PREFIX_DISPLAY)

            .define(SRC_INIT_SYNC_DELAY_CONFIG,
                ConfigDef.Type.INT,
                SRC_INIT_SYNC_DELAY_DEFAULT,
                ConfigDef.Importance.LOW,
                SRC_INIT_SYNC_DELAY_DOC,
                CONNECTOR_GROUP, 2,
                ConfigDef.Width.MEDIUM,
                SRC_INIT_SYNC_DELAY_DISPLAY)

            .define(REDISCOVERY_PERIOD_CONFIG,
                ConfigDef.Type.LONG,
                REDISCOVERY_PERIOD_DEFAULT,
                ConfigDef.Importance.LOW,
                REDISCOVERY_PERIOD_DOC,
                CONNECTOR_GROUP, 4,
                ConfigDef.Width.MEDIUM,
                REDISCOVERY_PERIOD_DISPLAY)
        ;
    }

    fun main() {
        println(config.toRst())
    }

    fun getAwsRegion(): String {
        return getString(AWS_REGION_CONFIG)
    }

    fun getAwsAccessKeyId(): Password {
        return getPassword(AWS_ACCESS_KEY_ID_CONFIG)
    }

    fun getAwsAccessKeyIdValue(): String? {
        return if (getPassword(AWS_ACCESS_KEY_ID_CONFIG) == null) null else getPassword(AWS_ACCESS_KEY_ID_CONFIG).value()
    }

    fun getAwsSecretKey(): Password {
        return getPassword(AWS_SECRET_KEY_CONFIG)
    }

    fun getAwsSecretKeyValue(): String? {
        return if (getPassword(AWS_SECRET_KEY_CONFIG) == null) null else getPassword(AWS_SECRET_KEY_CONFIG).value()
    }

    fun getSrcDynamoDBIngestionTagKey(): String {
        return getString(SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG)
    }

    fun getSrcDynamoDBEnvTagKey(): String {
        return getString(SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG)
    }

    fun getSrcDynamoDBEnvTagValue(): String {
        return getString(SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG)
    }

    fun getDestinationTopicPrefix(): String {
        return getString(DST_TOPIC_PREFIX_CONFIG)
    }

    fun getRediscoveryPeriod(): Long {
        return getLong(REDISCOVERY_PERIOD_CONFIG)
    }

    fun getInitSyncDelay(): Int {
        return get(SRC_INIT_SYNC_DELAY_CONFIG) as Int
    }

    fun getDynamoDBServiceEndpoint(): String {
        return getString(AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG)
    }

    fun getResourceTaggingServiceEndpoint(): String {
        return getString(AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG)
    }

    fun getWhitelistTables(): List<String>? {
        return if (getList(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG) != null) getList(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG) else null
    }

    fun getKCLTableBillingMode(): BillingMode {
        return BillingMode.valueOf(getString(SRC_KCL_TABLE_BILLING_MODE_CONFIG))
    }
}