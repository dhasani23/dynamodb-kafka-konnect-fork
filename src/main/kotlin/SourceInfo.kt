import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import utilities.AvroSchemaNameParser
import java.time.Clock
import java.time.Instant

class SourceInfo(
    val tableName: String,
    private val clock: Clock,
) {
    val version: String
    var exclusiveStartKey: Map<String, AttributeValue>? = null
    var initSync = false
    var initSyncStatus = InitSyncStatus.Undefined
    lateinit var lastInitSyncStart: Instant
    var lastInitSyncEnd: Instant? = null
    var initSyncCount = 0L

    init {
        version = "1.0"
    }

    companion object {
        private val gson = Gson()

        val Version = "version"
        val TableName = "table_name"
        val InitSync = "init_sync"
        val InitSyncState = "init_sync_state"
        val InitSyncStart = "init_sync_start"
        val InitSyncEnd = "init_sync_end"
        val InitSyncCount = "init_sync_count"
        val ExclusiveStartKey = "exclusive_start_key"

        val StructSchema = SchemaBuilder.struct()
            .name(AvroSchemaNameParser.defaultAdjuster().adjust("io.jhegarty14.connector.dynamodb.source"))
            .field(Version, Schema.STRING_SCHEMA)
            .field(TableName, Schema.STRING_SCHEMA)
            .field(InitSync, Schema.BOOLEAN_SCHEMA)
            .field(InitSyncState, Schema.STRING_SCHEMA)
            .field(InitSyncStart, Schema.INT64_SCHEMA)
            .field(InitSyncEnd, Schema.OPTIONAL_INT64_SCHEMA)
            .field(InitSyncCount, Schema.OPTIONAL_INT64_SCHEMA)
            .build()

        fun toStruct(sourceInfo: SourceInfo): Struct {
            val struct = Struct(StructSchema)
                .put(Version, sourceInfo.version)
                .put(TableName, sourceInfo.tableName)
                .put(InitSync, sourceInfo.initSync)
                .put(InitSyncState, sourceInfo.initSyncStatus.toString())
                .put(InitSyncStart, sourceInfo.lastInitSyncStart.toEpochMilli())

            if (sourceInfo.lastInitSyncEnd != null) {
                struct.put(InitSyncEnd, sourceInfo.lastInitSyncEnd?.toEpochMilli())
                    .put(InitSyncCount, sourceInfo.initSyncCount)
            }

            return struct
        }

        fun toOffset(sourceInfo: SourceInfo): MutableMap<String, Any?> {
            val offset = LinkedHashMap<String, Any?>()
            offset.put(Version, sourceInfo.version)
            offset.put(TableName, sourceInfo.tableName)
            offset.put(InitSyncState, sourceInfo.initSyncStatus.toString())
            offset.put(InitSyncStart, sourceInfo.lastInitSyncStart.toEpochMilli())

            if (sourceInfo.exclusiveStartKey != null) {
                offset.put(ExclusiveStartKey, gson.toJson(sourceInfo.exclusiveStartKey))
            }

            if (sourceInfo.lastInitSyncEnd != null) {
                sourceInfo.lastInitSyncEnd?.toEpochMilli()?.let { offset.put(InitSyncEnd, it) }
            }

            offset.put(InitSyncCount, sourceInfo.initSyncCount)

            return offset
        }

        fun fromOffset(offset: Map<String, Any>, clock: Clock): SourceInfo {
            val sourceInfo = SourceInfo(
                offset.get(TableName).toString(),
                clock
            )
            sourceInfo.initSyncStatus = InitSyncStatus.valueOf(offset.get(InitSyncState).toString())
            sourceInfo.lastInitSyncStart = Instant.ofEpochMilli(offset.get(InitSyncStart) as Long)

            if (offset.containsKey(ExclusiveStartKey)) {
                val empMapType = object : TypeToken<Map<String, AttributeValue>>() {}.type
                sourceInfo.exclusiveStartKey = gson.fromJson(offset.get(ExclusiveStartKey) as String, empMapType)
            }
            if (offset.containsKey(InitSyncEnd)) {
                sourceInfo.lastInitSyncEnd = Instant.ofEpochMilli(offset.get(InitSyncEnd) as Long)
            }
            if (offset.containsKey(InitSyncCount)) {
                sourceInfo.initSyncCount = offset.get(InitSyncCount) as Long
            }

            return sourceInfo
        }
    }

    fun startInitSync() {
        initSyncStatus = InitSyncStatus.Running
        lastInitSyncStart = Instant.now(clock)
        lastInitSyncEnd = null
        exclusiveStartKey = null
        initSyncCount = 0L
    }

    fun endInitSync() {
        initSyncStatus = InitSyncStatus.Finished
        lastInitSyncEnd = Instant.now(clock)
    }

    override fun toString(): String {
        return "SourceInfo {" +
                " tableName='${tableName}'" +
                ", initSync='${initSync}'" +
                ", initSyncStatus='${initSyncStatus}'" +
                ", initSyncCount='${exclusiveStartKey}'" +
                '}'
    }

    }
}