package utilities

import Envelope
import SourceInfo
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import java.time.Instant
import java.util.*
import java.util.stream.Collectors


class RecordConverter(
    private val tableDesc: TableDescription,
    private val topicNamePrefix: String,
) {
    private val topicName: String
    private val valueSchema: Schema
    private lateinit var keySchema: Schema

    private lateinit var keys: List<String>

    companion object {
        val objectMapper = ObjectMapper()
        val ShardId = "src_shard_id"
        val ShardSequenceNum = "src_shard_sequence_num"
    }

    init {
        topicName = topicNamePrefix + tableDesc.tableName()
        valueSchema = SchemaBuilder.struct()
            .name(AvroSchemaNameParser.DEFAULT.adjust("io.jhegarty14.connector.dynamodb.envelope"))
            .field(Envelope.FieldName.Version, Schema.STRING_SCHEMA)
            .field(Envelope.FieldName.Document, DynamoDbJson.schema())
//            .field(Envelope.FieldName.Source, SourceInfo)
            .field(Envelope.FieldName.Operation, Schema.STRING_SCHEMA)
            .field(Envelope.FieldName.Timestamp, Schema.INT64_SCHEMA)
            .build()
    }

    fun toSourceRecord(
        sourceInfo: SourceInfo,
        op: Envelope.Operation,
        attributes: Map<String, AttributeValue>,
        arrivalTimestamp: Instant,
        shardId: String?,
        sequenceNumber: String?,
    ): SourceRecord {
        val sanitizedAttributes = attributes.entries.stream()
            .collect(
                Collectors.toMap<Map.Entry<String, AttributeValue>, String, AttributeValue, LinkedHashMap<String, AttributeValue>>(
                    { e -> this.sanitizeAttributeName(e.key) },
                    { e -> e.value },
                    { u, _ -> u },
                    { LinkedHashMap() }))

        val offsets = SourceInfo.toOffset(sourceInfo)
        offsets.put(ShardId, shardId)
        offsets.put(ShardSequenceNum, sequenceNumber)

        if (this::keySchema.isInitialized.not()) {
            keys = tableDesc.keySchema().stream()
                .map { v -> sanitizeAttributeName(v) }
                .collect(Collectors.toList())
            keySchema = getKeySchema(keys)
        }

        val keyData = Struct(getKeySchema(keys))
        for (key in keys) {
            val attributeValue = sanitizedAttributes[key]
            if (attributeValue?.s() != null) {
                keyData.put(key, attributeValue.s())
                continue
            } else if (attributeValue?.n() != null) {
                keyData.put(key, attributeValue.n())
                continue
            }
            throw Exception("Unsupported key AttributeValue")
        }

        val valueData = Struct(valueSchema)
            .put(Envelope.FieldName.Version, sourceInfo.version)
            .put(Envelope.FieldName.Document, objectMapper.writeValueAsString(sanitizedAttributes))
            .put(Envelope.FieldName.Source, SourceInfo.toStruct(sourceInfo))
            .put(Envelope.FieldName.Operation, op.code)
            .put(Envelope.FieldName.Timestamp, arrivalTimestamp.toEpochMilli())

        return SourceRecord(
            Collections.singletonMap("table_name", sourceInfo.tableName),
            offsets,
            topicName,
            keySchema,
            keyData,
            valueSchema,
            valueData,
        )
    }

    fun getKeySchema(keys: List<String>): Schema {
        val keySchemaBuilder = SchemaBuilder.struct().name(AvroSchemaNameParser.DEFAULT.adjust(topicName + ".Key"))

        for (key in keys) {
            keySchemaBuilder.field(key, Schema.STRING_SCHEMA)
        }

        return keySchemaBuilder.build()
    }

    private fun sanitizeAttributeName(element: KeySchemaElement): String {
        return sanitizeAttributeName(element.attributeName())
    }

    private fun sanitizeAttributeName(attributeName: String): String {
        val sanitizedAttributeName = attributeName.replace(Regex("^[^a-zA-Z_]|(?<!^)[^a-zA-Z0-9_]"), "")

        if (sanitizedAttributeName.isEmpty()) {
            throw IllegalStateException(
                "The field name ${attributeName} couldn't be sanitized"
            )
        }

        return sanitizedAttributeName
    }
}