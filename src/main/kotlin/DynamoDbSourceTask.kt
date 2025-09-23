
import aws.AwsClients
import aws.DynamoDbTableScanner
import aws.TableScanner
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.kinesis.retrieval.KinesisClientRecord
import kcl.KclRecordsWrapper
import kcl.KclWorker
import kcl.KclWorkerImpl
import kcl.ShardInfo
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import utilities.RecordConverter
import java.math.BigInteger
import java.time.Clock
import java.time.Instant
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime


class DynamoDbSourceTask(
    private var clock: Clock?,
    private var client: DynamoDbClient?,
    private var tableScanner: TableScanner?,
    private var kclWorker: KclWorker?,
) : SourceTask() {
    private var shutdown: Boolean = false
    private val shardRegister = ConcurrentHashMap<String, ShardInfo>()
    private val eventsQueue = ArrayBlockingQueue<KclRecordsWrapper>(10, true)

    private lateinit var sourceInfo: SourceInfo
    private lateinit var tableDesc: TableDescription
    private lateinit var recordConverter: RecordConverter
    private var initSyncDelay: Int = -1

    init {
        if (clock == null) {
            clock = Clock.systemUTC()
        }
    }

    @SuppressWarnings("unused")
    public fun DynamoDBSourceTask() {}

    override fun version(): String {
        return "0.1"
    }

    override fun start(configProperties: Map<String, String>) {
        val config = DynamoDbSourceTaskConfig(configProperties);

        if (client == null) {
            client = AwsClients.buildDynamoDbClient(
                config.getAwsRegion(),
                config.getDynamoDBServiceEndpoint(),
                config.getAwsAccessKeyIdValue(),
                config.getAwsSecretKeyValue(),
            )
        }

        startWithClient(config, client!!);
    }

    private fun startWithClient(config: DynamoDbSourceTaskConfig, client: DynamoDbClient) {
        tableDesc = client.describeTable { it.tableName(config.getTableName()) }.table()
        initSyncDelay = config.getInitSyncDelay()

        if (tableScanner == null) {
            val readCapacity = if (tableDesc.provisionedThroughput() != null) {
                tableDesc.provisionedThroughput().readCapacityUnits()
            } else {
                0 // Default for on-demand capacity
            }
            tableScanner = DynamoDbTableScanner(client, tableDesc.tableName(), readCapacity)
        }

        recordConverter = RecordConverter(tableDesc, config.getDestinationTopicPrefix())

        val dynamoDbStreamsClient = AwsClients.buildDynamoDbStreamsClient(
            config.getAwsRegion(),
            config.getDynamoDBServiceEndpoint(),
            config.getAwsAccessKeyIdValue(),
            config.getAwsSecretKeyValue(),
        )

        if (kclWorker == null) {
            kclWorker = KclWorkerImpl(
                AwsClients.getCredentials(config.getAwsAccessKeyIdValue(), config.getAwsSecretKeyValue()),
                eventsQueue,
                shardRegister
            )
        }
        kclWorker!!.start(client, dynamoDbStreamsClient, tableDesc.tableName(), config.getTaskId(), config.getDynamoDBServiceEndpoint(), config.getKCLTableBillingMode())

        shutdown = false

        setStateFromOffset()
    }

    private fun setStateFromOffset() {
        val offset = context.offsetStorageReader()
            .offset(Collections.singletonMap("table_name", tableDesc.tableName()))

        if (offset != null) {
            sourceInfo = SourceInfo.fromOffset(offset, clock!!)
        } else {
            sourceInfo = SourceInfo(tableDesc.tableName(), clock!!)
            sourceInfo.startInitSync()
        }
    }

    override fun stop() {
        shutdown = true;
        kclWorker?.stop();
    }

    override fun poll(): List<SourceRecord> {
        if (shutdown) return listOf();

        if (sourceInfo.initSyncStatus == InitSyncStatus.Running) {
            return initSync();
        }

        if (sourceInfo.initSyncStatus == InitSyncStatus.Finished) {
            return sync();
        }

        throw Exception("Invalid SourceInfo->InitSyncStatus: ${sourceInfo.initSyncStatus}");
    }

    @OptIn(ExperimentalTime::class)
    private fun initSync(): LinkedList<SourceRecord> {
        if (sourceInfo.lastInitSyncStart <= Instant.now(clock!!).minusSeconds(
            Duration.convert(19.0, DurationUnit.HOURS, DurationUnit.SECONDS).toLong()
        )) {
            sourceInfo.startInitSync();
        }

        sourceInfo.initSync = true;

        if (sourceInfo.initSyncCount == 0L) {
            Thread.sleep((initSyncDelay * 1000).toLong());
        }

        val scanResult = sourceInfo.exclusiveStartKey?.let { tableScanner?.getItems(it) } ?: ScanResult();

        val result = LinkedList<SourceRecord>();
        var lastRecord: Map<String, AttributeValue>? = null;

        scanResult.items.forEach { record ->
            lastRecord = record;
            sourceInfo.initSyncCount += 1;
            result.add(recordConverter.toSourceRecord(
                    sourceInfo,
                    Envelope.Operation.Read,
                    record,
                    sourceInfo.lastInitSyncStart,
                    null,
                    null
                    ));
        }

        sourceInfo.exclusiveStartKey = scanResult.lastEvaluatedKey;
        if (sourceInfo.exclusiveStartKey == null) {
            sourceInfo.endInitSync();
        }

        if (!result.isEmpty() && lastRecord != null) {
            result.removeLast();
            result.add(recordConverter.toSourceRecord(
                sourceInfo,
                Envelope.Operation.Read,
                lastRecord!!,
                sourceInfo.lastInitSyncStart,
                null,
                null,
            ))
        }

        return result;
    }

    private fun sync(): LinkedList<SourceRecord> {
        val dynamoDBRecords = eventsQueue.poll(500, TimeUnit.MILLISECONDS)
            ?: return LinkedList<SourceRecord>();

        sourceInfo.initSync = false;

        val result = LinkedList<SourceRecord>();
        for (record in dynamoDBRecords.getRecords()) {
            try {
                val arrivalTimestamp = record.approximateArrivalTimestamp;
                if (isPreInitSyncRecord(arrivalTimestamp)) {
                    RegisterAsProcessed(dynamoDBRecords.getShardId(), record.sequenceNumber);
                    continue;
                }

                if (recordIsInDangerZone(arrivalTimestamp)) {
                    sourceInfo.startInitSync();
                    return LinkedList<SourceRecord>();
                }
                val dynamoDbRecord = (record as RecordAdapter).internalObject;
                val op = getOperation(dynamoDbRecord.eventName);
                val attributes = if (dynamoDbRecord.dynamodb.newImage != null) {
                    dynamoDbRecord.dynamodb.newImage;
                } else {
                    dynamoDbRecord.dynamodb.keys;
                }
                val sourceRecord: SourceRecord = recordConverter.toSourceRecord(
                    sourceInfo,
                    op,
                    attributes,
                    arrivalTimestamp.toInstant(),
                    dynamoDBRecords.getShardId(),
                    record.sequenceNumber
                );
                result.add(sourceRecord);
                if (op === Envelope.Operation.Delete) {
                    val tombstoneRecord = SourceRecord(
                        sourceRecord.sourcePartition(),
                        sourceRecord.sourceOffset(),
                        sourceRecord.topic(),
                        sourceRecord.keySchema(), sourceRecord.key(),
                        null, null
                    );
                    result.add(tombstoneRecord);
                }
            } catch (ex: java.lang.Exception) {
                RegisterAsProcessed(dynamoDBRecords.getShardId(), record.sequenceNumber);
            }
        }

        return result;
    }

    @OptIn(ExperimentalTime::class)
    private fun isPreInitSyncRecord(arrivalTimestamp: Date): Boolean {
        return arrivalTimestamp.toInstant()
            .plusSeconds(Duration.convert(
                1.0,
                DurationUnit.HOURS,
                DurationUnit.SECONDS
            ).toLong()) <= sourceInfo.lastInitSyncStart;
    }

    @OptIn(ExperimentalTime::class)
    private fun recordIsInDangerZone(arrivalTimestamp: Date): Boolean {
        return arrivalTimestamp.toInstant() <= Instant.now(clock).minusSeconds(
            Duration.convert(20.0, DurationUnit.HOURS, DurationUnit.SECONDS).toLong());
    }

    private fun getOperation(eventName: String): Envelope.Operation {
        return when (eventName) {
            "INSERT" -> Envelope.Operation.Create
            "MODIFY" -> Envelope.Operation.Update
            "REMOVE" -> Envelope.Operation.Delete
            else -> throw java.lang.Exception("Unsupported DynamoDB event name: $eventName");
        }
    }

    override fun commitRecord(record: SourceRecord) {
        if (record.sourceOffset().containsKey(RecordConverter.ShardSequenceNum)) {
            val shardId = record.sourceOffset()[RecordConverter.ShardId] as String?;
            val sequenceNumber = record.sourceOffset()[RecordConverter.ShardSequenceNum] as String?;
            if (!sequenceNumber.isNullOrEmpty()) {
                RegisterAsProcessed(shardId, sequenceNumber);
            }
        }
    }

    private fun RegisterAsProcessed(shardId: String?, sequenceNumber: String) {
        val shardInfo = shardRegister[shardId];
        val currentSeqNo: String = shardInfo?.getLastCommittedRecordSeqNum() ?: "";

        if (currentSeqNo != "") {
            val currentSeqNoInt: BigInteger = BigInteger(shardInfo!!.getLastCommittedRecordSeqNum());
            val sequenceNumberInt = BigInteger(sequenceNumber);
            if (currentSeqNoInt >= sequenceNumberInt) {
                return;
            }
        }
        shardInfo?.setLastCommittedRecordSeqNum(sequenceNumber);
    }

    fun getShardRegister(): ConcurrentHashMap<String, ShardInfo> {
        return shardRegister;
    }

    fun getEventsQueue(): ArrayBlockingQueue<KclRecordsWrapper> {
        return eventsQueue;
    }

    fun getSourceInfo(): SourceInfo {
        return sourceInfo;
    }
}