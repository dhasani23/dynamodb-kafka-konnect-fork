package kcl

import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import java.time.Clock
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

class KclRecordProcessorFactory(
    private val tableName: String,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val shardRegister: ConcurrentHashMap<String, ShardInfo>,
) : ShardRecordProcessorFactory {
    override fun shardRecordProcessor(): ShardRecordProcessor {
        return KclRecordProcessor(tableName, eventsQueue, shardRegister, Clock.systemUTC())
    }
}