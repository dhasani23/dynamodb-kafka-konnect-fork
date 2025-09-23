package kcl

import software.amazon.kinesis.retrieval.KinesisClientRecord

class KclRecordsWrapper(private val shardId: String, private val records: List<KinesisClientRecord>) {
    fun getShardId(): String {
        return shardId
    }

    fun getRecords(): List<KinesisClientRecord> {
        return records
    }
}