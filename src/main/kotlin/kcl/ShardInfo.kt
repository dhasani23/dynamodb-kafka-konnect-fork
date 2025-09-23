package kcl

class ShardInfo(private val shardId: String) {
    @Volatile
    private var lastCommittedRecordSeqNum = ""

    fun getShardId(): String {
        return shardId
    }

    fun getLastCommittedRecordSeqNum(): String {
        return lastCommittedRecordSeqNum
    }

    fun setLastCommittedRecordSeqNum(seqNum: String) {
        this.lastCommittedRecordSeqNum = seqNum
    }
}