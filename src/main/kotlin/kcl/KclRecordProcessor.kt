package kcl

import Constants
import software.amazon.kinesis.exceptions.InvalidStateException
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events.*
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.processor.ShardRecordProcessor
import java.time.Clock
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class KclRecordProcessor(
    private val tableName: String,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val shardRegister: ConcurrentHashMap<String, ShardInfo>,
    private val clock: Clock,
) : ShardRecordProcessor {

    private var shutdownRequested = false
    private lateinit var shardId: String
    private var lastCheckpointTime = 0L
    private lateinit var lastProcessedSeqNum: String

    override fun initialize(initializationInput: InitializationInput) {
        shardId = initializationInput.shardId()
        lastCheckpointTime = clock.millis()
        lastProcessedSeqNum = ""

        shardRegister.putIfAbsent(shardId, ShardInfo(initializationInput.shardId()))
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        val records = processRecordsInput.records() ?: return

        if (records.isEmpty()) return

        val events = KclRecordsWrapper(shardId, records)
        var added = false
        while (!added && !shutdownRequested) {
            added = try {
                eventsQueue.offer(events, 100, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
        }

        if (shutdownRequested) {
            // TODO: log
        }

        if (records.isNotEmpty()) {
            val firstProcessedSeqNum = records[0].sequenceNumber()
            val lastProcessedSeqNum = records[records.size - 1].sequenceNumber()
            // TODO: log first and last
        }

        // Try to checkpoint if it's time
        if (isTimeToCheckpoint()) {
            checkpoint(processRecordsInput.checkpointer())
        }
    }

    fun checkpoint(checkpointer: RecordProcessorCheckpointer) {
        if (isTimeToCheckpoint()) {
            val lastCommittedRecordSeqNum = shardRegister[shardId]?.getLastCommittedRecordSeqNum() ?: return

            try {
                checkpointer.checkpoint(lastCommittedRecordSeqNum)
                lastCheckpointTime = clock.millis()
            } catch (e: IllegalArgumentException) {
                throw RuntimeException("Invalid sequence number", e)
            } catch (e: InvalidStateException) {
                throw RuntimeException("Invalid kcl state", e)
            } catch (e: ShutdownException) {
                throw RuntimeException("Failed to checkpoint", e)
            }
        }
    }

    private fun isTimeToCheckpoint(): Boolean {
        val passedTime = clock.millis() - lastCheckpointTime
        return TimeUnit.MILLISECONDS.toSeconds(passedTime) >= Constants.KclRecordProcessorCheckpointingInterval
    }

    override fun leaseLost(leaseLostInput: LeaseLostInput) {
        // This is called when the lease is lost to another worker
        shutdownRequested = true
        shardRegister.remove(shardId)
    }

    override fun shardEnded(shardEndedInput: ShardEndedInput) {
        // This is called when the shard is closed and we need to checkpoint to complete processing
        shutdownRequested = true

        if (lastProcessedSeqNum.isNotEmpty()) {
            val processRegister = shardRegister[shardId] ?: return
            var i = 0
            while (processRegister.getLastCommittedRecordSeqNum() != lastProcessedSeqNum) {
                if (i % 20 == 0) {
                    // TODO log shared ended
                }
                i += 1

                Thread.sleep(500)
            }
        }

        shardRegister.remove(shardId)

        try {
            shardEndedInput.checkpointer().checkpoint()
        } catch (e: Exception) {
            // Log the exception
            throw RuntimeException("Failed to checkpoint at shard end", e)
        }
    }

    override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
        // This is called when the worker is being shut down
        shutdownRequested = true

        val shardInfo = shardRegister[shardId] ?: return
        if (shardInfo.getLastCommittedRecordSeqNum() != "") {
            // TODO log graceful shutdown requested
        }

        try {
            shutdownRequestedInput.checkpointer().checkpoint(shardInfo.getLastCommittedRecordSeqNum())
        } catch (e: Throwable) {
            // log failed to checkpoint at shutdown exception
        }
        shardRegister.remove(shardId)
    }
}