package kcl

import Constants
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class KclWorkerImpl(
    private val awsCredentialsProvider: AWSCredentialsProvider,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val recordProcessorsRegister: ConcurrentHashMap<String, ShardInfo>,
) : KclWorker {

    private lateinit var thread: Thread;
    private lateinit var worker: Worker;

    override fun start(
        dynamoDbClient: AmazonDynamoDB,
        dynamoDbStreamsClient: AmazonDynamoDBStreams,
        tableName: String,
        taskId: String,
        endpoint: String,
        kclTableBillingMode: BillingMode,
        ): Void? {
        val recordProcessorFactory = KclRecordProcessorFactory(
            tableName,
            eventsQueue,
            recordProcessorsRegister
        );

        val clientLibConfig = getClientLibConfig(
            tableName,
            taskId,
            dynamoDbClient,
            endpoint,
            kclTableBillingMode
        );

        val adapterClient = AmazonDynamoDBStreamsAdapterClient(dynamoDbStreamsClient);
        adapterClient.setGenerateRecordBytes(false);

        val cloudwatchClient = NoopKclCloudwatch();

        worker = StreamsWorkerFactory
            .createDynamoDbStreamsWorker(
                recordProcessorFactory,
                clientLibConfig,
                adapterClient,
                dynamoDbClient,
                cloudwatchClient
            );

        thread = Thread(worker);
        thread.isDaemon = true;
        thread.start();

        return null;
    }

    fun getClientLibConfig(
        tableName: String,
        taskId: String,
        dynamoDbClient: AmazonDynamoDB,
        endpoint: String,
        kclTableBillingMode: BillingMode
    ): KinesisClientLibConfiguration {
        val streamArn = dynamoDbClient.describeTable(
            DescribeTableRequest()
                .withTableName(tableName)
        ).table.latestStreamArn;

        val appName = Constants.KclWorkerApplicationNamePrefix + tableName;

        return KinesisClientLibConfiguration(
            appName,
            streamArn,
            awsCredentialsProvider,
            appName + Constants.KclWorkerNamePrefix + taskId,
        )
            .withCallProcessRecordsEvenForEmptyRecordList(true)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
            .withMaxRecords(Constants.StreamsRecordsLimit)
            .withIdleTimeBetweenReadsInMillis(Constants.IdleTimeBetweenReads.toLong())
            .withParentShardPollIntervalMillis(Constants.DefaultParentShardPollIntervalMillis)
            .withFailoverTimeMillis(Constants.KclFailoverTime.toLong())
            .withLogWarningForTaskAfterMillis(60 * 1000)
            .withIgnoreUnexpectedChildShards(true)
            .withDynamoDBEndpoint(endpoint)
            .withBillingMode(kclTableBillingMode)
    }

    fun getWorker(): Worker {
        return worker;
    }

    override fun stop(): Void? {
        var workerStopped = false;
        val workerShutdownFuture = worker.startGracefulShutdown();
        workerStopped = try {
            workerShutdownFuture.get(10, TimeUnit.SECONDS)
        } catch (e: Exception) {
            // TODO log
            false
        };

        if (!workerStopped) {
            worker.shutdown();
        }

        try {
            thread.join(1000);
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt();
        }

        return null;
    }
}