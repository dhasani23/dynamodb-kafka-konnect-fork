package kcl

import Constants
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.common.InitialPositionInStreamExtended
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import java.net.URI
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

class KclWorkerImpl(
    private val awsCredentialsProvider: AwsCredentialsProvider,
    private val eventsQueue: ArrayBlockingQueue<KclRecordsWrapper>,
    private val recordProcessorsRegister: ConcurrentHashMap<String, ShardInfo>,
) : KclWorker {

    private lateinit var thread: Thread
    private lateinit var scheduler: Scheduler
    private lateinit var schedulerFuture: Future<*>

    override fun start(
        dynamoDbClient: DynamoDbClient,
        dynamoDbStreamsClient: DynamoDbStreamsClient,
        tableName: String,
        taskId: String,
        endpoint: String,
        kclTableBillingMode: BillingMode,
    ): Void {
        val recordProcessorFactory = KclRecordProcessorFactory(
            tableName,
            eventsQueue,
            recordProcessorsRegister
        )

        val streamArn = dynamoDbClient.describeTable { it.tableName(tableName) }.table().latestStreamArn()
        val appName = Constants.KclWorkerApplicationNamePrefix + tableName
        val workerId = appName + Constants.KclWorkerNamePrefix + taskId

        // Configure the KCL v2 scheduler
        scheduler = configureScheduler(
            recordProcessorFactory,
            streamArn,
            appName,
            workerId,
            dynamoDbClient,
            dynamoDbStreamsClient,
            endpoint
        )

        // Start the scheduler in a separate thread
        thread = Thread { 
            try {
                schedulerFuture = scheduler.run()
            } catch (e: KinesisClientLibNonRetryableException) {
                // Handle exceptions
                e.printStackTrace()
            }
        }
        thread.isDaemon = true
        thread.start()

        return Unit as Void
    }

    private fun configureScheduler(
        recordProcessorFactory: ShardRecordProcessorFactory,
        streamArn: String,
        appName: String,
        workerId: String,
        dynamoDbClient: DynamoDbClient,
        dynamoDbStreamsClient: DynamoDbStreamsClient,
        endpoint: String
    ): Scheduler {
        val configsBuilder = ConfigsBuilder(
            streamArn,
            appName,
            dynamoDbStreamsClient,
            dynamoDbClient,
            workerId,
            recordProcessorFactory
        )

        val retrievalConfig = configsBuilder.retrievalConfig()
            .retrievalSpecificConfig(
                PollingConfig(streamArn, dynamoDbStreamsClient)
                    .maxRecords(Constants.StreamsRecordsLimit)
            )
            .initialPositionInStreamExtended(
                InitialPositionInStreamExtended.newInitialPosition(
                    software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON
                )
            )

        val processorConfig = configsBuilder.processingConfig()
            .callProcessRecordsEvenForEmptyRecordList(true)
            .maxRecords(Constants.StreamsRecordsLimit)

        val leaseManagementConfig = configsBuilder.leaseManagementConfig()
            .failoverTimeMillis(Constants.KclFailoverTime.toLong())
            .parentShardPollIntervalMillis(Constants.DefaultParentShardPollIntervalMillis)
            .ignoreUnexpectedChildShards(true)
            
        // If endpoint is not empty, override the endpoint
        if (endpoint.isNotEmpty()) {
            leaseManagementConfig.dynamoDbClient(
                DynamoDbClient.builder()
                    .endpointOverride(URI.create(endpoint))
                    .credentialsProvider(awsCredentialsProvider)
                    .build()
            )
        }

        val metricsConfig = configsBuilder.metricsConfig()
            .metricsLevel(MetricsLevel.NONE)
            .metricsPublishingEnabled(false)

        val cloudWatchConfig = configsBuilder.cloudWatchConfig()
            .publisherFlushMillis(60000)

        return Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            leaseManagementConfig,
            lifecycleConfig = configsBuilder.lifecycleConfig(),
            metricsConfig = metricsConfig,
            processorConfig = processorConfig,
            retrievalConfig = retrievalConfig,
            cloudWatchConfig = cloudWatchConfig
        )
    }

    override fun stop(): Void {
        if (this::scheduler.isInitialized) {
            try {
                scheduler.shutdown()
                
                if (this::schedulerFuture.isInitialized) {
                    schedulerFuture.cancel(true)
                }
                
                if (this::thread.isInitialized) {
                    thread.join(1000)
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        
        return Unit as Void
    }
}