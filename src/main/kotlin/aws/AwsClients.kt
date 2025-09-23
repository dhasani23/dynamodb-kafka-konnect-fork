package aws

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient
import java.net.URI

object AwsClients {
    @JvmStatic
    fun buildDynamoDbClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): DynamoDbClient {
        val builder = DynamoDbClient.builder()
            .credentialsProvider(getCredentials(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClientBuilder(ApacheHttpClient.builder())
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }

    @JvmStatic
    fun buildDynamoDbStreamsClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): DynamoDbStreamsClient {
        val builder = DynamoDbStreamsClient.builder()
            .credentialsProvider(getCredentials(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClientBuilder(ApacheHttpClient.builder())
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }

    @JvmStatic
    fun buildAwsResourceGroupsTaggingApiClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): ResourceGroupsTaggingApiClient {
        val builder = ResourceGroupsTaggingApiClient.builder()
            .credentialsProvider(getCredentials(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClientBuilder(ApacheHttpClient.builder())
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }

    @JvmStatic
    fun getCredentials(awsAccessKey: String?, awsSecretKey: String?): AwsCredentialsProvider {
        if (awsAccessKey.isNullOrBlank() || awsSecretKey.isNullOrBlank()) {
            return DefaultCredentialsProvider.create()
        }

        val awsCreds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey)
        return StaticCredentialsProvider.create(awsCreds)
    }
}