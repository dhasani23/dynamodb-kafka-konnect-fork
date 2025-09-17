package aws

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPIClientBuilder
// AWS SDK v2 imports for credentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
// AWS SDK v2 imports for DynamoDB
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.apache.ApacheHttpClient
import java.net.URI
// AWS SDK v2 imports for DynamoDB Streams
// import software.amazon.awssdk.services.dynamodbstreams.DynamoDbStreamsClient - COMMENTED OUT due to missing dependency
// AWS SDK v2 imports for Resource Groups Tagging API
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient
// AWS SDK v2 imports for STS
import software.amazon.awssdk.services.sts.StsClient

object AwsClients {
    @JvmStatic
    fun buildDynamoDbClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AmazonDynamoDB {
        return configureBuilder(
            AmazonDynamoDBClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }
    
    @JvmStatic
    fun buildDynamoDbClientV2(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): DynamoDbClient {
        val httpClient = ApacheHttpClient.builder()
            .build()
        
        val builder = DynamoDbClient.builder()
            .credentialsProvider(getCredentialsV2(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClient(httpClient)
        
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
    ): AmazonDynamoDBStreams {
        return configureBuilder(
            AmazonDynamoDBStreamsClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }

    // Commented out until the appropriate AWS SDK v2 dependency is added
    /*
    @JvmStatic
    fun buildDynamoDbStreamsClientV2(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): DynamoDbStreamsClient {
        val httpClient = ApacheHttpClient.builder()
            .build()
        
        val builder = DynamoDbStreamsClient.builder()
            .credentialsProvider(getCredentialsV2(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClient(httpClient)
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }
    */

    @JvmStatic
    fun buildAwsResourceGroupsTaggingApiClient(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AWSResourceGroupsTaggingAPI {
        return configureBuilder(
            AWSResourceGroupsTaggingAPIClientBuilder.standard(),
            awsRegion,
            serviceEndpoint,
            awsAccessKeyId,
            awsSecretKey
        ).build();
    }

    @JvmStatic
    fun buildResourceGroupsTaggingApiClientV2(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): ResourceGroupsTaggingApiClient {
        val httpClient = ApacheHttpClient.builder()
            .build()
        
        val builder = ResourceGroupsTaggingApiClient.builder()
            .credentialsProvider(getCredentialsV2(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClient(httpClient)
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }

    @JvmStatic
    fun buildStsClientV2(
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): StsClient {
        val httpClient = ApacheHttpClient.builder()
            .build()
        
        val builder = StsClient.builder()
            .credentialsProvider(getCredentialsV2(awsAccessKeyId, awsSecretKey))
            .region(Region.of(awsRegion))
            .httpClient(httpClient)
        
        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.endpointOverride(URI.create(serviceEndpoint))
        }
        
        return builder.build()
    }

    @JvmStatic
    fun getCredentials(awsAccessKey: String?, awsSecretKey: String?): AWSCredentialsProvider {
        if (awsAccessKey.isNullOrBlank() || awsSecretKey.isNullOrBlank()) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }

        val awsCreds = BasicAWSCredentials(awsAccessKey, awsSecretKey);
        return AWSStaticCredentialsProvider(awsCreds);
    }

    @JvmStatic
    fun getCredentialsV2(awsAccessKey: String?, awsSecretKey: String?): AwsCredentialsProvider {
        if (awsAccessKey.isNullOrBlank() || awsSecretKey.isNullOrBlank()) {
            return DefaultCredentialsProvider.create();
        }

        val awsCreds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);
        return StaticCredentialsProvider.create(awsCreds);
    }

    @JvmStatic
    private fun <SubClass : AwsClientBuilder<*, *>, TypeToBuild>configureBuilder(
        builder: AwsClientBuilder<SubClass, TypeToBuild>,
        awsRegion: String,
        serviceEndpoint: String?,
        awsAccessKeyId: String?,
        awsSecretKey: String?,
    ): AwsClientBuilder<SubClass, TypeToBuild> {
        builder.withCredentials(getCredentials(awsAccessKeyId, awsSecretKey))
            .withClientConfiguration(ClientConfiguration().withThrottledRetries(true));

        if (!serviceEndpoint.isNullOrEmpty()) {
            builder.withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(serviceEndpoint, awsRegion));
        } else {
            builder.withRegion(awsRegion);
        }

        return builder;
    }
}