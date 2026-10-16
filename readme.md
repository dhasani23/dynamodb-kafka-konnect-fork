# dynamodb-kafka-konnect

A Kotlin port of the [kafka-connect-dynamodb](https://github.com/trustpilot/kafka-connect-dynamodb/tree/master) project
which implements a [source connector](http://kafka.apache.org/documentation.html#connect) for AWS DynamoDB Streams. This
connector allows for effortless replication of data from DynamoDB tables into Kafka topics with strong consistency
guarantees, which is especially useful when building data pipelines or implementing transactional outboxes to support
asynchronous inter-service communication.

# NOT SUITED FOR PRODUCTION USE!

This was done as a way to practice Kotlin. This connector is missing test coverage, proper logging, and has not been
tested at any meaningful scale. Please use the `kafka-connect-dynamodb` project linked above.

## Required Connect worker configuration

Connector configurations can override Kafka client SASL/OAUTHBEARER settings, including
`sasl.oauthbearer.token.endpoint.url` and `sasl.oauthbearer.jwks.endpoint.url`. Deployers must restrict the URLs the
worker will accept by setting the allow-list system property on every Connect worker JVM (comma-separated list of the
approved identity provider URLs, no wildcards):

```bash
export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp.example.com/oauth2/token,https://idp.example.com/oauth2/jwks"
```

The allow-list is enforced by kafka-clients 3.9.1+ (this connector requires 3.9.2). Any endpoint URL not in the list is
rejected, which prevents local file reads and SSRF from submitted connector configs (CVE-2025-27817). The property has
no default: if it is unset, OAUTHBEARER endpoint URLs are rejected, so the value must be supplied at deployment time.
