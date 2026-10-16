# dynamodb-kafka-konnect

A Kotlin port of the [kafka-connect-dynamodb](https://github.com/trustpilot/kafka-connect-dynamodb/tree/master) project
which implements a [source connector](http://kafka.apache.org/documentation.html#connect) for AWS DynamoDB Streams. This
connector allows for effortless replication of data from DynamoDB tables into Kafka topics with strong consistency
guarantees, which is especially useful when building data pipelines or implementing transactional outboxes to support
asynchronous inter-service communication.

# NOT SUITED FOR PRODUCTION USE!

This was done as a way to practice Kotlin. This connector is missing test coverage, proper logging, and has not been
tested at any meaningful scale. Please use the `kafka-connect-dynamodb` project linked above.

## Kafka Connect worker requirements

This connector depends on `org.apache.kafka:kafka-clients` / `connect-api` 3.9.2, which restricts
the OAUTHBEARER endpoints clients may contact (CVE-2025-27817). On every Connect worker that runs
this connector, set the allow-list system property to the approved identity provider URLs, for example:

```
KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp.example.com/oauth2/token,https://idp.example.com/.well-known/jwks.json"
```

Without this property the worker rejects `sasl.oauthbearer.token.endpoint.url` and
`sasl.oauthbearer.jwks.endpoint.url` values in submitted connector configs.

## CI configuration

The `.github/atx-transform.yml` workflow verifies the ATX CLI installer before running it. Set the
repository variable `ATX_CLI_INSTALLER_SHA256` to the pinned SHA-256 of
`https://transform-cli.awsstatic.com/install.sh`. The install step fails when the variable is unset
or the downloaded installer does not match the checksum.
