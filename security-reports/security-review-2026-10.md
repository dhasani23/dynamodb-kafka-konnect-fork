# Security and production-readiness review: dynamodb-kafka-konnect

| | |
|---|---|
| Repository | `dhasani23/dynamodb-kafka-konnect-fork` |
| Revision reviewed | `3c8b139` (`main`) |
| Review date | 2026-10-14 |
| Reviewed by | Data Platform Security |
| Scope | `src/main/kotlin`, `build.gradle.kts`, `.github/atx-transform.yml` |
| Method | Manual code and configuration review. Tool results (Dependabot, Semgrep, detekt) were triaged separately and are only referenced here where they overlap. |

The connector is planned for the CDC pipeline that replicates the `orders` and `payments`
DynamoDB tables into Kafka. Ratings use our Critical / High / Medium / Low scale. Items marked
**Blocker** must be fixed before the connector is deployed to any shared Connect cluster.

## Summary

| ID | Rating | Title | Blocker |
|---|---|---|---|
| DPSR-01 | High | Long-lived AWS access keys accepted in connector configuration | Yes |
| DPSR-02 | Medium | Custom AWS endpoints accept plain HTTP | No |
| DPSR-03 | Medium | DynamoDB client is built with the tagging API endpoint | No |
| DPSR-04 | High | Kafka client allows file read and SSRF through OAUTHBEARER URLs (CVE-2025-27817) | Yes |
| DPSR-05 | Medium | AWS SDK for Java 1.x and KCL 1.x are past end of support | No |
| DPSR-06 | High | Change events are dropped silently when conversion fails | Yes |
| DPSR-07 | High | Source task never initializes or restores its state | Yes |
| DPSR-08 | High | KCL progress is never checkpointed, and empty batches crash the processor | Yes |
| DPSR-09 | Medium | `atx-transform.yml`: shell injection, unpinned actions and broad token permissions | No |

## Findings

### DPSR-01 (High): Long-lived AWS access keys accepted in connector configuration

**Where:** `src/main/kotlin/DynamoDbSourceConnectorConfig.kt:24-34` (`aws.access.key.id`,
`aws.secret.key`) and `src/main/kotlin/aws/AwsClients.kt:66-73` (`getCredentials`).

When both options are set, `getCredentials` wraps them in `BasicAWSCredentials` and an
`AWSStaticCredentialsProvider`. Connector configuration is stored in the Connect config topic
and returned by the Connect REST API, so the keys are readable by anyone with access to either.
IAM user keys used this way are not rotated, and the same keys end up in every task (CWE-522).

**Recommendation:** Remove the static key options and rely on the default credential provider
chain (instance profile, ECS task role or EKS pod identity). If a deployment cannot use a role,
require the keys to come from a Kafka Connect `ConfigProvider` and document that in
`readme.md`.

### DPSR-02 (Medium): Custom AWS endpoints accept plain HTTP

**Where:** `src/main/kotlin/aws/AwsClients.kt:86-87`, options `dynamodb.service.endpoint`
(`DynamoDbSourceConnectorConfig.kt:83`) and `resource.tagging.service.endpoint` (`:78`).

Any URL is passed to `withEndpointConfiguration`. An `http://` endpoint sends table items and
signed requests in clear text (CWE-319).

**Recommendation:** Validate both options with a `ConfigDef.Validator` that requires `https`,
and allow `http` only for loopback hosts so DynamoDB Local keeps working.

### DPSR-03 (Medium): DynamoDB client is built with the tagging API endpoint

**Where:** `src/main/kotlin/DynamoDbSourceConnector.kt:38-43`.

`start()` builds the DynamoDB client with `getResourceTaggingServiceEndpoint()` instead of
`getDynamoDBServiceEndpoint()`. When the tagging endpoint is set (we route it through a VPC
endpoint), `DescribeTable` calls are sent to the tagging host, table discovery fails and every
table is skipped without an error.

**Recommendation:** Pass `connectorConfig.getDynamoDBServiceEndpoint()` to
`buildDynamoDbClient`.

### DPSR-04 (High): Kafka client allows file read and SSRF through OAUTHBEARER URLs (CVE-2025-27817)

**Where:** `build.gradle.kts:14-15` (`org.apache.kafka:kafka-clients:3.6.1`,
`org.apache.kafka:connect-api:3.6.1`).

Kafka clients before 3.9.1 accept any URL in `sasl.oauthbearer.token.endpoint.url` and
`sasl.oauthbearer.jwks.endpoint.url` (GHSA-vgq5-3255-v292). Our Connect clusters let connector
configs override client settings, so a connector config can make the worker read local files or
call internal endpoints. GitHub rates this advisory Medium. We rate it High for Connect because
connector configs are submitted by many teams.

**Recommendation:** Upgrade `kafka-clients` and `connect-api` together to 3.9.2, which also
fixes the kafka-clients issue in the Dependabot export. On the workers, set the
`org.apache.kafka.sasl.oauthbearer.allowed.urls` system property to the identity provider URLs
we use.

### DPSR-05 (Medium): AWS SDK for Java 1.x and KCL 1.x are past end of support

**Where:** `build.gradle.kts:16-19` (`amazon-kinesis-client:1.15.1`,
`dynamodb-streams-kinesis-adapter:1.6.0`, `aws-java-sdk-sts:1.12.669`,
`aws-java-sdk-resourcegroupstaggingapi:1.12.669`).

AWS SDK for Java 1.x reached end of support on December 31, 2025 and receives no further
security fixes. KCL 1.x and the 1.x DynamoDB Streams Kinesis Adapter are built on it. Most of
the transitive Jackson and protobuf alerts in the Dependabot export come in through this stack.

**Recommendation:** Migrate to AWS SDK for Java 2.x and a supported KCL release line with the
matching DynamoDB Streams Kinesis Adapter. Until then, pin patched versions of the transitive
libraries with Gradle constraints.

### DPSR-06 (High): Change events are dropped silently when conversion fails

**Where:** `src/main/kotlin/DynamoDbSourceTask.kt:229-231`.

Any exception while converting a stream record is caught, the record's sequence number is
registered as processed, and nothing is logged or produced. A malformed item or an unsupported
key type therefore disappears from the topic with no trace, which breaks the replication
guarantee downstream consumers rely on (CWE-390). detekt reports the swallowed exception on the
same line.

**Recommendation:** Log the table, shard and sequence number and fail the task, or send the
record to a dead-letter topic. Do not mark a record processed unless it was produced.

### DPSR-07 (High): Source task never initializes or restores its state

**Where:** `src/main/kotlin/DynamoDbSourceTask.kt:101-111` (`setStateFromOffset`) and `:118-127`
(`poll`).

`sourceInfo` is a `lateinit` property that is only assigned in `setStateFromOffset()`, and
nothing calls that function. The first `poll()` throws `UninitializedPropertyAccessException`,
so the task fails on start and stored offsets are never read. detekt reports the unused function.

**Recommendation:** Call `setStateFromOffset()` at the end of `startWithClient()`, after
`tableDesc` is loaded, and cover start and restart with a unit test.

### DPSR-08 (High): KCL progress is never checkpointed, and empty batches crash the processor

**Where:** `src/main/kotlin/kcl/KclRecordProcessor.kt:37-58` and `:60`;
`src/main/kotlin/kcl/KclWorkerImpl.kt:90`.

- `processRecords` never calls `checkpoint()`, so progress is only checkpointed when a shard
  ends. After any restart the worker reads from `TRIM_HORIZON` again and replays up to 24 hours
  of changes.
- The worker enables `withCallProcessRecordsEvenForEmptyRecordList(true)`, but
  `processRecords` reads `records.get(0)` (line 55) without checking for an empty list, which
  throws `IndexOutOfBoundsException` on idle shards.
- The local `lastProcessedSeqNum` on line 56 shadows the property of the same name, so the
  property stays empty and the shutdown wait in `onTerminate` never sees progress.

**Recommendation:** Return early for an empty batch, store the last sequence number in the
property, and checkpoint through `processRecordsInput.checkpointer` after the batch is queued.

### DPSR-09 (Medium): `atx-transform.yml`: shell injection, unpinned actions and broad token permissions

**Where:** `.github/atx-transform.yml:30-33`, `:47-54`, `:97-107`, `:91` and `:141-142`.

The file is not under `.github/workflows/` yet, so GitHub does not run it, but
`.github/setup-atx-workflow.sh` moves it there. Once it is active:

- `${{ inputs.transform_name }}`, `${{ inputs.build_command }}` and `${{ inputs.config_yaml }}`
  are expanded inside `run:` scripts, and the result is passed to `eval` (CWE-78). Anyone who
  can dispatch the workflow can run commands with its credentials.
- `id-token`, `contents` and `pull-requests` write permissions are granted to the whole
  workflow instead of the steps that need them.
- The CLI installer is piped from `curl` straight into `bash`.
- The GitHub App token is written into the remote URL and the branch is force-pushed.

Semgrep reports the injection points and the unpinned actions separately.

**Recommendation:** Pass inputs through `env:` and quote them, build the command as an array
instead of using `eval`, pin every action to a commit SHA, move permissions to the job with the
minimum each step needs, and verify the installer's checksum before running it.
