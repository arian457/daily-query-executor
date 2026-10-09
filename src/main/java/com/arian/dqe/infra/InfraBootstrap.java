package com.arian.dqe.infra;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;

/**
 * Crea colas, tablas, bucket y parámetros si no existen. Reemplaza al IaC (CDK/Terraform)
 * del PDF para la demo local: una sola definición sirve a docker-compose y a los tests.
 * Corre en @PostConstruct, es decir antes de que arranquen los listeners SQS.
 */
@Component
public class InfraBootstrap {

	private static final Logger log = LoggerFactory.getLogger(InfraBootstrap.class);

	private final SqsAsyncClient sqs;
	private final DynamoDbClient dynamo;
	private final S3Client s3;
	private final Params params;
	private final AppProps props;

	public InfraBootstrap(SqsAsyncClient sqs, DynamoDbClient dynamo, S3Client s3, Params params, AppProps props) {
		this.sqs = sqs;
		this.dynamo = dynamo;
		this.s3 = s3;
		this.params = params;
		this.props = props;
	}

	@PostConstruct
	void create() {
		AppProps.Queues q = props.queues();
		String dlqArn = createQueue(q.dlq(), Map.of(QueueAttributeName.FIFO_QUEUE, "true"));
		createQueue(q.tasks(), Map.of(
				QueueAttributeName.FIFO_QUEUE, "true",
				QueueAttributeName.FIFO_THROUGHPUT_LIMIT, "perMessageGroupId",
				QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup",
				QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(q.visibilitySeconds()),
				QueueAttributeName.REDRIVE_POLICY,
				"{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"" + q.maxReceiveCount() + "\"}"));
		createQueue(q.retry(), Map.of());

		createTable(props.tables().plan(), "date");
		createTable(props.tables().execution(), "pk");
		createTable(props.tables().breaker(), "providerId");

		try {
			s3.createBucket(b -> b.bucket(props.bucket()));
		}
		catch (BucketAlreadyOwnedByYouException ignored) {
		}

		params.ensureDefaults(props.limits().maxConcurrentProviders(), props.limits().ratePerSecond());
		log.info("infra lista: colas={},{},{} tablas={},{},{} bucket={}", q.tasks(), q.dlq(), q.retry(),
				props.tables().plan(), props.tables().execution(), props.tables().breaker(), props.bucket());
	}

	private String createQueue(String name, Map<QueueAttributeName, String> attrs) {
		String url = sqs.createQueue(r -> r.queueName(name).attributes(attrs)).join().queueUrl();
		return sqs.getQueueAttributes(r -> r.queueUrl(url).attributeNames(QueueAttributeName.QUEUE_ARN)).join()
				.attributes().get(QueueAttributeName.QUEUE_ARN);
	}

	private void createTable(String name, String pk) {
		try {
			dynamo.createTable(t -> t.tableName(name)
					.billingMode(BillingMode.PAY_PER_REQUEST)
					.attributeDefinitions(AttributeDefinition.builder().attributeName(pk).attributeType(ScalarAttributeType.S).build())
					.keySchema(KeySchemaElement.builder().attributeName(pk).keyType(KeyType.HASH).build()));
		}
		catch (ResourceInUseException ignored) {
		}
	}
}
