package com.arian.dqe;

import com.arian.dqe.contracts.ExecutionStatus;
import com.arian.dqe.contracts.TaskMessage;
import com.arian.dqe.infra.AppProps;
import com.arian.dqe.loader.LoaderScheduler;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Un lote completo contra el mock (integración dev del PDF): todo termina en done o
 * failed_*, la DLQ solo tiene el poison, nunca 2 en vuelo por proveedor, reentregar no cambia nada.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
		"server.port=18080",
		"app.api.base-url=http://localhost:18080",
		"app.loader.cron=-",
		"app.queues.visibility-seconds=5",
		"app.retry.base-seconds=0",
		"app.breaker.provider-pause-seconds=1",
		"app.breaker.provider-max-pause-seconds=2",
		"app.breaker.global-probe-seconds=1",
		"app.limits.rate-per-second=200",
		"app.mockapi.min-latency-ms=0",
		"app.mockapi.max-latency-ms=5",
		"app.mockapi.retry-after-seconds=1"
})
@ActiveProfiles({ "loader", "worker", "reinjector", "mockapi" })
class EndToEndTest {

	@Container
	static final LocalStackContainer localstack = new LocalStackContainer("localstack/localstack:4");

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine")
			.withDatabaseName("dqe")
			.withCopyFileToContainer(MountableFile.forHostPath("infra/postgres/init.sql"), "/docker-entrypoint-initdb.d/init.sql");

	@DynamicPropertySource
	static void props(DynamicPropertyRegistry r) {
		r.add("spring.cloud.aws.endpoint", localstack::getEndpoint);
		r.add("spring.datasource.url", postgres::getJdbcUrl);
	}

	@Autowired LoaderScheduler loader;
	@Autowired DynamoDbClient dynamo;
	@Autowired S3Client s3;
	@Autowired SqsAsyncClient sqs;
	@Autowired SqsTemplate sqsTemplate;
	@Autowired JsonMapper json;
	@Autowired AppProps props;

	@Test
	void oneBatchEndsInFinalStatesWithoutConcurrencyPerProvider() {
		loader.tick(); // 00:00 · plan: N = 10 000, Q = 455, primer lote = ids 1..455 (id 7 es poison)

		await().atMost(Duration.ofSeconds(180)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
			Map<String, Long> byStatus = statusCounts();
			assertEquals(454L, byStatus.values().stream().mapToLong(Long::longValue).sum(), byStatus.toString());
			assertEquals(0L, byStatus.getOrDefault(ExecutionStatus.RETRY.name(), 0L), byStatus.toString());
		});

		Map<String, Long> byStatus = statusCounts();
		assertEquals(9L, byStatus.get(ExecutionStatus.FAILED_EXHAUSTED.name()), "p-013 → 500 × 5 intentos");
		assertEquals(9L, byStatus.get(ExecutionStatus.FAILED_PERMANENT.name()), "p-042 → 404 sin reintento");
		assertEquals(436L, byStatus.get(ExecutionStatus.DONE.name()), "el resto, incluido p-027 tras los 429");

		assertEquals(445, s3.listObjectsV2(r -> r.bucket(props.bucket()).prefix("results/")).keyCount(),
				"un body por done + failed_permanent");

		await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
				assertEquals(1, queueSize(props.queues().dlq()), "solo el poison (endpoint fuera de allowlist)"));

		Map<?, ?> stats = RestClient.create().get().uri("http://localhost:18080/stats").retrieve().body(Map.class);
		assertTrue(stats.values().stream().allMatch(v -> ((Number) v).intValue() <= 1), "1 en vuelo por proveedor: " + stats);

		// Idempotencia: reentregar un mensaje ya hecho no toca DynamoDB
		TaskMessage done = new TaskMessage(1, "p-001", "/providers/p-001/records/1", today(), 0);
		Map<String, AttributeValue> before = execution(done);
		sqsTemplate.send(to -> to.queue(props.queues().tasks()).payload(json.writeValueAsString(done))
				.messageGroupId(done.providerId()).messageDeduplicationId("redelivery-test"));
		await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(0, queueSize(props.queues().tasks())));
		assertEquals(before, execution(done));
	}

	private Map<String, Long> statusCounts() {
		return dynamo.scanPaginator(r -> r.tableName(props.tables().execution())).items().stream()
				.collect(Collectors.groupingBy(i -> i.get("status").s(), TreeMap::new, Collectors.counting()));
	}

	private Map<String, AttributeValue> execution(TaskMessage m) {
		return dynamo.getItem(r -> r.tableName(props.tables().execution())
				.key(Map.of("pk", AttributeValue.fromS(m.executionKey())))).item();
	}

	private int queueSize(String queue) {
		String url = sqs.getQueueUrl(r -> r.queueName(queue)).join().queueUrl();
		return sqs.getQueueAttributes(r -> r.queueUrl(url).attributeNames(List.of(
				QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)))
				.join().attributes().values().stream().mapToInt(Integer::parseInt).sum();
	}

	private static String today() {
		return java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString();
	}
}
