package com.arian.dqe.worker;

import com.arian.dqe.contracts.ExecutionStatus;
import com.arian.dqe.contracts.TaskMessage;
import com.arian.dqe.infra.AppProps;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Estado por registro y día en DynamoDB con escritura condicional (idempotente, D1);
 * bodies en S3 con campos sensibles enmascarados. Clave S3 = results/date=…/provider=…/id.json.
 */
@Component
public class ResultStore {

	public record Execution(ExecutionStatus status, int attempts) {
	}

	private final DynamoDbClient dynamo;
	private final S3Client s3;
	private final String table;
	private final String bucket;
	private final List<Pattern> masks;

	public ResultStore(DynamoDbClient dynamo, S3Client s3, AppProps props) {
		this.dynamo = dynamo;
		this.s3 = s3;
		this.table = props.tables().execution();
		this.bucket = props.bucket();
		this.masks = props.maskFields().stream()
				.map(f -> Pattern.compile("(\"" + Pattern.quote(f) + "\"\\s*:\\s*)(\"[^\"]*\"|[^,}\\s]+)"))
				.toList();
	}

	public Optional<Execution> get(TaskMessage m) {
		Map<String, AttributeValue> item = dynamo.getItem(r -> r.tableName(table).key(key(m))).item();
		if (item == null || item.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new Execution(ExecutionStatus.valueOf(item.get("status").s()),
				Integer.parseInt(item.get("attempts").n())));
	}

	/** true si este intento quedó registrado; false si ya existía uno igual o posterior (duplicado). */
	public boolean markRetry(TaskMessage m, int nextAttempt) {
		return write(m, Map.of(
				":st", AttributeValue.fromS(ExecutionStatus.RETRY.name()),
				":a", AttributeValue.fromN(String.valueOf(nextAttempt)),
				":now", AttributeValue.fromS(Instant.now().toString())),
				"SET #st = :st, attempts = :a, providerId = :p, updatedAt = :now",
				"attribute_not_exists(pk) OR attempts < :a");
	}

	/** Guarda el body en S3 y cierra el registro. false si ya estaba en estado final (reentrega). */
	public boolean markFinal(TaskMessage m, ExecutionStatus status, int httpStatus, String body) {
		String s3Key = body == null || body.isEmpty() ? null
				: "results/date=" + m.date() + "/provider=" + m.providerId() + "/" + m.recordId() + ".json";
		if (s3Key != null) {
			s3.putObject(r -> r.bucket(bucket).key(s3Key).contentType("application/json"), RequestBody.fromString(mask(body)));
		}
		Map<String, AttributeValue> values = new HashMap<>(Map.of(
				":st", AttributeValue.fromS(status.name()),
				":a", AttributeValue.fromN(String.valueOf(m.attempt())),
				":http", AttributeValue.fromN(String.valueOf(httpStatus)),
				":now", AttributeValue.fromS(Instant.now().toString()),
				":done", AttributeValue.fromS(ExecutionStatus.DONE.name()),
				":fp", AttributeValue.fromS(ExecutionStatus.FAILED_PERMANENT.name()),
				":fe", AttributeValue.fromS(ExecutionStatus.FAILED_EXHAUSTED.name())));
		String update = "SET #st = :st, attempts = :a, httpStatus = :http, providerId = :p, updatedAt = :now";
		if (s3Key != null) {
			values.put(":key", AttributeValue.fromS(s3Key));
			update += ", s3Key = :key";
		}
		return write(m, values, update, "attribute_not_exists(pk) OR NOT (#st IN (:done, :fp, :fe))");
	}

	private boolean write(TaskMessage m, Map<String, AttributeValue> values, String update, String condition) {
		Map<String, AttributeValue> all = new HashMap<>(values);
		all.put(":p", AttributeValue.fromS(m.providerId()));
		try {
			dynamo.updateItem(r -> r.tableName(table).key(key(m))
					.updateExpression(update)
					.conditionExpression(condition)
					.expressionAttributeNames(Map.of("#st", "status"))
					.expressionAttributeValues(all));
			return true;
		}
		catch (ConditionalCheckFailedException duplicate) {
			return false;
		}
	}

	String mask(String body) {
		String out = body;
		for (Pattern p : masks) {
			out = p.matcher(out).replaceAll("$1\"***\"");
		}
		return out;
	}

	private static Map<String, AttributeValue> key(TaskMessage m) {
		return Map.of("pk", AttributeValue.fromS(m.executionKey()));
	}
}
