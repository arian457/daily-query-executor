package com.arian.dqe.loader;

import com.arian.dqe.infra.AppProps;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.util.Map;
import java.util.Optional;

/**
 * Plan del día en DynamoDB: N, Q y cursor. Escrituras condicionales: dos cargas concurrentes
 * (o una hora reinvocada) nunca encolan el mismo lote dos veces.
 */
@Component
public class PlanStore {

	public record Plan(String date, long n, int q, long cursor) {
		public boolean exhausted() {
			return cursor >= n;
		}
	}

	private final DynamoDbClient dynamo;
	private final String table;

	public PlanStore(DynamoDbClient dynamo, AppProps props) {
		this.dynamo = dynamo;
		this.table = props.tables().plan();
	}

	public Optional<Plan> get(String date) {
		Map<String, AttributeValue> item = dynamo.getItem(r -> r.tableName(table).key(key(date))).item();
		if (item == null || item.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new Plan(date, Long.parseLong(item.get("n").n()), Integer.parseInt(item.get("q").n()),
				Long.parseLong(item.get("cursor").n())));
	}

	/** true si este proceso creó el plan (y debe encolar el primer lote). */
	public boolean create(Plan p) {
		try {
			dynamo.putItem(r -> r.tableName(table).conditionExpression("attribute_not_exists(#d)")
					.expressionAttributeNames(Map.of("#d", "date"))
					.item(Map.of(
							"date", AttributeValue.fromS(p.date()),
							"n", AttributeValue.fromN(String.valueOf(p.n())),
							"q", AttributeValue.fromN(String.valueOf(p.q())),
							"cursor", AttributeValue.fromN(String.valueOf(p.cursor())))));
			return true;
		}
		catch (ConditionalCheckFailedException exists) {
			return false;
		}
	}

	/** true si el cursor avanzó desde el valor esperado; false si otro proceso ya lo movió. */
	public boolean advance(Plan p, long newCursor) {
		try {
			dynamo.updateItem(r -> r.tableName(table).key(key(p.date()))
					.updateExpression("SET #c = :new")
					.conditionExpression("#c = :old")
					.expressionAttributeNames(Map.of("#c", "cursor"))
					.expressionAttributeValues(Map.of(
							":new", AttributeValue.fromN(String.valueOf(newCursor)),
							":old", AttributeValue.fromN(String.valueOf(p.cursor())))));
			return true;
		}
		catch (ConditionalCheckFailedException moved) {
			return false;
		}
	}

	public void recordNotExecuted(String date, long count) {
		dynamo.updateItem(r -> r.tableName(table).key(key(date))
				.updateExpression("SET notExecuted = :n")
				.expressionAttributeValues(Map.of(":n", AttributeValue.fromN(String.valueOf(count)))));
	}

	private static Map<String, AttributeValue> key(String date) {
		return Map.of("date", AttributeValue.fromS(date));
	}
}
