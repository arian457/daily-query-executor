package com.arian.dqe.reinjector;

import com.arian.dqe.contracts.TaskMessage;
import com.arian.dqe.infra.AppProps;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * ponytail: stand-in local del EventBridge Pipe (D8). En AWS esto es configuración, no código:
 * Pipe cola estándar → FIFO con MessageGroupId = providerId y DeduplicationId = recordId#date#attempt.
 */
@Component
@Profile("reinjector")
public class RetryReinjector {

	private static final Logger log = LoggerFactory.getLogger(RetryReinjector.class);

	private final SqsTemplate sqs;
	private final JsonMapper json;
	private final AppProps props;

	public RetryReinjector(SqsTemplate sqs, JsonMapper json, AppProps props) {
		this.sqs = sqs;
		this.json = json;
		this.props = props;
	}

	@SqsListener("${app.queues.retry}")
	public void onMessage(String payload, Acknowledgement ack) {
		TaskMessage m = json.readValue(payload, TaskMessage.class);
		sqs.send(to -> to.queue(props.queues().tasks()).payload(payload)
				.messageGroupId(m.providerId()).messageDeduplicationId(m.dedupId()));
		log.info("reinyectado record={} provider={} attempt={}", m.recordId(), m.providerId(), m.attempt());
		ack.acknowledge();
	}
}
