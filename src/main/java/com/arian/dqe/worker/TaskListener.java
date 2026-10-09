package com.arian.dqe.worker;

import com.arian.dqe.contracts.ExecutionStatus;
import com.arian.dqe.contracts.TaskMessage;
import com.arian.dqe.infra.AppProps;
import com.arian.dqe.infra.Params;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consume la FIFO (grupo = proveedor): la infraestructura garantiza 1 en vuelo por proveedor.
 * Pausar un proveedor = extender visibility sin borrar; reintentar = borrar y esperar fuera de la FIFO.
 */
@Component
@Profile("worker")
public class TaskListener {

	private static final Logger log = LoggerFactory.getLogger(TaskListener.class);
	private static final int GLOBAL_PAUSE_SECONDS = 30;
	private static final int RETRY_AFTER_DEFAULT = 30;

	private final ApiClient api;
	private final ResultStore results;
	private final ProviderBreaker providerBreaker;
	private final GlobalBreaker globalBreaker;
	private final RateLimiter rate;
	private final SqsTemplate sqs;
	private final JsonMapper json;
	private final MeterRegistry metrics;
	private final AppProps props;

	public TaskListener(ApiClient api, ResultStore results, ProviderBreaker providerBreaker, GlobalBreaker globalBreaker,
			Params params, SqsTemplate sqs, JsonMapper json, MeterRegistry metrics, AppProps props) {
		this.api = api;
		this.results = results;
		this.providerBreaker = providerBreaker;
		this.globalBreaker = globalBreaker;
		this.rate = new RateLimiter(params::ratePerSecond);
		this.sqs = sqs;
		this.json = json;
		this.metrics = metrics;
		this.props = props;
	}

	@SqsListener("${app.queues.tasks}")
	public void onMessage(String payload, Acknowledgement ack, Visibility visibility) throws InterruptedException {
		TaskMessage m = json.readValue(payload, TaskMessage.class);

		var existing = results.get(m);
		if (existing.isPresent() && (existing.get().status().isFinal() || existing.get().attempts() > m.attempt())) {
			log.debug("duplicado ignorado record={} attempt={}", m.recordId(), m.attempt());
			ack.acknowledge();
			return;
		}
		if (globalBreaker.isOpen()) {
			visibility.changeTo(GLOBAL_PAUSE_SECONDS);
			return;
		}
		ProviderBreaker.State breaker = providerBreaker.state(m.providerId());
		int pause = providerBreaker.remainingPause(breaker);
		if (pause > 0) {
			visibility.changeTo(pause);
			return;
		}
		api.validate(m.endpoint());

		rate.acquire();
		ApiClient.Response res = api.call(m.endpoint(), RETRY_AFTER_DEFAULT);
		ResponseClassifier.Outcome outcome = ResponseClassifier.classify(res.status());
		metrics.counter("dqe.calls", "outcome", outcome.name()).increment();
		log.info("record={} provider={} attempt={} http={} outcome={}", m.recordId(), m.providerId(), m.attempt(),
				res.status(), outcome);

		switch (outcome) {
			case DONE -> {
				results.markFinal(m, ExecutionStatus.DONE, res.status(), res.body());
				providerBreaker.recordSuccess(m.providerId(), breaker);
				globalBreaker.record(m.providerId(), true);
				ack.acknowledge();
			}
			case FAILED_PERMANENT -> {
				results.markFinal(m, ExecutionStatus.FAILED_PERMANENT, res.status(), res.body());
				globalBreaker.record(m.providerId(), true);
				ack.acknowledge();
			}
			case THROTTLED -> visibility.changeTo(res.retryAfterSeconds());
			case RETRY -> {
				providerBreaker.recordFailure(m.providerId(), breaker);
				globalBreaker.record(m.providerId(), false);
				if (m.attempt() >= props.retry().maxAttempts()) {
					results.markFinal(m, ExecutionStatus.FAILED_EXHAUSTED, res.status(), null);
				}
				else if (results.markRetry(m, m.attempt() + 1)) {
					TaskMessage next = m.nextAttempt();
					int delay = Backoff.seconds(next.attempt(), props.retry().baseSeconds(), props.retry().capSeconds());
					sqs.send(to -> to.queue(props.queues().retry()).payload(json.writeValueAsString(next)).delaySeconds(delay));
				}
				ack.acknowledge();
			}
		}
	}
}
