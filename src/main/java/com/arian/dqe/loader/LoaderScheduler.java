package com.arian.dqe.loader;

import com.arian.dqe.contracts.TaskMessage;
import com.arian.dqe.infra.AppProps;
import com.arian.dqe.infra.Params;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.LocalDate;
import java.util.List;

/**
 * Carga horaria (D2, D3). A las 00:00 planifica (N = MAX(id), Q = N / slots) y encola el
 * primer lote; cada hora admite o difiere según backlog y breaker global. Idempotente:
 * plan y cursor se escriben condicionalmente, reinvocar una hora no duplica lotes.
 */
@Component
@Profile("loader")
public class LoaderScheduler {

	private static final Logger log = LoggerFactory.getLogger(LoaderScheduler.class);

	private final RegistroRepository registro;
	private final PlanStore plans;
	private final SqsTemplate sqs;
	private final SqsAsyncClient sqsClient;
	private final Params params;
	private final JsonMapper json;
	private final MeterRegistry metrics;
	private final AppProps props;
	private final Clock clock;

	@Autowired
	public LoaderScheduler(RegistroRepository registro, PlanStore plans, SqsTemplate sqs, SqsAsyncClient sqsClient,
			Params params, JsonMapper json, MeterRegistry metrics, AppProps props) {
		this(registro, plans, sqs, sqsClient, params, json, metrics, props, Clock.systemUTC());
	}

	LoaderScheduler(RegistroRepository registro, PlanStore plans, SqsTemplate sqs, SqsAsyncClient sqsClient,
			Params params, JsonMapper json, MeterRegistry metrics, AppProps props, Clock clock) {
		this.registro = registro;
		this.plans = plans;
		this.sqs = sqs;
		this.sqsClient = sqsClient;
		this.params = params;
		this.json = json;
		this.metrics = metrics;
		this.props = props;
		this.clock = clock;
	}

	@Scheduled(cron = "${app.loader.cron}", zone = "UTC")
	public void tick() {
		LocalDate today = LocalDate.now(clock);
		PlanStore.Plan plan = plans.get(today.toString()).orElseGet(() -> plan(today));
		if (plan == null) {
			return;
		}
		admit(plan);
	}

	/** 00:00 · Planifica. Devuelve null si otro loader creó el plan en paralelo (ya encoló el primer lote). */
	PlanStore.Plan plan(LocalDate today) {
		closePrevious(today.minusDays(1).toString());
		long n = registro.maxId();
		int q = (int) Math.ceil((double) n / props.loader().slots());
		PlanStore.Plan plan = new PlanStore.Plan(today.toString(), n, q, 0);
		if (!plans.create(plan)) {
			return null;
		}
		log.info("plan {}: N={} Q={}", plan.date(), n, q);
		return plan;
	}

	/** Cada hora · Admite o difiere. */
	void admit(PlanStore.Plan plan) {
		if (plan.exhausted()) {
			log.info("plan {} completo: cursor={} N={}", plan.date(), plan.cursor(), plan.n());
			return;
		}
		if (params.globalBreakerOpen()) {
			defer(plan, "breaker global abierto");
			return;
		}
		int backlog = backlog();
		if (backlog > plan.q() / 2) {
			defer(plan, "backlog " + backlog + " > Q/2");
			return;
		}
		loadBatch(plan);
	}

	private void defer(PlanStore.Plan plan, String reason) {
		// ponytail: difiere sin repartir; repartir = recalcular Q sobre las horas restantes cuando haga falta
		metrics.counter("dqe.loader.deferred").increment();
		log.warn("lote diferido ({}): cursor={} N={}", reason, plan.cursor(), plan.n());
	}

	private void loadBatch(PlanStore.Plan plan) {
		List<RegistroRepository.Row> rows = registro.after(plan.cursor(), plan.q()).stream()
				.filter(r -> r.id() <= plan.n()).toList();
		if (rows.isEmpty()) {
			return;
		}
		long last = rows.getLast().id();
		if (!plans.advance(plan, last)) {
			log.info("lote ya encolado por otra carga: cursor={}", plan.cursor());
			return;
		}
		for (RegistroRepository.Row r : rows) {
			TaskMessage m = new TaskMessage(r.id(), r.providerId(), r.endpoint(), plan.date(), 0);
			sqs.send(to -> to.queue(props.queues().tasks()).payload(json.writeValueAsString(m))
					.messageGroupId(m.providerId()).messageDeduplicationId(m.dedupId()));
		}
		metrics.counter("dqe.loader.enqueued").increment(rows.size());
		log.info("lote encolado: {} filas, cursor {} → {}", rows.size(), plan.cursor(), last);
	}

	private void closePrevious(String date) {
		plans.get(date).filter(p -> !p.exhausted()).ifPresent(p -> {
			long notExecuted = p.n() - p.cursor();
			plans.recordNotExecuted(date, notExecuted);
			metrics.counter("dqe.loader.not_executed").increment(notExecuted);
			log.warn("día {} cerrado con {} registros no ejecutados", date, notExecuted);
		});
	}

	private int backlog() {
		String url = sqsClient.getQueueUrl(r -> r.queueName(props.queues().tasks())).join().queueUrl();
		return Integer.parseInt(sqsClient.getQueueAttributes(r -> r.queueUrl(url)
				.attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)).join()
				.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
	}
}
