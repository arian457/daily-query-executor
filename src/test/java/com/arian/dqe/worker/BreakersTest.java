package com.arian.dqe.worker;

import com.arian.dqe.infra.AppProps;
import com.arian.dqe.infra.Params;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BreakersTest {

	static final AppProps.Breaker CFG = new AppProps.Breaker(5, 120, 1800, 0.5, 3, 20, 120, 30);
	static final AppProps PROPS = new AppProps(null, null, new AppProps.Tables("plan", "execution", "breaker"), "b",
			null, null, CFG, null, null, null, List.of());

	@Test
	void providerBreakerOpensAfter5FailuresThenDoublesPauseUpTo30Min() {
		Clock clock = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
		ProviderBreaker breaker = new ProviderBreaker(mock(DynamoDbClient.class), PROPS, clock);

		ProviderBreaker.State s = ProviderBreaker.State.CLOSED;
		for (int i = 0; i < 4; i++) {
			s = breaker.recordFailure("p", s);
			assertEquals(0, breaker.remainingPause(s));
		}
		s = breaker.recordFailure("p", s);                       // 5º fallo seguido → pausa 2 min
		assertEquals(120, breaker.remainingPause(s));

		Clock later = Clock.fixed(Instant.ofEpochSecond(1_000_000 + 121), ZoneOffset.UTC);
		breaker = new ProviderBreaker(mock(DynamoDbClient.class), PROPS, later);
		assertEquals(0, breaker.remainingPause(s));               // vencida: el próximo mensaje es la sonda
		s = breaker.recordFailure("p", s);                        // sonda falla → duplica
		assertEquals(240, breaker.remainingPause(s));

		for (int i = 0; i < 10; i++) {
			breaker = new ProviderBreaker(mock(DynamoDbClient.class), PROPS,
					Clock.fixed(Instant.ofEpochSecond(s.openUntil() + 1), ZoneOffset.UTC));
			s = breaker.recordFailure("p", s);
		}
		assertEquals(1800, s.pause());                            // tope 30 min
	}

	@Test
	void globalBreakerOpensOnCorrelatedFailuresAndProbeCloses() {
		Params params = mock(Params.class);
		ApiClient api = mock(ApiClient.class);
		GlobalBreaker breaker = new GlobalBreaker(params, PROPS, api, Clock.systemUTC());

		for (int i = 0; i < 30; i++) {
			breaker.record("p-" + (i % 2), false);                // 100 % fallos pero solo 2 proveedores
		}
		verify(params, never()).setGlobalBreaker(true);

		breaker.record("p-9", false);                             // 3er proveedor fallando → abre
		verify(params).setGlobalBreaker(true);

		when(params.globalBreakerOpen()).thenReturn(true);
		when(api.healthy()).thenReturn(false, true);
		breaker.probe();
		verify(params, never()).setGlobalBreaker(false);
		breaker.probe();
		verify(params).setGlobalBreaker(false);
	}

	@Test
	void maskHidesSensitiveFieldsBeforeStoring() {
		AppProps props = new AppProps(null, null, PROPS.tables(), "b", null, null, CFG, null, null, null, List.of("taxId", "email"));
		ResultStore store = new ResultStore(mock(DynamoDbClient.class), null, props);
		String masked = store.mask("{\"id\":1,\"taxId\":\"76.123.456-7\",\"email\": \"a@b.cl\",\"balance\":42}");
		assertEquals("{\"id\":1,\"taxId\":\"***\",\"email\": \"***\",\"balance\":42}", masked);
		assertTrue(!masked.contains("76.123"));
	}
}
