package com.arian.dqe.worker;

import com.arian.dqe.worker.ResponseClassifier.Outcome;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseClassifierTest {

	// Tabla "Errores" del PDF: 2xx done · 400/401/403/404/422 failed_permanent · 429 pausa · 5xx/timeout retry
	@ParameterizedTest
	@CsvSource({
			"200, DONE", "204, DONE",
			"400, FAILED_PERMANENT", "401, FAILED_PERMANENT", "403, FAILED_PERMANENT", "404, FAILED_PERMANENT", "422, FAILED_PERMANENT",
			"429, THROTTLED",
			"500, RETRY", "502, RETRY", "503, RETRY", "0, RETRY"
	})
	void classifiesLikeThePdfTable(int status, Outcome expected) {
		assertEquals(expected, ResponseClassifier.classify(status));
	}
}
