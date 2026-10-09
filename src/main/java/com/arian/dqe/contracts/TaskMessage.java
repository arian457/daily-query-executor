package com.arian.dqe.contracts;

/**
 * Mensaje de la cola FIFO. Contrato compartido entre carga, worker y reinyector.
 * attempt = 0 primera ejecución; 1..maxAttempts reintentos.
 */
public record TaskMessage(long recordId, String providerId, String endpoint, String date, int attempt) {

	public TaskMessage nextAttempt() {
		return new TaskMessage(recordId, providerId, endpoint, date, attempt + 1);
	}

	public String executionKey() {
		return recordId + "#" + date;
	}

	public String dedupId() {
		return recordId + "#" + date + "#" + attempt;
	}
}
