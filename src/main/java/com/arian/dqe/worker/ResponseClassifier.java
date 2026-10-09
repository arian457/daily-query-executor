package com.arian.dqe.worker;

import java.util.Set;

/**
 * Tabla de errores del PDF (sección "Errores"). Decide en el worker, no en la cola.
 */
public final class ResponseClassifier {

	public enum Outcome {
		DONE, FAILED_PERMANENT, THROTTLED, RETRY
	}

	private static final Set<Integer> PERMANENT = Set.of(400, 401, 403, 404, 422);

	private ResponseClassifier() {
	}

	/** status = 0 representa timeout de cadena / sin respuesta. */
	public static Outcome classify(int status) {
		if (status >= 200 && status < 300) {
			return Outcome.DONE;
		}
		if (status == 429) {
			return Outcome.THROTTLED;
		}
		if (status == 0 || status >= 500) {
			return Outcome.RETRY;
		}
		if (PERMANENT.contains(status)) {
			return Outcome.FAILED_PERMANENT;
		}
		// ponytail: cualquier otro 4xx también es resultado del cliente; se amplía la tabla si la API lo justifica
		return Outcome.FAILED_PERMANENT;
	}
}
