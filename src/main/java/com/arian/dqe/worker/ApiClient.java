package com.arian.dqe.worker;

import com.arian.dqe.infra.AppProps;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Única salida HTTP del worker: siempre la URL base configurada + endpoint validado contra
 * allowlist (evita SSRF). Timeout de cadena = 29 s. Nunca lanza por status: lo clasifica el llamador.
 */
@Component
public class ApiClient {

	public record Response(int status, int retryAfterSeconds, String body) {
		public static final int TIMEOUT = 0;
	}

	private final RestClient http;
	private final Pattern allowlist;

	public ApiClient(AppProps props) {
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory();
		factory.setReadTimeout(Duration.ofSeconds(props.api().timeoutSeconds()));
		this.http = RestClient.builder().baseUrl(props.api().baseUrl()).requestFactory(factory).build();
		this.allowlist = Pattern.compile(props.api().endpointAllowlist());
	}

	public void validate(String endpoint) {
		if (endpoint == null || !allowlist.matcher(endpoint).matches()) {
			throw new IllegalArgumentException("endpoint fuera de allowlist: " + endpoint);
		}
	}

	public Response call(String endpoint, int retryAfterDefault) {
		try {
			return http.get().uri(endpoint).exchange((req, res) -> {
				int status = res.getStatusCode().value();
				String ra = res.getHeaders().getFirst("Retry-After");
				int retryAfter = ra == null ? retryAfterDefault : Integer.parseInt(ra.trim());
				return new Response(status, retryAfter, new String(res.getBody().readAllBytes()));
			});
		}
		catch (ResourceAccessException timeoutOrIo) {
			return new Response(Response.TIMEOUT, retryAfterDefault, "");
		}
	}

	public boolean healthy() {
		return call("/health", 0).status() / 100 == 2;
	}
}
