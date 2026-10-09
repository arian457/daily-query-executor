package com.arian.dqe.infra;

import io.awspring.cloud.autoconfigure.core.AwsClientBuilderConfigurer;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.ssm.SsmClient;

import java.time.Duration;

@Configuration
public class AwsConfig {

	@Bean
	SsmClient ssmClient(AwsClientBuilderConfigurer configurer) {
		return configurer.configure(SsmClient.builder()).build();
	}

	/**
	 * maxMessagesPerPoll = 1: cada poll trae un mensaje de un grupo distinto, así una pausa
	 * (429 / breaker) deja al grupo entero bloqueado en SQS y no solo al mensaje recibido.
	 * MANUAL ack: el worker decide cuándo borrar (nunca en 429 ni en excepción).
	 */
	@Bean
	SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(SqsAsyncClient sqs, Params params,
			InfraBootstrap infraReady) {
		return SqsMessageListenerContainerFactory.builder()
				.sqsAsyncClient(sqs)
				.configure(o -> o.maxConcurrentMessages(params.maxConcurrentProviders())
						.maxMessagesPerPoll(1)
						.pollTimeout(Duration.ofSeconds(5))
						.acknowledgementMode(AcknowledgementMode.MANUAL))
				.build();
	}
}
