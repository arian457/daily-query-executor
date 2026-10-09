package com.arian.dqe;

import com.arian.dqe.infra.AppProps;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(AppProps.class)
public class DqeApplication {

	public static void main(String[] args) {
		SpringApplication.run(DqeApplication.class, args);
	}

}
