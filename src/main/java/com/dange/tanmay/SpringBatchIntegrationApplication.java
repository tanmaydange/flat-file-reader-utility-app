package com.dange.tanmay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.annotation.IntegrationComponentScan;

@IntegrationComponentScan
@SpringBootApplication
public class SpringBatchIntegrationApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchIntegrationApplication.class, args);
	}

}
