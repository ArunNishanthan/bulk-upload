package com.example.demo.ingestion.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class IngestionExecutorConfiguration {

    @Bean(destroyMethod = "close")
    public ExecutorService ingestionExecutor() {
        log.info("Creating virtual-thread-backed ingestion executor");
        return Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("ingestion-", 0).factory()
        );
    }
}
