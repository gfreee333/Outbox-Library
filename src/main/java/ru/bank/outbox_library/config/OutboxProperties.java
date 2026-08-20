package ru.bank.outbox_library.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "outbox")
public class OutboxProperties {
    private int batchSize = 100;
    private int maxRetry = 10;
    private long initialDelaySec = 5;
    private long fixedDelay = 5000;
}
