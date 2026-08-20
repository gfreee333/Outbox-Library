package ru.bank.outbox_library.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import ru.bank.outbox_library.processor.OutboxEventProcessor;
import ru.bank.outbox_library.processor.TopicResolver;
import ru.bank.outbox_library.relay.OutboxEventRelay;
import ru.bank.outbox_library.repository.OutboxRepository;
import ru.bank.outbox_library.status.OutboxStatusManager;
import ru.bank.outbox_library.store.OutboxEventStore;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxEventStore outboxEventStore(OutboxRepository repository, ObjectMapper objectMapper){
        return new OutboxEventStore(repository, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxStatusManager outboxStatusManager(OutboxRepository repository, OutboxProperties properties){
        return new OutboxStatusManager(repository, properties);
    }

    @Bean
    @ConditionalOnBean({KafkaTemplate.class, TopicResolver.class})
    public OutboxEventProcessor outboxEventProcessor(
            @Qualifier("criticalKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
            OutboxStatusManager statusManager,
            TopicResolver topicResolver
    ){
        return new OutboxEventProcessor(kafkaTemplate, statusManager, topicResolver);
    }

    @Bean
    @ConditionalOnMissingBean(name = "outboxExecutor")
    public TaskExecutor outboxExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("outbox-");
        executor.initialize();
        return executor;
    }

    @Bean
    @ConditionalOnBean(OutboxEventProcessor.class)
    public OutboxEventRelay outboxEventRelay(
            OutboxRepository repository,
            OutboxEventProcessor processor,
            @Qualifier("outboxExecutor") TaskExecutor taskExecutor,
            OutboxProperties properties
    ){
        return new OutboxEventRelay(repository, processor, taskExecutor, properties);
    }

}
