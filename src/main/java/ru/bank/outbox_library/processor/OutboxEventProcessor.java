package ru.bank.outbox_library.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import ru.bank.outbox_library.model.entity.Outbox;
import ru.bank.outbox_library.model.enums.OutboxStatus;
import ru.bank.outbox_library.status.OutboxStatusManager;

@RequiredArgsConstructor
@Slf4j
public class OutboxEventProcessor {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxStatusManager statusManager;
    private final TopicResolver topicResolver;

    public void process(Outbox event) {
        if (event.getStatus().isTerminal()) {
            return;
        }
        if (event.getStatus() != OutboxStatus.PROCESSING) {
            log.warn("Событие: {} уже в процессе обработки", event.getId());
            return;
        }
        try {
            String topic = topicResolver.resolver(event.getPayload());
            String key = event.getUserId() != null ? event.getUserId().toString() : null;
            if(key == null){
                throw new Exception("Id пользователя: null");
            }
            kafkaTemplate.send(topic, key, event.getPayload());
            statusManager.markAsSent(event);
        } catch (Exception ex){
            log.warn("Ошибка отправки события: {} message: {}", event.getId(), ex.getMessage());
            statusManager.incrementRetry(event);
        }
    }

}
