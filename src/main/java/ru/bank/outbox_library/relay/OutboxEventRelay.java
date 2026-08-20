package ru.bank.outbox_library.relay;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import ru.bank.outbox_library.config.OutboxProperties;
import ru.bank.outbox_library.model.entity.Outbox;
import ru.bank.outbox_library.model.enums.OutboxStatus;
import ru.bank.outbox_library.processor.OutboxEventProcessor;
import ru.bank.outbox_library.repository.OutboxRepository;

import java.time.LocalDateTime;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class OutboxEventRelay {


    private final OutboxRepository outboxRepository;
    private final OutboxEventProcessor processor;
    private final TaskExecutor outboxExecutor;
    private final OutboxProperties properties;


    @Scheduled(fixedDelayString = "${outbox.fixed-delay:5000}")
    public void relay(){
        List<Outbox> events = outboxRepository.findAndLockPendingEvents(properties.getBatchSize());
        if(events.isEmpty()){
            return;
        }
        events.forEach(e -> {
            e.setStatus(OutboxStatus.PROCESSING);
            e.setLastAttemptAt(LocalDateTime.now());
        });
        outboxRepository.saveAll(events);
        events.forEach(event -> outboxExecutor.execute(() -> processor.process(event)));
    }

}
