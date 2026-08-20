package ru.bank.outbox_library.status;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.bank.outbox_library.config.OutboxProperties;
import ru.bank.outbox_library.model.entity.Outbox;
import ru.bank.outbox_library.model.enums.OutboxStatus;
import ru.bank.outbox_library.repository.OutboxRepository;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxStatusManager {
    private final OutboxRepository outboxRepository;
    private final OutboxProperties properties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsSent(Outbox event) {
        Outbox managed = outboxRepository.findById(event.getId()).orElseThrow();
        managed.setStatus(OutboxStatus.SENT);
        managed.setLastAttemptAt(LocalDateTime.now());
        managed.setNextAttemptAt(null);
        outboxRepository.save(managed);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void incrementRetry(Outbox event) {
        Outbox managed = outboxRepository.findById(event.getId()).orElseThrow();
        managed.setRetryCount(managed.getRetryCount() + 1);
        managed.setLastAttemptAt(LocalDateTime.now());
        if (managed.getRetryCount() >= properties.getMaxRetry()) {
            managed.setStatus(OutboxStatus.DEAD);
            managed.setNextAttemptAt(null);
            log.warn("Событие: {} достигло лимитов retry, статус DEAD", managed.getId());
        } else {
            managed.setStatus(OutboxStatus.PENDING);
            long delaySeconds = Math.min(3600, (long) Math.pow(2, managed.getRetryCount()) * properties.getInitialDelaySec());
            managed.setNextAttemptAt(LocalDateTime.now().plusSeconds(delaySeconds));
        }
        outboxRepository.save(managed);
    }

}
