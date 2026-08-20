package ru.bank.outbox_library.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.bank.outbox_library.model.entity.Outbox;
import ru.bank.outbox_library.repository.OutboxRepository;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxEventStore {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public void save(Object event, UUID userId){
        try {
            String payload = objectMapper.writeValueAsString(event);
            Outbox outbox = Outbox.builder()
                    .userId(userId)
                    .payload(payload)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxRepository.save(outbox);
        } catch (Exception ex){
            throw new RuntimeException("Ошибка сохранения в Outbox", ex);
        }
    }

}
