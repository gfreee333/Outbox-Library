package ru.bank.outbox_library.processor;

@FunctionalInterface
public interface TopicResolver {
    String resolver(String payload);
}
