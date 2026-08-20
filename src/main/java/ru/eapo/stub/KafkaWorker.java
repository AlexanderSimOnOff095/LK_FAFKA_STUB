package ru.eapo.stub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaWorker implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaWorker.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String topic;
    private final Repository repository;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProducer<byte[], byte[]> producer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public KafkaWorker(String bootstrapServers, String topic, String group, Repository repository) {
        this.topic = topic;
        this.repository = repository;

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.consumer = new KafkaConsumer<>(consumerProperties);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        this.producer = new KafkaProducer<>(producerProperties);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        try {
            while (running.get()) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
                    process(record);
                }
            }
        } catch (WakeupException exception) {
            if (running.get()) {
                throw exception;
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private void process(ConsumerRecord<byte[], byte[]> record) {
        try {
            JsonNode event = JSON.readTree(record.value());
            String key = Domain.decodeMessageKey(record.key());
            if (!Domain.REQUEST_TYPE.equals(event.path("eventType").asText())
                    || !"eapo-cab".equals(event.path("producer").asText())) {
                commit(record);
                return;
            }

            Domain.validateRequest(event, key);
            String fingerprint = Domain.fingerprint(event);
            Repository.ProcessedRequest old = repository.findProcessed(event.path("requestId").asText());
            JsonNode result;
            if (old != null) {
                result = old.fingerprint().equals(fingerprint)
                        ? old.payload()
                        : Domain.buildResult(event, null, new Domain.ProcessingError(
                                "REQUEST_ID_CONFLICT",
                                "requestId already used with another payload",
                                false
                        ));
            } else {
                Settings settings = repository.settings();
                if (settings.errorSimulationEnabled()) {
                    result = Domain.buildResult(event, null, new Domain.ProcessingError(
                            settings.errorCode(), settings.errorMessage(), settings.retryable()
                    ));
                } else {
                    try {
                        String nextStatus = Domain.chooseStatus(
                                event.path("currentStatus").asText(), settings, repository.transitions()
                        );
                        result = Domain.buildResult(event, nextStatus, null);
                    } catch (NoSuchElementException exception) {
                        result = Domain.buildResult(event, null, new Domain.ProcessingError(
                                "UNKNOWN_STATUS", "Status transition is not configured", false
                        ));
                    }
                }
                repository.saveProcessed(event.path("requestId").asText(), fingerprint, result);
            }

            int delay = repository.settings().resultPublishDelayMs();
            if (delay > 0) {
                Thread.sleep(delay);
            }
            publish(key, result);
            commit(record);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            LOG.warn("Kafka worker interrupted");
        } catch (Exception exception) {
            LOG.error("Message processing failed", exception);
        }
    }

    private void publish(String key, JsonNode value) throws Exception {
        ProducerRecord<byte[], byte[]> result = new ProducerRecord<>(
                topic,
                Domain.encodeMessageKey(key),
                JSON.writeValueAsBytes(value)
        );
        producer.send(result).get(10, TimeUnit.SECONDS);
    }

    private void commit(ConsumerRecord<byte[], byte[]> record) {
        consumer.commitSync(Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        ));
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
