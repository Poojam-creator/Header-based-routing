package solutions.Infy.kafka.connect.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import solutions.Infy.kafka.connect.config.HeaderBasedSinkConnectorConfig;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Writer {
    private static final Logger log = LoggerFactory.getLogger(Writer.class);
    private final String dlqTopic;
    private final Properties lookupConsumerProps;
    private final Properties producerProps;

    public Writer(Map<String, String> properties) {
        HeaderBasedSinkConnectorConfig config = new HeaderBasedSinkConnectorConfig(properties);

        // Producer Properties
        this.producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(HeaderBasedSinkConnectorConfig.BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Consumer Properties
        this.lookupConsumerProps = new Properties();
        lookupConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(HeaderBasedSinkConnectorConfig.BOOTSTRAP_SERVERS));
        lookupConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "header-router-group");
        lookupConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        lookupConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        lookupConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        lookupConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.dlqTopic = config.getString(HeaderBasedSinkConnectorConfig.DLQ_TOPIC);
    }

    private String getHeaderMapping(String headerValue) {
        log.info("Fetching mapping for header: {}", headerValue);

        String latestMapping = null;

        try (KafkaConsumer<String, String> lookupConsumer = new KafkaConsumer<>(lookupConsumerProps)) {
            lookupConsumer.subscribe(Collections.singletonList("header-to-topic"));

            while (true) {
                ConsumerRecords<String, String> records = lookupConsumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    if (headerValue.equals(record.key())) {
                        latestMapping = record.value();
                    }
                }
            }

            if (latestMapping != null) {
                log.info("Final mapping for header {}: {}", headerValue, latestMapping);
            } else {
                log.warn("No mapping found for header: {}", headerValue);
            }
        } catch (Exception e) {
            log.error("Error fetching header mapping for: {}", headerValue, e);
        }
        return latestMapping;
    }

    public void write(Collection<SinkRecord> records) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (SinkRecord sinkRecord : records) {
                log.info("Processing record: {}", sinkRecord);

                try {
                    // Extract header and ensure header exists
                    Header destinationTypeHeader = sinkRecord.headers().lastWithName("destinationType");
                    String routeHeader = (destinationTypeHeader != null) ? destinationTypeHeader.value().toString() : null;

                    // If no destinationType header is found, route to DLQ (or handle according to your rules)
                    if (routeHeader == null) {
                        log.warn("No destinationType header found for record: {}", sinkRecord);
                        routeHeader = "default";  // Default route or DLQ
                    }

                    // Fetch mapping dynamically for routing
                    String destinationTopic = getHeaderMapping(routeHeader);
                    String targetTopic = (destinationTopic != null) ? destinationTopic : dlqTopic; // DLQ fallback

                    // Prepare the headers as a string
                    StringBuilder headersBuilder = new StringBuilder();
                    for (Header header : sinkRecord.headers()) {
                        headersBuilder.append(header.key()).append(":").append((String) header.value()).append(" ");
                    }

                    // Append the headers to the value
                    String valueWithHeaders = headersBuilder.toString().trim() + sinkRecord.value();

                    // Send record to the target topic
                    log.info("Routing record to topic: {} with value: {}", targetTopic, valueWithHeaders);
                    producer.send(new ProducerRecord<>(
                            targetTopic,
                            sinkRecord.key() != null ? sinkRecord.key().toString() : null,
                            valueWithHeaders
                    ));
                } catch (Exception e) {
                    log.error("Error processing record: {}", sinkRecord, e);
                }
            }
            producer.flush();
        } catch (Exception e) {
            log.error("Error initializing producer: ", e);
        }
    }
}
















