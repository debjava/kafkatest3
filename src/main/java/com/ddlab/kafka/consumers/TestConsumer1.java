package com.ddlab.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestConsumer1 {
    private static final String TOPIC_NAME = "testTopic1";
    private static final String KAFKA_SERVER_HOST = "192.168.206.129";
    private static final String KAFKA_PORT = "9092";
    private static final String KAFKA_BOOTSTRAP = KAFKA_SERVER_HOST + ":" + KAFKA_PORT;
    private static final String GROUP_ID = "my-test-group-id";

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BOOTSTRAP);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", GROUP_ID);//You can give any group id
        return properties;
    }

    public static void main(String[] args) {
        Properties kafkaProp = getKafkaProperties();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaProp);
        List<String> topics = new ArrayList<String>();
        topics.add(TOPIC_NAME);
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
