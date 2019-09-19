package com.ddlab.kafka.producers;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TestProducer1 {

  private static final String TOPIC_NAME = "testTopic1";
  private static final String KAFKA_SERVER_HOST = "192.168.206.129";
  private static final String KAFKA_PORT = "9092";
  private static final String KAFKA_BOOTSTRAP = KAFKA_SERVER_HOST + ":" + KAFKA_PORT;

  private static Properties getKafkaProperties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return properties;
  }

  public static void main(String[] args) {
    Properties kafkaProp = getKafkaProperties();
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProp);

    try {
      for (int i = 0; i < 100; i++) {
        TimeUnit.SECONDS.sleep(1);
        String textString = "Vehicle started running at a speed of " + i + " km/hour";
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<String, String>(TOPIC_NAME, textString);
        Future<RecordMetadata> sendFuture = kafkaProducer.send(producerRecord);
        RecordMetadata recordData = sendFuture.get();
        System.out.println("Record Data : " + recordData);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      kafkaProducer.close();
    }
  }
}
