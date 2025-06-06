package fr.michaelchlon.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
    String bootstrapServers = "127.0.0.1:9092";
    String topic = "first-topic";

    // create consumer properties / configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // assign and seek are mostly used to replay data or fetch a specific message
    // seek to the beginning (earliest offset)
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
    consumer.assign(Arrays.asList(partitionToReadFrom));

    // seek to a specific offset
    long offsetToReadFrom = 5L;
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    int numberOfMessagesToRead = 5;
    boolean keepOnReading = true;
    int numberOfMessagesReadSoFar = 0;

    // pull for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar += 1;
        logger.info(
            "Key: "
                + record.key()
                + ", Value: "
                + record.value()
                + ", Partition: "
                + record.partition()
                + ", Offset: "
                + record.offset());

        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false; // exit the while loop
          break; // exit the for loop
        }
      }
    }
    logger.info("Exiting the application");
    consumer.close();
  }
}
