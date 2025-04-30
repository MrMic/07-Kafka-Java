package fr.michaelchlon.kafka.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    String bootstrapServers = "127.0.0.1:9092";

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      // create a producer record
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>("first-topic", "hello world" + Integer.toString(i));

      // send data - asynchronous
      producer.send(
          record,
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
              // Executes every time a record is successfully sent or an exception is thrown
              if (exception == null) {
                // the record was successfully sent
                logger.info(
                    "Received new metadata. \n"
                        + "Topic:"
                        + recordMetadata.topic()
                        + "\n"
                        + "Partition:"
                        + recordMetadata.partition()
                        + "\n"
                        + "Offset:"
                        + recordMetadata.offset()
                        + "\n"
                        + "Timestamp:"
                        + recordMetadata.timestamp());
              } else {
                // the record failed to be sent
                logger.error("Error while producing", exception);
              }
            }
          });
    }
    // flush data
    producer.flush();

    // flush & close producer
    producer.close();
  }
}
