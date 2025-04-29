package fr.michaelchlon.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
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
      String topic = "first-topic";
      String value = "Hello World" + Integer.toString(i);
      String key = "id_" + Integer.toString(i);

      logger.info("Key: " + key);

      // create a producer record
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      // send data - INFO: asynchronous
      producer
          .send(
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
              })
          .get(); // blocked the .send() call to make it synchronous - don't do this in production
    }
    // flush data
    producer.flush();

    // flush & close producer
    producer.close();
  }
}
