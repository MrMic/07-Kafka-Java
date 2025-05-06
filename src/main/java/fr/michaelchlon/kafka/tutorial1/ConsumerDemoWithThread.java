package fr.michaelchlon.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private ConsumerDemoWithThread() {}

  private void run() {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first-topic";

    // Latch for dealing with multiple threads
    CountDownLatch latch = new CountDownLatch(1);

    // Create the consumer runnable
    logger.info("Creating the consumer thread");
    Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

    // Start the thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // Add a shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Caught shutdown hook");
                  ((ConsumerRunnable) myConsumerRunnable).shutdown();
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  logger.info("Application has exited");
                }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      // e.printStackTrace();
      logger.error("Application got interrupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }

  // * INFO: ══ THREAD ══════════════════════════════════════════════════════════
  public class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    // * INFO: ══ CONSTRUCTOR ═════════════════════════════════════════════════════
    public ConsumerRunnable(
        String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
      this.latch = latch;

      // create consumer properties / configs
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-sixth-application");
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);
      // subscribe to topic(s)
      consumer.subscribe(Arrays.asList(topic));
    }

    // ______________________________________________________________________
    @Override
    public void run() {

      // pull for new data
      while (true) {
        try {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            logger.info(
                "Key: "
                    + record.key()
                    + ", Value: "
                    + record.value()
                    + ", Partition: "
                    + record.partition()
                    + ", Offset: "
                    + record.offset());
          }
        } catch (WakeupException e) {
          logger.info("Receive shutdown signal!");
          break;
        } finally {
          consumer.close();
          // Tell our main code we're done with the consumer
          latch.countDown();
        }
      }
    }

    // ______________________________________________________________________
    public void shutdown() {
      // La méthode `consumer.wakeup()` est utilisée dans le contexte des
      // consommateurs Kafka. Elle permet de réveiller un consommateur qui est
      // actuellement en attente de messages.
      consumer.wakeup();
      latch.countDown();
    }
  }
}
