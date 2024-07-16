package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperativeStrategy {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeStrategy.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "demo_java";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"Y2hlZXJmdWwtbWFtbWFsLTg2MjYkzN0UsSH3LyMs9u99Ub8Bb__F-994egst4cs\" " +
                        "password=\"MGUzY2VmMGEtOTUyZC00OGUzLWJlY2MtZGYyNWU4ZjUzMmM4\";");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // stragegy for partiton assignment
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // proerties.setProperty("group.instance.id", "...."); // stragey for static assignment

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }

//        try {
//
//            // subscribe consumer to our topic(s)
//            consumer.subscribe(Arrays.asList(topic));
//
//            // poll for new data
//            while (true) {
//                ConsumerRecords<String, String> records =
//                        consumer.poll(Duration.ofMillis(100));
//
//                for (ConsumerRecord<String, String> record : records) {
//                    log.info("Key: " + record.key() + ", Value: " + record.value());
//                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
//                }
//            }
//
//        } catch (WakeupException e) {
//            log.info("Wake up exception!");
//            // we ignore this as this is an expected exception when closing a consumer
//        } catch (Exception e) {
//            log.error("Unexpected exception", e);
//        } finally {
//            consumer.close(); // this will also commit the offsets if need be.
//            log.info("The consumer is now gracefully closed.");
//        }

    }
}
