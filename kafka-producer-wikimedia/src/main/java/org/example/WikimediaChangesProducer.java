package org.example;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

  public static void main(String[] args) throws InterruptedException {

    String bootstrapServers = "127.0.0.1:9092";

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //set high throughput producer configs
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

    String topic = "wikimedia.recentchanges";
//    String topic = "test-topic";

    EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";
    EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
    EventSource eventSource = builder.build();

    // start the producer in another thread
    eventSource.start();

    // we produce for 10 minutes and block the program until then
    TimeUnit.MINUTES.sleep(10);

  }

}