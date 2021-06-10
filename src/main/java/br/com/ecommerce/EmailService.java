package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) throws InterruptedException {
        var consumer = new KafkaConsumer<String,String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL")); //RARO escutar mais de um tópico

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                //return;
                for (var record : records) {
                    System.out.println("----------------------------------------------");
                    System.out.println("Sending email...");
                    System.out.println("KEY: " + record.key());
                    System.out.println("VALUE: " + record.value());
                    System.out.println("PARTITION: " + record.partition());
                    System.out.println("OFFSET: " + record.offset());
                    Thread.sleep(2000);
                    System.out.println("----------------------------------------------");
                    System.out.println("Processed!");
                }
            }
        }

    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName()); // #nome do grupo
        return properties;
    }
}
