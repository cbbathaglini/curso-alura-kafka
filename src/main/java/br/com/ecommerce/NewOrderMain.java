package br.com.ecommerce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //<chave, mensagem>
        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,456,52.89";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER",value,value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso no envio! " + data.topic() + " - partition:" + data.partition() + " | offset: " + data.offset() + " | timestamp: " + data.timestamp());

        };
        producer.send(record, callback).get();//assíncrono

        var email = "Welcome! We're processing your order! ";
        var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL",email,email);
        producer.send(emailRecord,callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
