package com.soits.utils;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public class testProducerMonitoring
{
    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);


        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("KD12", getRandomString()));
                Thread.sleep(100);
                producer.send(newRandomTransaction("NB11", getRandomString()));
                Thread.sleep(100);
                producer.send(newRandomTransaction("OK01", getRandomString()));
                Thread.sleep(100);
                i += 1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String code, String typeMeasuring)
    {
        // creates an empty json {}
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        float leftLimit = 1F;
        float rightLimit = 100F;
        float measuringGenerated = leftLimit + new Random().nextFloat() * (rightLimit - leftLimit);

        // Instant.now() is to get the current time using Java 8
        Instant now = Instant.now();

        // we write the data to the json document
        transaction.put("code", code);
        transaction.put("date_measuring", now.toString());
        transaction.put("type_measuring", typeMeasuring);
        transaction.put("measuring", measuringGenerated);

        return new ProducerRecord<>("measuring-test", code, transaction.toString());
    }


    static String getRandomString()
    {
        int r = (int) (Math.random()*7);
        String name = new String [] {"Ph","Tu","H1","Hr","Ta","T","Q"}[r];
        return name;
    }

}
