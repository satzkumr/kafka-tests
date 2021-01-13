package com.kafkatests.producers;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {

        // Producer Configurations

        String BOOTSTRAP_SERVERS="somehost.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // Creating producer
        KafkaProducer producer = new KafkaProducer(properties);

        //Create produce record

        Integer i=0;

        while(true)
        {
            i++;
            ProducerRecord<String,String> record= new ProducerRecord<String,String>("source.replication-test-3", i.toString());
            //Sending a record
            producer.send(record);

        }

    }
}
