package com.kafkatests.producers;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SynchronousProducer {

    public static void main(String[] args) {


        // Producer Configurations
        String BOOTSTRAP_SERVERS="somehost.com:9092";
        int numberofrecords=100;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Creating producer
        KafkaProducer producer = new KafkaProducer(properties);

        //Create produce record

        RecordMetadata metadata;
        for(int i=1;i<=numberofrecords;i++)
        {
            i++;
            ProducerRecord<String,String> record= new ProducerRecord<String,String>("source.replication-test-3", "Message Number:"+ i);

            //Sending a record Synchronously
             //metadata = producer.send(record).get(1000,TimeUnit.MILLISECONDS);
            //System.out.println("Message written to Kafka with offset" + metadata.offset() + " Timestamp: " + metadata.timestamp() + " Partition : " + metadata.partition());
        }

    }
}