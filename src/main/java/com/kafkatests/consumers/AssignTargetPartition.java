package com.kafkatests.consumers;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class AssignTargetPartition {

    public static void main(String[] args)
    {
        //Creating Logger
        Logger log = LoggerFactory.getLogger(AssignTargetPartition.class.getName());

        //Consumer Application Configurations
        String bootstrapservers="somehost.com:9092";
        String topic="test";

        //Setting properties for consumer
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1800000");
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1800000000");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "11000");
        properties.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "50000000");
        
        //Create Kafka Consumer
        KafkaConsumer<String,String> kafkaConsumer= new KafkaConsumer<String,String>(properties);

        //Assigning the partition
        kafkaConsumer.assign(Collections.singleton(new TopicPartition(topic,0)));

        //Start reading messages from the assigned topic partition

        while(true)
        {
            log.info("Starting to poll");
            ConsumerRecords<String,String> records = kafkaConsumer.poll(400000);
            log.info("Poll finished..Polled : " + records.count() + " Records");
            for(ConsumerRecord<String,String> record : records)
            {
                //System.out.println("Topic: " + topic + " Partition: " + record.partition() + " has key of : " + record.key() + " Value of: " + record.value());
            }
        }
    }

}
