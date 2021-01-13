package com.kafkatests.producers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.*;
import java.util.Properties;

class ReadSocket{
    public static void main(String args[])throws Exception{

        //Creating a server socket listening on 3333
        ServerSocket ss=new ServerSocket(3333);
        Socket s=ss.accept();

        //Create producer configurations

        // Producer Configurations

        String BOOTSTRAP_SERVERS="somehost.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // Creating producer
        KafkaProducer producer = new KafkaProducer(properties);

        //Create Inputstream for reading the messages from the Socket
        DataInputStream din=new DataInputStream(s.getInputStream());

        //Placeholders for keeping the input strings from sockets
        String message="";

        //Reading indefinitely from the Server socket and wait for "stop" message from Client
        while(!message.equals("stop")){
            message = din.readUTF();
            String[] splits=message.split(",");
            String key=splits[0];
            String value=splits[1];
            ProducerRecord<String,String> record= new ProducerRecord<String,String>("source.replication-test-3",0,key,value);
            producer.send(record);
            System.out.println("Produced message: "+message +"To topic: source.replication-test-3");
        }

        //Closing all cleanly
        din.close();
        s.close();
        ss.close();
    }
}