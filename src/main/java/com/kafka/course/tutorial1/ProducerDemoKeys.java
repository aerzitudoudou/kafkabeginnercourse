package com.kafka.course.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger  = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "localhost:9092";
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create a producer record
        for(int i = 0; i < 10; i++){
            String topic = "first_topic";
            String value = "message" + i;
            String key = "id_" + i;
            logger.info("Key: " + key); //same key is guaranteed to always go to same partition
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            //send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                //this executes everytime a record being sent or exception is thrown
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        //the record was successfully sent
                        logger.info("Received new metadata. \n " +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing", e);

                    }

                }
            }).get(); //block the send to make it synchronous - don't do this in production
        }

        //flush data
        producer.flush();
    }
}
