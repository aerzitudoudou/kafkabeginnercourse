package course.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {
    public static void main(String[] args) {
        Logger logger  = LoggerFactory.getLogger(ProducerWithCallBack.class);
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
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "message " + i);
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
            });
        }

        //flush data
        producer.flush();
    }

}
