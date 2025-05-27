package com.example.springkafkapubsub.sub;

import com.example.kafkapubsub.avro.SUser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class AvroReadConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(AvroReadConsumerService.class);

    //    private final KafkaTemplate<String, SUser> kafkaTemplate;
    private final String TOPIC = "SUser-avro";
    private final String GROUP_ID = "My-SUser-read-group";

//    public AvroReadConsumerService(KafkaTemplate<String, SUser> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }

    @KafkaListener(id = "myUserConsumer", topics = TOPIC, groupId = GROUP_ID)
    public void readUser(ConsumerRecord<String, SUser> recode, Acknowledgment acknowledgment) {
        String key = recode.key();
        SUser user = recode.value();

        System.out.println("key=" + key);
        System.out.println("user = " + user);

        logger.info("Received message from topic '{}', partition {}, offset {}", recode.topic(), recode.partition(), recode.offset());
        logger.info("Key: {}, Value: {}", key, user);

        try {
            acknowledgment.acknowledge();
            logger.info("Offset {} committed for partition {}", recode.offset(), recode.partition());
        } catch (Exception e) {
            logger.error("Error committing offset {} for partition {}: {}", recode.offset(), recode.partition(), e.getMessage(), e);
        }

    }


}
