package com.example.springkafkapubsub.sub;

import com.example.kafkapubsub.avro.SUser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class AvroReadConsumerService {

    private final KafkaTemplate<String, SUser> kafkaTemplate;
    private final String TOPIC = "SUser-avro";
    private final String GROUP_ID = "My-SUser-read-group";

    public AvroReadConsumerService(KafkaTemplate<String, SUser> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = TOPIC, groupId = GROUP_ID)
    public void readUser(ConsumerRecord<String, SUser> recode) {
        String key = recode.key();
        SUser user = recode.value();

        System.out.println("key="+key);
        System.out.println("user = " + user);
    }



}
