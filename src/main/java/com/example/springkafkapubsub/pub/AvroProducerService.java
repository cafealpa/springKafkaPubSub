package com.example.springkafkapubsub.pub;

import com.example.kafkapubsub.avro.SUser;
import com.fasterxml.uuid.Generators;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class AvroProducerService {

    private final KafkaTemplate<String, SUser> kafkaTemplate;
//    private final String TOPIC = "SUser-avro";


    public AvroProducerService(KafkaTemplate<String, SUser> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void sendUser(String topicName, SUser user) {
        System.out.println("Sending User: " + user);
        // kafka로 user 데이터를 publishing. Key는 user객체의 Id를 사용하고 consumer group id를 설정

        // UUID7 생성
        String key = Generators.timeBasedEpochGenerator().generate().toString();

        kafkaTemplate.send(topicName, key, user);
    }

}
