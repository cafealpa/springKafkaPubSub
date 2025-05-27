package com.example.springkafkapubsub;

import com.example.kafkapubsub.avro.SUser;
import com.example.springkafkapubsub.pub.AvroProducerService;
import com.example.springkafkapubsub.sub.AvroReadConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class SpringKafkaPubSubApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaPubSubApplication.class, args);
    }

    // AvroReadConsumerService의 sendUser 메소드를 실행하는 메소드 작성
    @Autowired
    private AvroProducerService avroProducerService;

    @Scheduled(fixedDelay = 30, timeUnit = TimeUnit.SECONDS)
    public void sendUser() {
        // Given
//        String userId = "user1235";
        long timestamp = System.currentTimeMillis();
        String userId = "userId" + timestamp;

        String userName = "John Doe" + timestamp;
        String userEmail = "john.doe@example.com";

        SUser user = new SUser();
        user.setId(userId);
        user.setName(userName);
        user.setEmail(userEmail);

        avroProducerService.sendUser(user);

    }



}

