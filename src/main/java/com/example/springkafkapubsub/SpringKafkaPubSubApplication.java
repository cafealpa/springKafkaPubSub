package com.example.springkafkapubsub;

import com.example.kafkapubsub.avro.SUser;
import com.example.springkafkapubsub.pub.AvroProducerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class SpringKafkaPubSubApplication {
    // AvroReadConsumerService의 sendUser 메소드를 실행하는 메소드 작성
    private final AvroProducerService avroProducerService;
    private final KafkaListenerEndpointRegistry kafkaRegistry;

    public SpringKafkaPubSubApplication(AvroProducerService avroProducerService, KafkaListenerEndpointRegistry kafkaRegistry) {
        this.avroProducerService = avroProducerService;
        this.kafkaRegistry = kafkaRegistry;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaPubSubApplication.class, args);
    }



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

        avroProducerService.sendUser("SUser-avro", user);
        avroProducerService.sendUser("example-topic", user);

    }

    @EventListener(ApplicationReadyEvent.class)
    public void rebalanceRunner() throws InterruptedException {

            System.out.println("=====================================================");
            System.out.println("ApplicationRunner is running...");
            System.out.println("=====================================================");

            Thread.sleep(1000 * 60);

            System.out.println("강제 리밸런싱 시작!");

            MessageListenerContainer container = kafkaRegistry.getListenerContainer("myUserConsumer");

            if (container != null) {
                // enforceRebalance() 메서드를 호출합니다.
                // 이 메서드는 다음 poll() 작업 시 실제 리밸런싱이 발생하도록 Kafka Consumer에게 알립니다.
                container.enforceRebalance();
                System.out.println("enforceRebalance() 호출 완료. 다음 poll()에서 리밸런싱이 발생할 것입니다.");
            } else {
                System.out.println("ID 'myListenerId'를 가진 컨테이너를 찾을 수 없습니다.");
            }

    }



}

