package com.example.springkafkapubsub.sub;

import com.example.kafkapubsub.avro.SUser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class AvroRebalanceAwareConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(AvroRebalanceAwareConsumerService.class);
    
    private final String TOPIC = "SUser-rebalance-topic";
    private final String GROUP_ID = "rebalance-aware-group";

    @KafkaListener(
        topics = "${kafka.topic.example:example-topic}", 
        groupId = "${spring.kafka.consumer.group-id:rebalance-test-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(ConsumerRecord<String, SUser> record, Acknowledgment acknowledgment) {
        try {
            String key = record.key();
            SUser user = record.value();
            
            logger.info("Consumed message: key={}, value={}", key, user);
            logger.info("From partition: {}, offset: {}", record.partition(), record.offset());
            
            // 메시지 처리 로직
            
            // 수동 커밋
            acknowledgment.acknowledge();
            logger.info("Message acknowledged");
        } catch (Exception e) {
            logger.error("Error processing message", e);
            // 예외 처리 로직 (재시도, 데드 레터 큐 등)
        }
    }
}