package com.example.springkafkapubsub.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class KafkaConsumerAwareRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerAwareRebalanceListener.class);

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked before commit: {}", partitions);
        // 파티션이 회수되기 전, 커밋 전에 필요한 로직 구현
        // 예: 현재 상태 저장, 리소스 정리 준비 등
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked after commit: {}", partitions);
        // 파티션이 회수된 후, 커밋 후에 필요한 로직 구현
        // 예: 리소스 정리 완료 등
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned with consumer: {}", partitions);
        // 파티션이 할당될 때 필요한 로직 구현
        // Consumer 객체를 활용한 고급 작업 가능
        // 예: 특정 오프셋부터 소비 시작, 초기화 작업 등
        
        if (!partitions.isEmpty()) {
            logger.info("Consumer group: {}", consumer.groupMetadata().groupId());
            for (TopicPartition partition : partitions) {
                logger.info("Assigned partition: {} with beginning offset: {}", 
                    partition, consumer.beginningOffsets(partitions).get(partition));
            }
        }
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.info("Partitions lost with consumer: {}", partitions);
        // 파티션이 다른 컨슈머에게 강제로 재할당될 때 호출됨
        // Consumer 객체를 활용한 복구 작업 가능
    }
}