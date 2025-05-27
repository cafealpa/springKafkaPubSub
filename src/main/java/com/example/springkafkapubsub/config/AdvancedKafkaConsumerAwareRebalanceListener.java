package com.example.springkafkapubsub.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AdvancedKafkaConsumerAwareRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedKafkaConsumerAwareRebalanceListener.class);

    // 파티션별 오프셋 정보를 저장하는 맵
    private final Map<TopicPartition, Long> partitionOffsetMap = new ConcurrentHashMap<>();

    // 컨슈머 그룹별 통계 정보
    private final Map<String, Integer> groupRebalanceCount = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String groupId = consumer.groupMetadata().groupId();
        logger.info("Partitions revoked before commit for group {}: {}", groupId, partitions);

        // 현재 오프셋 정보 저장
        if (!partitions.isEmpty()) {
            for (TopicPartition partition : partitions) {
                try {
                    long position = consumer.position(partition);
                    partitionOffsetMap.put(partition, position);
                    logger.info("Saved offset for partition {}: {}", partition, position);
                } catch (Exception e) {
                    logger.error("Error getting position for partition {}: {}", partition, e.getMessage());
                }
            }
        }

        // 리밸런스 카운트 증가
        groupRebalanceCount.compute(groupId, (k, v) -> (v == null) ? 1 : v + 1);
        logger.info("Group {} has been rebalanced {} times", groupId, groupRebalanceCount.get(groupId));
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked after commit: {}", partitions);

        // 리소스 정리 로직
        for (TopicPartition partition : partitions) {
            logger.info("Cleaning up resources for partition: {}", partition);
            // 실제 리소스 정리 로직 구현
        }
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String groupId = consumer.groupMetadata().groupId();
        logger.info("Partitions assigned to consumer in group {}: {}", groupId, partitions);

        // 파티션 할당 정보 로깅
        if (!partitions.isEmpty()) {
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            for (TopicPartition partition : partitions) {
                long beginOffset = beginningOffsets.get(partition);
                long endOffset = endOffsets.get(partition);
                long lag = endOffset - beginOffset;

                logger.info("Partition {} - beginning offset: {}, end offset: {}, lag: {}", 
                    partition, beginOffset, endOffset, lag);

                // 이전에 저장된 오프셋이 있으면 해당 오프셋부터 소비 시작
                if (partitionOffsetMap.containsKey(partition)) {
                    long savedOffset = partitionOffsetMap.get(partition);
                    if (savedOffset > beginOffset && savedOffset <= endOffset) {
                        logger.info("Seeking to saved offset {} for partition {}", savedOffset, partition);
                        consumer.seek(partition, savedOffset);
                    } else {
                        logger.warn("Saved offset {} for partition {} is out of range [{}, {}]", 
                            savedOffset, partition, beginOffset, endOffset);
                    }
                }
            }
        }
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        logger.warn("Partitions lost: {}", partitions);

        // 파티션 손실 처리 로직
        for (TopicPartition partition : partitions) {
            logger.warn("Lost partition: {}", partition);
            // 파티션 손실에 대한 복구 로직 구현
            partitionOffsetMap.remove(partition);
        }
    }

    /**
     * 현재 저장된 오프셋 정보를 반환
     */
    public Map<TopicPartition, Long> getPartitionOffsetMap() {
        return new HashMap<>(partitionOffsetMap);
    }

    /**
     * 컨슈머 그룹별 리밸런스 횟수 반환
     */
    public Map<String, Integer> getGroupRebalanceCount() {
        return new HashMap<>(groupRebalanceCount);
    }
}
