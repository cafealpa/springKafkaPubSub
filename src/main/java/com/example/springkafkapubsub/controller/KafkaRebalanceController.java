package com.example.springkafkapubsub.controller;

import com.example.springkafkapubsub.config.AdvancedKafkaConsumerAwareRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaRebalanceController {

    private final AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener;

    public KafkaRebalanceController(AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
    }

    @GetMapping("/rebalance/offsets")
    public Map<String, Long> getPartitionOffsets() {
        Map<TopicPartition, Long> offsetMap = rebalanceListener.getPartitionOffsetMap();
        Map<String, Long> result = new HashMap<>();
        
        // TopicPartition 객체를 문자열로 변환하여 반환
        for (Map.Entry<TopicPartition, Long> entry : offsetMap.entrySet()) {
            TopicPartition partition = entry.getKey();
            String key = partition.topic() + "-" + partition.partition();
            result.put(key, entry.getValue());
        }
        
        return result;
    }

    @GetMapping("/rebalance/stats")
    public Map<String, Integer> getRebalanceStats() {
        return rebalanceListener.getGroupRebalanceCount();
    }
}