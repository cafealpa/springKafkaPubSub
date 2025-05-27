package com.example.springkafkapubsub.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConsumerConfig {


    private final ConsumerFactory<String, Object> consumerFactory;
    private final AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener;

    public KafkaConsumerConfig(ConsumerFactory<String, Object> consumerFactory, AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener) {
        this.consumerFactory = consumerFactory;
        this.rebalanceListener = rebalanceListener;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 컨테이너 속성 설정
        ContainerProperties containerProperties = factory.getContainerProperties();

        // ConsumerAwareRebalanceListener 설정
        containerProperties.setConsumerRebalanceListener(rebalanceListener);

        // AckMode는 application.yml에 이미 설정되어 있음 (manual_immediate)

        return factory;
    }
}
