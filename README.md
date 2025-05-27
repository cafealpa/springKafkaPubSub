# Spring Boot Kafka ConsumerAwareRebalanceListener 구현

이 프로젝트는 Spring Boot에서 Kafka의 `ConsumerAwareRebalanceListener`를 구현하는 방법을 보여줍니다. `ConsumerAwareRebalanceListener`는 기본 `ConsumerRebalanceListener`보다 더 많은 기능을 제공하며, Kafka Consumer 객체에 직접 접근할 수 있어 고급 리밸런싱 시나리오를 처리할 수 있습니다.

## 주요 구성 요소

### 1. 기본 ConsumerAwareRebalanceListener 구현

`KafkaConsumerAwareRebalanceListener` 클래스는 `ConsumerAwareRebalanceListener` 인터페이스의 기본 구현을 제공합니다:

```java
@Component
public class KafkaConsumerAwareRebalanceListener implements ConsumerAwareRebalanceListener {

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션이 회수되기 전, 커밋 전에 호출됨
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션이 회수된 후, 커밋 후에 호출됨
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션이 할당될 때 호출됨
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션이 다른 컨슈머에게 강제로 재할당될 때 호출됨
    }
}
```

### 2. 고급 ConsumerAwareRebalanceListener 구현

`AdvancedKafkaConsumerAwareRebalanceListener` 클래스는 오프셋 관리, 통계 수집 등의 고급 기능을 제공합니다:

```java
@Component
public class AdvancedKafkaConsumerAwareRebalanceListener implements ConsumerAwareRebalanceListener {
    
    // 파티션별 오프셋 정보를 저장하는 맵
    private final Map<TopicPartition, Long> partitionOffsetMap = new ConcurrentHashMap<>();
    
    // 컨슈머 그룹별 통계 정보
    private final Map<String, Integer> groupRebalanceCount = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 현재 오프셋 정보 저장
        // 리밸런스 통계 수집
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션 정보 로깅
        // 이전에 저장된 오프셋이 있으면 해당 오프셋부터 소비 시작
    }
    
    // 기타 메서드 및 통계 제공 메서드
}
```

### 3. Kafka 컨슈머 설정

`KafkaConsumerConfig` 클래스는 Kafka 리스너 컨테이너 팩토리를 설정하고 리밸런스 리스너를 등록합니다:

```java
@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private ConsumerFactory<String, Object> consumerFactory;
    
    @Autowired
    private AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setConsumerRebalanceListener(rebalanceListener);
        
        return factory;
    }
}
```

### 4. 컨슈머 서비스

`AvroRebalanceAwareConsumerService` 클래스는 리밸런스 리스너가 적용된 Kafka 컨슈머 서비스입니다:

```java
@Service
public class AvroRebalanceAwareConsumerService {

    @KafkaListener(
        topics = "${kafka.topic.example:example-topic}", 
        groupId = "${spring.kafka.consumer.group-id:rebalance-test-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(ConsumerRecord<String, SUser> record, Acknowledgment acknowledgment) {
        // 메시지 처리 로직
        // 수동 커밋
    }
}
```

### 5. 모니터링 API

`KafkaRebalanceController` 클래스는 리밸런스 정보를 모니터링하기 위한 REST API를 제공합니다:

```java
@RestController
@RequestMapping("/api/kafka")
public class KafkaRebalanceController {

    @Autowired
    private AdvancedKafkaConsumerAwareRebalanceListener rebalanceListener;

    @GetMapping("/rebalance/offsets")
    public Map<String, Long> getPartitionOffsets() {
        // 현재 저장된 오프셋 정보 반환
    }

    @GetMapping("/rebalance/stats")
    public Map<String, Integer> getRebalanceStats() {
        // 리밸런스 통계 정보 반환
    }
}
```

## ConsumerAwareRebalanceListener vs ConsumerRebalanceListener

### 주요 차이점

1. **Consumer 객체 접근**: `ConsumerAwareRebalanceListener`는 모든 메서드에서 Kafka Consumer 객체에 직접 접근할 수 있습니다.
2. **세분화된 이벤트**: 파티션 회수 이벤트가 커밋 전/후로 세분화되어 있습니다.
3. **고급 오프셋 관리**: Consumer 객체를 통해 오프셋을 직접 관리할 수 있습니다.
4. **더 많은 컨텍스트 정보**: Consumer 객체를 통해 그룹 ID, 메타데이터 등 더 많은 정보에 접근할 수 있습니다.

### 사용 시나리오

1. **정교한 오프셋 관리**: 특정 조건에 따라 오프셋을 저장하고 복구하는 경우
2. **리밸런스 모니터링**: 리밸런스 이벤트를 모니터링하고 통계를 수집하는 경우
3. **커스텀 상태 관리**: 파티션 할당 변경 시 상태 정보를 저장하고 복구하는 경우
4. **고급 에러 처리**: 리밸런스 중 발생하는 에러를 세밀하게 처리하는 경우

## 결론

`ConsumerAwareRebalanceListener`는 Spring Kafka에서 제공하는 확장된 리밸런스 리스너로, 기본 `ConsumerRebalanceListener`보다 더 많은 기능과 유연성을 제공합니다. 특히 Consumer 객체에 직접 접근할 수 있어 오프셋 관리, 상태 저장/복구, 모니터링 등의 고급 시나리오를 처리하는 데 유용합니다.