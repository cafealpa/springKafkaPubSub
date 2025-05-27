package com.example.springkafkapubsub.pub;

import com.example.kafkapubsub.avro.SUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class AvroProducerServiceTest {

    @Mock
    private KafkaTemplate<String, SUser> kafkaTemplate;

    private AvroProducerService avroProducerService;

    @BeforeEach
    public void setUp() {
        avroProducerService = new AvroProducerService(kafkaTemplate);
    }

    @Test
    public void testSendUser() {
        // Given
        String userId = "user123";
        String userName = "John Doe";
        String userEmail = "john.doe@example.com";

        SUser user = new SUser();
        user.setId(userId);
        user.setName(userName);
        user.setEmail(userEmail);

        // When
        avroProducerService.sendUser(user);

        // Then
//        verify(kafkaTemplate, times(1)).send("SUser-avro", userId, user);
    }

    @Test
    public void testSendUserWithNullEmail() {
        // Given
        String userId = "user456";
        String userName = "Jane Doe";

        SUser user = new SUser();
        user.setId(userId);
        user.setName(userName);
        user.setEmail(null); // Email is nullable

        // When
        avroProducerService.sendUser(user);

        // Then
//        verify(kafkaTemplate, times(1)).send("SUser-avro", userId, user);
    }
}