package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @SpyBean
  private LibraryEventsConsumer libraryEventsConsumerSpy;

  @SpyBean
  private LibraryEventsService libraryEventsServiceSpy;

  @Autowired
  private LibraryEventsRepository libraryEventsRepository;

  @Autowired
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {

    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
    }
  }

  @AfterEach
  void tearDown() {
    libraryEventsRepository.deleteAll();
  }

  @Test
  void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

    String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(json).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();

    assert libraryEvents.size() == 1;

    libraryEvents.forEach(libraryEvent -> {
      assert libraryEvent.getLibraryEventId() != null;
      assertEquals(456, libraryEvent.getBook().getBookId());
    });
  }

  @Test
  void publishUpdatedLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {

    String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEventsRepository.save(libraryEvent);

    Book updatedBook = Book.builder().
        bookId(456).bookName("Kafka Using Spring Book 2.x").bookAuthor("Dilip").build();

    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEvent.setBook(updatedBook);
    String updatedJson = objectMapper.writeValueAsString(libraryEvent);

    kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

//    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
//    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();

    assertEquals("Kafka Using Spring Book 2.x", persistedLibraryEvent.getBook().getBookName());
  }

  @Test
  void publishModifyLibraryEvent_Invalid_libraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {

    Integer libraryEventId = 380;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456," +
        "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();


    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(libraryEventId);

    assertFalse(persistedLibraryEvent.isPresent());
  }

  @Test
  void publishModifyLibraryEvent_Null_libraryEventId() throws JsonProcessingException, InterruptedException,
      ExecutionException {

    Integer libraryEventId = null;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\"," +
        "\"book\":{\"bookId\":456," +
        "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();


    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
  }

  @Test
  void publishModifyLibraryEvent_0_libraryEventId() throws JsonProcessingException, InterruptedException,
      ExecutionException {

    Integer libraryEventId = 0;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\"," +
        "\"book\":{\"bookId\":456," +
        "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();


    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));
  }
}
