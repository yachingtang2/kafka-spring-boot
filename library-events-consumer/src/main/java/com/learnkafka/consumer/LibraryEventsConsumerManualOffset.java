package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

  @Override
  @KafkaListener(topics = {"library-events"}, groupId = "library-events-listener-group")
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {

    log.info("ConsumerRecord: {}", consumerRecord);
//    acknowledgment.acknowledge();
  }
}