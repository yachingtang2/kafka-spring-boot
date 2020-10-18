package com.learnkafka.config;

import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

  @Autowired
  private LibraryEventsService libraryEventsService;

  @Bean
  ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> kafkaConsumerFactory) {

    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory);
    factory.setConcurrency(3);
//    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.setErrorHandler((thrownException, data) -> {
      log.info("Exception in LibraryEventsConsumerConfig and the record is {}", thrownException.getMessage(), data);
    });

    factory.setRetryTemplate(setUpRetryTemplate());
    factory.setRecoveryCallback(context -> {

      if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
        log.info("Inside the Recoverable logic!!!");
//        Arrays.asList(context.attributeNames())
//            .forEach(attributeName -> {
//              log.info("Attribute name is: {}", attributeName);
//              log.info("     Attribute is: {}", context.getAttribute(attributeName));
//            });

        ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");

        libraryEventsService.handleRecovery(consumerRecord);

      } else {
        log.info("Inside the NON Recoverable logic!!!");
        throw new RuntimeException(context.getLastThrowable().getMessage());
      }

      return null;
    });

    return factory;
  }

  private RetryTemplate setUpRetryTemplate() {

    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setBackOffPolicy(setUpFixedBackOffPolicy());
    retryTemplate.setRetryPolicy(setUpSimpleRetryPolicy());
    return retryTemplate;
  }

  private FixedBackOffPolicy setUpFixedBackOffPolicy() {

    FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
    fixedBackOffPolicy.setBackOffPeriod(1000);
    return fixedBackOffPolicy;
  }

  private SimpleRetryPolicy setUpSimpleRetryPolicy() {

    Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
    retryableExceptions.put(IllegalArgumentException.class, false);
    retryableExceptions.put(RecoverableDataAccessException.class, true);
    SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, retryableExceptions, true);
    simpleRetryPolicy.setMaxAttempts(3);
    return simpleRetryPolicy;
  }

}
