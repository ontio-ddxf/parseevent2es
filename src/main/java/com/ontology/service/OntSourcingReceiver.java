package com.ontology.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class OntSourcingReceiver {

    @Autowired
    private ProducerService producerService;
    private String contractHash = "e2510ed1044503faf6e3e66b98372606bbeae38f";
    private String topic = "ont_sourcing_2c_e2510e";

    @KafkaListener(topics = {"topic-all-events"}, groupId = "ont_sourcing_2c_e2510e")
    public void receiveMessage(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        log.info("ont_sourcing_2c_e2510e：{}", Thread.currentThread().getName());
        String value = (String) record.value();
        producerService.parseAndSend(value,contractHash,topic);
        ack.acknowledge();
    }
}
