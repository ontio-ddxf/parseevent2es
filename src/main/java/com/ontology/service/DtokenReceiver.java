package com.ontology.service;

import com.ontology.utils.ConfigParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class DtokenReceiver {

    @Autowired
    private ConfigParam configParam;
    @Autowired
    private ProducerService producerService;

    private String topic = "topic-dtoken";
    private String contractHash = "06633f64506fbf7fd4b65b422224905d362d1f55";

    @KafkaListener(topics = {"topic-test-event"}, groupId = "group-dtoken-parse")
    public void receiveMessage(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        log.info("dtokenreceiveMessage");
        String value = (String) record.value();
        producerService.parseAndSendOne(value,contractHash,topic);
        ack.acknowledge();
    }
}
