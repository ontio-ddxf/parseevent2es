package com.ontology.service;

import com.ontology.utils.ConfigParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


//@Component
@Slf4j
public class DdxfReceiver {

    @Autowired
    private ConfigParam configParam;
    @Autowired
    private ProducerService producerService;

    private String topic = "topic-ddxf";

    @KafkaListener(topics = {"topic-block-event"}, groupId = "group-ddxf-parse")
    public void receiveMessage(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        String value = (String) record.value();
        producerService.parseAndSendOne(value,configParam.CONTRACT_HASH,topic);
        ack.acknowledge();
    }
}
