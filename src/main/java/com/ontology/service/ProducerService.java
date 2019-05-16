package com.ontology.service;

public interface ProducerService {
    void parseAndSend(String value, String contractHash, String topic);
}
