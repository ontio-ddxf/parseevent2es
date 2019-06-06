package com.ontology.service;

public interface ProducerService {
    void parseAndSend(String value, String contractHash, String topic);

    void parseAndSendOne(String value, String contract_hash, String topic);
}
