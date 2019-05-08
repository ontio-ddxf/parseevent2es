package com.ontology.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ontology.utils.ConfigParam;
import com.ontology.utils.Helper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;



@Component
@Slf4j
public class DdxfReceiver {

    @Autowired
    private ConfigParam configParam;
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = {"topic-events"},groupId = "group-ddxf-parse")
    public void receiveMessage(ConsumerRecord<?, ?> record) {
        log.info("ddxf-parse：{}", Thread.currentThread().getName());
        try {
            String value = (String) record.value();
            JSONObject data = JSONObject.parseObject(value);
            Object events = data.get("events");
            if (Helper.isEmptyOrNull(events)) {
                return;
            }

            JSONArray smartCodeEvent = (JSONArray) events;
            for (int j = 0; j < smartCodeEvent.size(); j++) {

                JSONObject event = smartCodeEvent.getJSONObject(j);
                JSONArray notifys = event.getJSONArray("Notify");

                for (int k = 0; k < notifys.size(); k++) {
                    JSONObject notify = notifys.getJSONObject(k);
                    String contractAddress = notify.getString("ContractAddress");
                    if (configParam.CONTRACT_HASH.equals(contractAddress)) {
                        kafkaTemplate.send("topic-ddxf",event.toJSONString());
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
