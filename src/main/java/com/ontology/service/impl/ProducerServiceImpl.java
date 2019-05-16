package com.ontology.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ontology.service.ProducerService;
import com.ontology.utils.Helper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerServiceImpl implements ProducerService {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Override
    public void parseAndSend(String value, String contractHash, String topic) {
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
                if (contractHash.equals(contractAddress)) {
                    kafkaTemplate.send(topic, event.toJSONString());
                    break;
                }
            }
        }
    }

}
