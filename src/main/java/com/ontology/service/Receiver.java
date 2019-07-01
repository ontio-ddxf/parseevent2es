package com.ontology.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ontology.utils.ElasticsearchUtil;
import com.ontology.utils.Helper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


import java.util.*;

@Component
@Slf4j
public class Receiver {
    private String indexName = "event_index";
    private String esType = "events";

//    @KafkaListener(topics = {"topic-block-event"}, groupId = "group-common-parse")
//    public void receiveMessage(ConsumerRecord<?, ?> record, Acknowledgment ack) {
//        log.info("common-parse：{}", Thread.currentThread().getName());
//        try {
//            String value = (String) record.value();
//            JSONObject data = JSONObject.parseObject(value);
//            int i = (int) data.get("height");
//            log.info("height:{}", i);
//
//            Object events = data.get("events");
//            if (!Helper.isEmptyOrNull(events)) {
//                JSONArray smartCodeEvent = (JSONArray) events;
//                for (int j = 0; j < smartCodeEvent.size(); j++) {
//                    JSONObject event = smartCodeEvent.getJSONObject(j);
//                    String txHash = event.getString("TxHash");
//                    JSONArray notifys = event.getJSONArray("Notify");
//
//                    for (int k = 0; k < notifys.size(); k++) {
//                        Map<String, Object> map = new LinkedHashMap<>();
//                        JSONObject notify = notifys.getJSONObject(k);
//                        String contractAddress = notify.getString("ContractAddress");
//
//                        map.put("txHash", txHash);
//                        map.put("contractAddress", contractAddress);
//                        map.put("blockHeight", i);
//                        map.put("event", event.toJSONString());
//
//                        Object statesObj = notify.get("States");
//                        if (statesObj instanceof String) {
//                            map.put("eventParam0", statesObj);
//
//                        } else {
//                            JSONArray states = (JSONArray) statesObj;
//                            for (int n = 0; n < states.size(); n++) {
//                                Object param = states.get(n);
//                                if (param instanceof String) {
//                                    map.put("eventParam" + n, param);
//                                } else {
//                                    map.put("eventParam" + n, JSON.toJSONString(param));
//                                }
//                            }
//                        }
//                        ElasticsearchUtil.addData(map, indexName, esType);
//                    }
//                }
//            }
//            ack.acknowledge();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    @KafkaListener(topics = {"topic-test-event"}, groupId = "group-common-parse")
    public void receiveOneByOne(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        log.info("common-parse：{}", Thread.currentThread().getName());
        try {
            String value = (String) record.value();
            JSONObject data = JSONObject.parseObject(value);
            int i = (int) data.get("height");
            log.info("height:{}", i);

            JSONObject events = JSONObject.parseObject(data.getString("events"));
            if (!Helper.isEmptyOrNull(events)) {
                String txHash = events.getString("TxHash");
                JSONArray notifys = events.getJSONArray("Notify");

                for (int k = 0; k < notifys.size(); k++) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    JSONObject notify = notifys.getJSONObject(k);
                    String contractAddress = notify.getString("ContractAddress");

                    map.put("txHash", txHash);
                    map.put("contractAddress", contractAddress);
                    map.put("blockHeight", i);
                    map.put("event", events.toJSONString());

                    Object statesObj = notify.get("States");
                    if (statesObj instanceof String) {
                        map.put("eventParam0", statesObj);

                    } else {
                        JSONArray states = (JSONArray) statesObj;
                        for (int n = 0; n < states.size(); n++) {
                            Object param = states.get(n);
                            if (param instanceof String) {
                                map.put("eventParam" + n, param);
                            } else {
                                map.put("eventParam" + n, JSON.toJSONString(param));
                            }
                        }
                    }
                    ElasticsearchUtil.addData(map, indexName, esType);
                }
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("catch exception:",e);
        }
    }
}
