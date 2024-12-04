package com.Flink.Flink_Demo_p.Service;

import com.Flink.Flink_Demo_p.Dto.UserEntity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaPublisher {
    @Autowired
    private KafkaTemplate<String, Object> template;

    public void sendMessage(UserEntity message){
         template.send("inputTopic", message);
        System.out.println("message was sent");
    }



}
