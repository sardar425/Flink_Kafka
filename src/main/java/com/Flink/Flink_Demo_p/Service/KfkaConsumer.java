package com.Flink.Flink_Demo_p.Service;

import com.Flink.Flink_Demo_p.Dto.UserEntity;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KfkaConsumer {

    @KafkaListener(topics = "inputTopic",groupId = "OddTopic-id")
    public void consumer(UserEntity message){


        System.out.println(String.format(" message consumed ->%s", message.toString()));
    }



}
