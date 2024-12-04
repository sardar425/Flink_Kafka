package com.Flink.Flink_Demo_p.Config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class kafkaConfig {

   @Bean
    public NewTopic topic(){
        return TopicBuilder.name("inputTopic")
                .build();
    }
}
