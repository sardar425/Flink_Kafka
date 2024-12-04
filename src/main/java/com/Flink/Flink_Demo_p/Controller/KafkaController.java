package com.Flink.Flink_Demo_p.Controller;


import com.Flink.Flink_Demo_p.Dto.UserEntity;
import com.Flink.Flink_Demo_p.Service.KafkaPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer-app")
public class KafkaController {

    @Autowired
    private KafkaPublisher publisher;

    @PostMapping("/publish")
    public ResponseEntity<?> publishMessage(@RequestBody UserEntity message){
        publisher.sendMessage(message);
        return ResponseEntity.ok("sent controller");
    }
}
