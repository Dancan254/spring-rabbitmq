package com.javaguy.springrabbitmq.controller;

import com.javaguy.springrabbitmq.producer.HelloProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class HelloController {

    private final HelloProducer producer;

    public HelloController(HelloProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam String message) {
        producer.send(message);
        return ResponseEntity.ok("Message sent: " + message);
    }
}
