package com.javaguy.springrabbitmq.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.javaguy.springrabbitmq.producer.ReliablePublisher;

@RestController
@RequestMapping("/api/reliable-orders")
public class ReliableOrderController {

    private final ReliablePublisher reliablePublisher;

    public ReliableOrderController(ReliablePublisher reliablePublisher) {
        this.reliablePublisher = reliablePublisher;
    }

    @PostMapping
    public ResponseEntity<String> publish(
            @RequestParam String key,
            @RequestParam String message) {
        reliablePublisher.publish(key, message);
        return ResponseEntity.ok("Published [" + key + "]: " + message);
    }
}
