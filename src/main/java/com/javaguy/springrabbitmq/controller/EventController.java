package com.javaguy.springrabbitmq.controller;

import com.javaguy.springrabbitmq.producer.EventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventPublisher eventPublisher;

    public EventController(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam String event) {
        eventPublisher.publish(event);
        return ResponseEntity.ok("Broadcasted: " + event);
    }
}
