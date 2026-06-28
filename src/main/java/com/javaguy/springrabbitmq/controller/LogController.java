package com.javaguy.springrabbitmq.controller;

import com.javaguy.springrabbitmq.producer.LogPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/logs")
public class LogController {

    private final LogPublisher logPublisher;

    public LogController(LogPublisher logPublisher) {
        this.logPublisher = logPublisher;
    }

    @PostMapping
    public ResponseEntity<String> log(
            @RequestParam String level,
            @RequestParam String message) {
        logPublisher.log(level, message);
        return ResponseEntity.ok("Logged [" + level + "]: " + message);
    }
}
