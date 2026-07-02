package com.javaguy.springrabbitmq.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.javaguy.springrabbitmq.producer.AppLogPublisher;

@RestController
@RequestMapping("/api/app-logs")
public class AppLogController {

    private final AppLogPublisher appLogPublisher;

    public AppLogController(AppLogPublisher appLogPublisher) {
        this.appLogPublisher = appLogPublisher;
    }

    @PostMapping
    public ResponseEntity<String> log(
            @RequestParam String key,
            @RequestParam String message) {
        appLogPublisher.publish(key, message);
        return ResponseEntity.ok("Published [" + key + "]: " + message);
    }
}
