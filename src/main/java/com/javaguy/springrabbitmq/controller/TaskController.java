package com.javaguy.springrabbitmq.controller;

import com.javaguy.springrabbitmq.producer.TaskProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskProducer taskProducer;

    public TaskController(TaskProducer taskProducer) {
        this.taskProducer = taskProducer;
    }

    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam String task) {
        taskProducer.dispatch(task);
        return ResponseEntity.ok("Dispatched: " + task);
    }

    @PostMapping("/batch")
    public ResponseEntity<String> batch(@RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            String dots = ".".repeat((i % 5) + 1); // 1 to 5 seconds of work
            taskProducer.dispatch("Task-" + i + " " + dots);
        }
        return ResponseEntity.ok("Dispatched " + count + " tasks");
    }

}
