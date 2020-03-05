package com.mq.rabbitmq.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestTaskController {
    @GetMapping("/hello")
    public ResponseEntity hello() {
        return ResponseEntity.ok("666666666666666");
    }
}
