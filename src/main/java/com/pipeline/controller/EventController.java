package com.pipeline.controller;

import com.pipeline.model.TelemetryEvent;
import com.pipeline.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventController {

    private final KafkaProducerService producerService;

    @PostMapping("/emit")
    public ResponseEntity<TelemetryEvent> emitEvent(@RequestBody TelemetryEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(LocalDateTime.now());
        }
        producerService.sendEvent(event);
        return ResponseEntity.ok(event);
    }
}
