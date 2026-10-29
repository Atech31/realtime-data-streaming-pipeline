package com.pipeline.service;

import com.pipeline.model.TelemetryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private static final String TOPIC = "telemetry-events";

    public void sendEvent(TelemetryEvent event) {
        kafkaTemplate.send(TOPIC, event.getDeviceId(), event);
        log.info("Produced event ID: {} for device: {}", event.getEventId(), event.getDeviceId());
    }
}
