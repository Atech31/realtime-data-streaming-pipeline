package com.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryEvent {
    private String eventId;
    private String deviceId;
    private String eventType;
    private double metricValue;
    private LocalDateTime timestamp;
}
