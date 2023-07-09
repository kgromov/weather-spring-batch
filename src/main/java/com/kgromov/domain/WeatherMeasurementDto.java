package com.kgromov.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherMeasurementDto {
    private LocalTime time;
    private int temperature;
}
