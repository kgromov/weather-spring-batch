package com.kgromov.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

import static com.kgromov.domain.PartOfTheDay.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureMeasurementsDto {
    private LocalDate date;
    private List<WeatherMeasurementDto> dailyMeasurements;

    public double getMorningTemperature() {
        return getAvgTemperatureAtDayPart(MORNING);
    }

    public double getAfternoonTemperature() {
        return getAvgTemperatureAtDayPart(AFTERNOON);
    }

    public double getEveningTemperature() {
        return getAvgTemperatureAtDayPart(EVENING);
    }

    public double getNightTemperature() {
        return getAvgTemperatureAtDayPart(NIGHT);
    }

    private double getAvgTemperatureAtDayPart(PartOfTheDay partOfTheDay) {
        LocalTime min = partOfTheDay.getStart();
        LocalTime max = partOfTheDay.getEnd();
        return dailyMeasurements.stream()
                .filter(m -> m.getTime().compareTo(min) >= 0)
                .filter(m -> m.getTime().isBefore(max))
                .mapToInt(WeatherMeasurementDto::getTemperature)
                .average()
                .orElse(0.0);
    }
}
