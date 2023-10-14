package com.kgromov.dtos;

import com.kgromov.domain.PartOfTheDay;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
        double temperature = dailyMeasurements.stream()
                .filter(m -> m.getTime().compareTo(min) >= 0)
                .filter(m -> m.getTime().isBefore(max))
                .mapToInt(WeatherMeasurementDto::getTemperature)
                .average()
                .orElse(0.0);
        return BigDecimal.valueOf(temperature).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    @Override
    public String toString() {
        return "TemperatureMeasurementsDto{" +
                "date=" + date +
                ", morning=" + BigDecimal.valueOf(this.getMorningTemperature()).setScale(2, RoundingMode.HALF_UP) +
                ", afternoon=" + BigDecimal.valueOf(this.getAfternoonTemperature()).setScale(2, RoundingMode.HALF_UP) +
                ", evening=" + BigDecimal.valueOf(this.getEveningTemperature()).setScale(2, RoundingMode.HALF_UP)  +
                ", night=" + BigDecimal.valueOf(this.getNightTemperature()).setScale(2, RoundingMode.HALF_UP)  +
                '}';
    }
}
