package com.kgromov.domain;

import lombok.*;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.util.stream.DoubleStream;

@Document
@Data
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(of = {"_id", "date"})
@Builder
public class DailyTemperatureDocument {
    @Id
    private ObjectId _id;
    private LocalDate date;
    private Double morningTemperature;
    private Double afternoonTemperature;
    private Double eveningTemperature;
    private Double nightTemperature;

    public Double getMax() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature)
                .max()
                .getAsDouble();
    }

    public Double getMin() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature)
                .min()
                .getAsDouble();
    }

    public Double getAverage() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature)
                .average()
                .getAsDouble();
    }
}
