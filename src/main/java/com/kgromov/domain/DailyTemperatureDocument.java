package com.kgromov.domain;

import lombok.*;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.stream.DoubleStream;

@Document(collection = "weather_archive")
@Data
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(of = {"_id", "date"})
@ToString(exclude = {"_id"})
@Builder
public class DailyTemperatureDocument implements Serializable {
    @Serial
    private static final long serialVersionUID = -5976257794245662775L;
    @Id
    private ObjectId _id;
    private LocalDateTime date;
    private Double morningTemperature;
    private Double afternoonTemperature;
    private Double eveningTemperature;
    private Double nightTemperature;

    public Double getMax() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, /*eveningTemperature,*/ nightTemperature)
                .max()
                .getAsDouble();
    }

    public Double getMin() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, /*eveningTemperature, */nightTemperature)
                .min()
                .getAsDouble();
    }

    public Double getAverage() {
        return DoubleStream.of(morningTemperature, afternoonTemperature, /*eveningTemperature, */nightTemperature)
                .average()
                .getAsDouble();
    }
}
