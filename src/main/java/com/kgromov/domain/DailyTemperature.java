package com.kgromov.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "DayTemperature_batch")
@Data
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(of = {"id", "date"})
@Builder
public class DailyTemperature {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @JoinColumn(unique = true)
    private LocalDate date;
    @Column(name = "morningTemperature")
    private Double morningTemperature;
    @Column(name = "afternoonTemperature")
    private Double afternoonTemperature;
    @Column(name = "eveningTemperature")
    private Double eveningTemperature;
    @Column(name = "nightTemperature")
    private Double nightTemperature;

    public DailyTemperature(TemperatureMeasurementsDto temperatureMeasurementsDto) {
        this.date = temperatureMeasurementsDto.getDate();
        this.morningTemperature = temperatureMeasurementsDto.getMorningTemperature();
        this.afternoonTemperature = temperatureMeasurementsDto.getAfternoonTemperature();
        this.eveningTemperature = temperatureMeasurementsDto.getEveningTemperature();
        this.nightTemperature = temperatureMeasurementsDto.getNightTemperature();
    }
}
