package com.kgromov.repository;

import com.kgromov.domain.DailyTemperatureDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.LocalDate;

public interface DailyTemperatureRepository extends MongoRepository<DailyTemperatureDocument, String> {

    default LocalDate getLatestDateTemperature() {
        return this.findFirstByOrderByDateDesc().getDate();
    }
    DailyTemperatureDocument findFirstByOrderByDateDesc();
}
