package com.kgromov.repository;

import com.kgromov.domain.DailyTemperatureDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public interface DailyTemperatureRepository extends MongoRepository<DailyTemperatureDocument, String> {

    default LocalDate getLatestDateTemperature() {
        return this.findFirstByOrderByDateDesc().getDate().toLocalDate();
    }
    DailyTemperatureDocument findFirstByOrderByDateDesc();

    List<DailyTemperatureDocument> findByDateGreaterThanOrderByDateAsc(LocalDateTime date);
}
