package com.kgromov.repository;

import com.kgromov.domain.DailyTemperature;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.List;

public interface DailyTemperatureRepository extends JpaRepository<DailyTemperature, Long> {
    DailyTemperature findByDate(LocalDate date);
    List<DailyTemperature> findByDateBetween(LocalDate from, LocalDate to);
    @Query(
            value = "SELECT * FROM DayTemperature d " +
                    "WHERE DAY(d.date) = DAY(?1) AND MONTH(d.date) = MONTH(?1)",
            nativeQuery = true
    )
    List<DailyTemperature> findByDateNative(LocalDate date);

    @Query(
            value = "SELECT * FROM DayTemperature d " +
                    "WHERE DAY(d.date) = DAY(?1) AND MONTH(d.date) = MONTH(?1) " +
                    "ORDER BY d.date DESC " +
                    "LIMIT ?2",
            nativeQuery = true
    )
    List<DailyTemperature> findByDateInRange(LocalDate date, int years);

    @Query(
            value = "SELECT * FROM DayTemperature d " +
                    "ORDER BY d.date DESC " +
                    "LIMIT 1",
            nativeQuery = true
    )
    DailyTemperature findLatestDateTemperature();
}
