package com.kgromov.batch;

import com.kgromov.domain.City;
import com.kgromov.dtos.DailyTemperatureDto;
import com.kgromov.dtos.TemperatureMeasurementsDto;
import com.kgromov.service.TemperatureExtractor;
import jakarta.annotation.PostConstruct;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.time.format.DateTimeFormatter.ISO_DATE;

@Slf4j
@RequiredArgsConstructor
public class TemperatureReader extends AbstractItemCountingItemStreamItemReader<DailyTemperatureDto> {
    private City city;
    private LocalDate startDate;
    private ThreadLocal<LocalDate> currentDate = new ThreadLocal<>();
    private LocalDate endDate;
    private final TemperatureExtractor temperatureExtractor;
    private final AtomicLong daysOffset = new AtomicLong();

    @Builder
    private TemperatureReader(City city, LocalDate startDate, LocalDate endDate, TemperatureExtractor temperatureExtractor) {
        this.city = city;
        this.startDate = startDate;
        this.temperatureExtractor = temperatureExtractor;
    }

    @PostConstruct
    private void setContextName() {
        this.setName(TemperatureReader.class.getSimpleName());
    }

    @Override
    protected DailyTemperatureDto doRead() {
        log.info("Read daily temperature");
        if (city == null || startDate == null || endDate == null || !endDate.isAfter(startDate)) {
            return null;
        }
        synchronized (DailyTemperatureDto.class) {
            LocalDate currentDate = startDate.plusDays(daysOffset.get());
            this.currentDate.set(currentDate);
            daysOffset.getAndIncrement();
        }
        if (currentDate.get().isAfter(endDate)) {
            return null;
        }
        DailyTemperatureDto temperature;
        try {
            LocalDate currentDate = this.currentDate.get();
            temperature = temperatureExtractor.getTemperatureAt(city, currentDate).map(this::mapToDto)
                    .orElseThrow(getRuntimeExceptionSupplier(currentDate));
            log.info("Thread {}: currentItemCount = {}, daysOffset = {}, currentDate = {}, startDate = {}",
                    Thread.currentThread().getName(), getCurrentItemCount(), daysOffset, currentDate.format(ISO_DATE), startDate.format(ISO_DATE));
        } catch (Exception e) {
            throw getRuntimeExceptionSupplier(currentDate.get(), e).get();
        }
        return temperature;
    }

    @Override
    protected void doOpen() {

    }

    @Override
    protected void doClose() {

    }

    private static Supplier<RuntimeException> getRuntimeExceptionSupplier(LocalDate currentDate) {
        return () -> new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE));
    }

    private static Supplier<RuntimeException> getRuntimeExceptionSupplier(LocalDate currentDate, Throwable throwable) {
        return () -> new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE), throwable);
    }

    private DailyTemperatureDto mapToDto(TemperatureMeasurementsDto measurementsDto) {
        return DailyTemperatureDto.builder()
                .date(measurementsDto.getDate())
                .morningTemperature(measurementsDto.getMorningTemperature())
                .afternoonTemperature(measurementsDto.getAfternoonTemperature())
                .eveningTemperature(measurementsDto.getEveningTemperature())
                .nightTemperature(measurementsDto.getNightTemperature())
                .build();
    }
}
