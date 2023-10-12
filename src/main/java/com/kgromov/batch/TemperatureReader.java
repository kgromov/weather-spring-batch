package com.kgromov.batch;

import com.kgromov.domain.City;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.service.TemperatureExtractor;
import jakarta.annotation.PostConstruct;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.time.format.DateTimeFormatter.ISO_DATE;

@Slf4j
@RequiredArgsConstructor
public class TemperatureReader extends AbstractItemCountingItemStreamItemReader<DailyTemperature> {
    private City city;
    private LocalDate startDate;
    private ThreadLocal<LocalDate> currentDate = new ThreadLocal<>();
    private LocalDate endDate = LocalDate.now();
    private final TemperatureExtractor temperatureExtractor;
    private AtomicLong daysOffset = new AtomicLong();

    @Builder
    private TemperatureReader(City city, LocalDate startDate, LocalDate endDate, TemperatureExtractor temperatureExtractor) {
        this.city = city;
        this.startDate = startDate;
        this.endDate = endDate;
        this.temperatureExtractor = temperatureExtractor;
    }

    @PostConstruct
    private void setContextName() {
        this.setName(TemperatureReader.class.getSimpleName());
    }

    @Override
    protected DailyTemperature doRead() {
        log.info("Read daily temperature");
        if (city == null || startDate == null || endDate == null || startDate.isAfter(endDate)) {
            return null;
        }
        synchronized (DailyTemperature.class) {
            LocalDate currentDate = startDate.plusDays(daysOffset.get());
            this.currentDate.set(currentDate);
            daysOffset.getAndIncrement();
        }
        if (currentDate.get().isAfter(endDate)) {
            return null;
        }
        DailyTemperature temperature;
        try {
            LocalDate currentDate = this.currentDate.get();
            temperature = temperatureExtractor.getTemperatureAt(city, currentDate).map(DailyTemperature::new)
                    .orElseThrow(getRuntimeExceptionSupplier(currentDate));
            log.info("Thread {}: currentItemCount = {}, daysOffset = {}, currentDate = {}, startDate = {}",
                    Thread.currentThread().getName(), getCurrentItemCount(), daysOffset, currentDate.format(ISO_DATE), startDate.format(ISO_DATE));
        } catch (Exception e) {
            throw getRuntimeExceptionSupplier(currentDate.get(), e).get();
        }
        return temperature;
    }

    @BeforeStep
    public void saveStartDate(StepExecution stepExecution) {
        log.info("Save syncStartDate = {} before step execution", startDate);
        stepExecution.getJobExecution().getExecutionContext().put("syncStartDate", startDate);
    }

    @Override
    protected void doOpen() throws Exception {

    }

    @Override
    protected void doClose() throws Exception {

    }

    private static Supplier<RuntimeException> getRuntimeExceptionSupplier(LocalDate currentDate) {
        return () -> new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE));
    }

    private static Supplier<RuntimeException> getRuntimeExceptionSupplier(LocalDate currentDate, Throwable throwable) {
        return () -> new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE), throwable);
    }
}
