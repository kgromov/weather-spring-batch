package com.kgromov.batch;

import com.kgromov.domain.City;
import com.kgromov.dtos.DailyTemperatureDto;
import com.kgromov.dtos.TemperatureMeasurementsDto;
import com.kgromov.service.TemperatureExtractor;
import jakarta.annotation.PostConstruct;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.time.LocalDate;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static java.time.format.DateTimeFormatter.ISO_DATE;

@Slf4j
@RequiredArgsConstructor
public class TemperatureDatesReader extends AbstractItemCountingItemStreamItemReader<DailyTemperatureDto> {
    private City city;
    private Queue<LocalDate> datesQueue;
    private final TemperatureExtractor temperatureExtractor;

    public TemperatureDatesReader(City city, TemperatureExtractor temperatureExtractor) {
        this.city = city;
        this.temperatureExtractor = temperatureExtractor;
    }

    @PostConstruct
    private void setContextName() {
        this.setName(TemperatureDatesReader.class.getSimpleName());
    }

    @BeforeStep
    public void initQueue(StepExecution stepExecution) {
        log.info("Read before step execution");
        Set<LocalDate> datesToSync = (Set<LocalDate>) stepExecution.getJobExecution().getExecutionContext().get("datesToSync");
        this.datesQueue = new ArrayBlockingQueue<>(datesToSync.size(), true, datesToSync);
        log.info("Read datesToSync = {} before step execution", datesToSync);
    }

    @Override
    protected DailyTemperatureDto doRead() {
        log.info("Read daily temperature");
        if (datesQueue.isEmpty()) {
            return null;
        }
        LocalDate currentDate = datesQueue.poll();
        try {
            log.info("Thread {}: currentDate = {}", Thread.currentThread().getName(), currentDate.format(ISO_DATE));
            return temperatureExtractor.getTemperatureAt(city, currentDate).map(this::mapToDto)
                    .orElseThrow(() -> new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE)));
        } catch (Exception e) {
            throw new RuntimeException("Unable to get data for date = " + currentDate.format(ISO_DATE), e);
        }
    }

    @Override
    protected void doOpen() {

    }

    @Override
    protected void doClose() {

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
