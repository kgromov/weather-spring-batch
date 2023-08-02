package com.kgromov.batch;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class TemperatureWriter implements ItemWriter<DailyTemperature> {
    private final DailyTemperatureRepository temperatureRepository;

    @Override
    public void write(Chunk<? extends DailyTemperature> chunk) {
        log.info("{}: write temperature of chunk of size = {}", Thread.currentThread().getName(), chunk.getItems().size());
        temperatureRepository.saveAll(chunk.getItems());
    }
}
