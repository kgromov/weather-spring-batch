package com.kgromov.batch;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TemperatureWriter implements ItemWriter<DailyTemperature> {
    private final DailyTemperatureRepository temperatureRepository;

    @Override
    public void write(Chunk<? extends DailyTemperature> chunk) throws Exception {
        temperatureRepository.saveAll(chunk.getItems());
    }
}
