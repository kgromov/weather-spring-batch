package com.kgromov.batch;

import com.kgromov.domain.DailyTemperatureDocument;
import com.kgromov.dtos.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;

@RequiredArgsConstructor
public class WriteToMongoProcessor implements ItemProcessor<DailyTemperatureDto, DailyTemperatureDocument> {

    @Override
    public DailyTemperatureDocument process(DailyTemperatureDto dto) {
        return DailyTemperatureDocument.builder()
                .date(dto.getDate().plusDays(1))             // due to Mongo timezone diff
                .morningTemperature(dto.getMorningTemperature())
                .afternoonTemperature(dto.getAfternoonTemperature())
                .eveningTemperature(dto.getEveningTemperature())
                .nightTemperature(dto.getNightTemperature())
                .build();
    }
}
