package com.kgromov.service;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.dtos.DailyTemperatureDto;
import org.springframework.stereotype.Component;

// TODO: replace with MapStruct
@Component
public class DailyTemperatureMapper {

    public DailyTemperature mapToEntity(DailyTemperatureDto dto) {
        return DailyTemperature.builder()
                .date(dto.getDate())
                .morningTemperature(dto.getMorningTemperature())
                .afternoonTemperature(dto.getAfternoonTemperature())
                .eveningTemperature(dto.getEveningTemperature())
                .nightTemperature(dto.getNightTemperature())
                .build();
    }

    public DailyTemperatureDto mapToDto(DailyTemperature entity) {
        return DailyTemperatureDto.builder()
                .id(entity.getId())
                .date(entity.getDate())
                .morningTemperature(entity.getMorningTemperature())
                .afternoonTemperature(entity.getAfternoonTemperature())
                .eveningTemperature(entity.getEveningTemperature())
                .nightTemperature(entity.getNightTemperature())
                .build();
    }
}
