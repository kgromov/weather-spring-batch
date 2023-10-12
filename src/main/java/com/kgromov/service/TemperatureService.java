package com.kgromov.service;

import com.kgromov.domain.City;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.dtos.TemperatureMeasurementsDto;
import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.Year;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class TemperatureService {
    private final DailyTemperatureRepository temperatureRepository;
    private final SinoptikExtractor sinoptikExtractor;

    public List<DailyTemperature> getTemperatureForYearsInCity(City city, int startYear, int endYear) {
        LocalDate startDate = Year.of(startYear).atDay(1);
        LocalDate endDate = Year.of(endYear).atDay(1);
        return getTemperatureForYearsInCity(city, startDate, endDate);
    }

    public List<DailyTemperature> getTemperatureForYearsInCity(City city, LocalDate startDate, LocalDate endDate) {
        List<TemperatureMeasurementsDto> temperatures = sinoptikExtractor.getTemperatureForRange(city, startDate, endDate);
        return temperatures.stream()
                .parallel()
                .map(DailyTemperature::new)
                .sorted(Comparator.comparing(DailyTemperature::getDate))
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public DailyTemperature getLatestDateTemperature() {
        return temperatureRepository.findLatestDateTemperature();
    }

    @Transactional
    public void saveTemperature(Collection<DailyTemperature> daysTemperature) {
        temperatureRepository.saveAll(daysTemperature);
    }

    @Transactional
    public void saveTemperature(DailyTemperature dailyTemperature) {
        temperatureRepository.save(dailyTemperature);
    }
}
