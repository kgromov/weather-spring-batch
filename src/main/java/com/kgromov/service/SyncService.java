package com.kgromov.service;

import com.kgromov.domain.City;
import com.kgromov.domain.DailyTemperature;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.time.LocalDate;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_DATE;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncService {
    private final TemperatureService temperatureService;

    public void syncDailyTemperature() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("syncDailyTemperature");
        DailyTemperature latestDateTemperature = temperatureService.getLatestDateTemperature();
        LocalDate startDate = latestDateTemperature.getDate().plusDays(1);
        LocalDate endDate = LocalDate.now();
        List<DailyTemperature> temperatureForYearsInCity = temperatureService.getTemperatureForYearsInCity(City.ODESSA, startDate, endDate);
        temperatureService.saveTemperature(temperatureForYearsInCity);
        stopWatch.stop();
        log.info("Sync temperature for [{} to {}] is finished within {} ms",
                startDate.format(ISO_DATE), endDate.format(ISO_DATE), stopWatch.getLastTaskTimeMillis());
    }
}
