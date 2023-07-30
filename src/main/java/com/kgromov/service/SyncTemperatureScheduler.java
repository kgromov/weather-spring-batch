package com.kgromov.service;

import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureScheduler {
    private final JobLauncher jobLauncher;
    private final DailyTemperatureRepository temperatureRepository;
    private final Job syncTemperatureJob;

    @Scheduled(cron = "0 5 * * * *")
    public void perform() throws Exception {
        log.info("Schedule sync temperature job ...");
        LocalDate syncDate = temperatureRepository.getLatestDateTemperature();
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("syncStartDate", syncDate)
                .addLong("startedAt", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(syncTemperatureJob, jobParameters);
        log.info("Finish sync temperature job ...");
    }
}
