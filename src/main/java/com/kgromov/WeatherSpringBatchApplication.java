package com.kgromov;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.repository.DailyTemperatureRepository;
import com.kgromov.service.TemperatureService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Sort;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
@SpringBootApplication
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }

    @Bean
    ApplicationRunner exportToMongo(JobLauncher jobLauncher, Job exportFromRdbmsToMongoJob) {
        return args -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(exportFromRdbmsToMongoJob, jobParameters);
        };
    }

    //    @Bean
    ApplicationRunner findMissedMeasurementDays(DailyTemperatureRepository temperatureRepository) {
        return args -> {
            List<LocalDate> dates = temperatureRepository.findAll(Sort.by(Sort.Direction.ASC, "date"))
                    .stream()
                    .map(DailyTemperature::getDate)
                    .toList();
            List<LocalDate> missedDates = new ArrayList<>();
            for (int i = 0; i < dates.size() - 1; i++) {
                LocalDate prev = dates.get(i);
                LocalDate next = dates.get(i + 1);
                long diffInDays = ChronoUnit.DAYS.between(prev, next);
                if (diffInDays > 1) {
                    List<LocalDate> iterMissed = IntStream.range(1, (int) diffInDays).boxed().map(prev::plusDays).toList();
                    missedDates.addAll(iterMissed);
                }
            }
            log.info("Missed measurements: {}", missedDates);
        };
    }

    //    @Bean
    ApplicationRunner syncRange(JobLauncher jobLauncher,
                                Job fetchTemperatureJob) {
        return args -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("syncStartDate", LocalDate.parse("2022-01-21"))
                    .addLocalDate("syncEndDate", LocalDate.now().minusDays(1))
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(fetchTemperatureJob, jobParameters);
        };
    }

    //    @Bean()
    ApplicationRunner addNextDays(JobLauncher jobLauncher,
                                  Job readFromMongoJob,
                                  TemperatureService temperatureService) {
        return args -> {
            LocalDate syncDate = temperatureService.getLatestDateTemperature().getDate();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("syncStartDate", syncDate)
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(readFromMongoJob, jobParameters);
        };
    }
}
