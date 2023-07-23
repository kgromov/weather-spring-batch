package com.kgromov;

import com.kgromov.service.TemperatureService;
import jakarta.persistence.Persistence;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

@SpringBootApplication
//@EnableBatchProcessing
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }

    @Bean
    ApplicationRunner applicationRunner(JobLauncher jobLauncher,
                                        Job fetchTemperatureJob,
                                        Job syncTemperatureJob,
                                        TemperatureService temperatureService) {
        return args -> {
//            Persistence.createEntityManagerFactory().createEntityManager()
            LocalDate syncDate = temperatureService.getLatestDateTemperature().getDate();
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("syncStartDate", syncDate)
                    .toJobParameters();
//            jobLauncher.run(fetchTemperatureJob, jobParameters);
            jobLauncher.run(syncTemperatureJob, jobParameters);

        };
    }
}
