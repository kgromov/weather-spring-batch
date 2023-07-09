package com.kgromov;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SpringBootApplication
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }

    @Bean
    ApplicationRunner applicationRunner(JobLauncher jobLauncher, Job fetchTemperatureJob) {
        return args -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("startAt", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME))
                    .toJobParameters();
            jobLauncher.run(fetchTemperatureJob, jobParameters);
        };
    }
}
