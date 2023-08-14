package com.kgromov;

import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.shell.command.annotation.CommandScan;
import org.springframework.shell.command.annotation.EnableCommand;

@SpringBootApplication
@EnableScheduling
@EnableCommand
@CommandScan
@Log4j2
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }

//    @Bean
    ApplicationRunner applicationRunner(JobLauncher jobLauncher, Job syncTemperatureJob) {
        return args -> {
            log.info("Schedule sync temperature job ...");
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(syncTemperatureJob, jobParameters);
            log.info("Finish sync temperature job ...");
        };
    }

}
