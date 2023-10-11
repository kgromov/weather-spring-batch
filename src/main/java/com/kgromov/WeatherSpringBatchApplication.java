package com.kgromov;

import com.kgromov.service.UpdateTemperatureScheduler;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Log4j2
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }


    @Profile("crap")
    @Bean
    ApplicationRunner crapData(JobLauncher jobLauncher, Job findCrapDataJob) {
        return args -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(findCrapDataJob, jobParameters);
        };
    }
    @Profile("correction")
    @Bean
    ApplicationRunner removeDuplicates(JobLauncher jobLauncher, Job removeDuplicatesJob) {
       return args -> {
           log.info("Start removeDuplicatesJob job ...");
           JobParameters jobParameters = new JobParametersBuilder()
                   .addLong("startedAt", System.currentTimeMillis())
                   .toJobParameters();
           jobLauncher.run(removeDuplicatesJob, jobParameters);
           log.info("Finish removeDuplicatesJob job ...");
       };
    }

    @Profile("scheduler")
    @Bean
    ApplicationRunner applicationRunner(UpdateTemperatureScheduler temperatureScheduler) {
        return args -> {
            temperatureScheduler.perform();
            System.exit(0);
        };
    }
}
