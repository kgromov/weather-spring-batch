package com.kgromov;

import com.kgromov.service.UpdateTemperatureScheduler;
import lombok.extern.log4j.Log4j2;
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

    @Profile("scheduler")
    @Bean
    ApplicationRunner applicationRunner(UpdateTemperatureScheduler temperatureScheduler) {
        return args -> {
            temperatureScheduler.perform();
            System.exit(0);
        };
    }
}
