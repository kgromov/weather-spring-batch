package com.kgromov;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class WeatherSpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherSpringBatchApplication.class, args);
    }

}
