package com.kgromov.config;

import lombok.AccessLevel;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;

@Setter(AccessLevel.PACKAGE)
@Configuration
@ConfigurationProperties("weather")
public class WeatherConfig {
    private WeatherSource source;

    @Bean
    WeatherSource weatherSource() {
        return source;
    }

    @Qualifier("meteopostTemplateBuilder")
    @Bean
    RestTemplateBuilder restTemplateBuilder(WeatherSource weatherSource) {
        return new RestTemplateBuilder()
                .rootUri(weatherSource.meteopostUrl())
                .defaultHeader("Content-Type", MediaType.APPLICATION_FORM_URLENCODED.toString());
    }

}
