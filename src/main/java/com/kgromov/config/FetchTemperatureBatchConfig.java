package com.kgromov.config;

import com.kgromov.batch.TemperatureReader;
import com.kgromov.batch.TemperatureWriter;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.service.TemperatureExtractor;
import com.kgromov.service.TemperatureService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.time.LocalDate;

import static com.kgromov.domain.City.ODESSA;


@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class FetchTemperatureBatchConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final TemperatureWriter temperatureWriter;
    private final TemperatureService temperatureService;

    private final TemperatureExtractor temperatureExtractor;

    @Lazy
    @Bean
    public TemperatureReader temperatureReader() {
        DailyTemperature latestDateTemperature = temperatureService.getLatestDateTemperature();
        LocalDate startDate = latestDateTemperature.getDate().plusDays(1);
        return TemperatureReader.builder()
                .temperatureExtractor(temperatureExtractor)
                .city(ODESSA)
                .startDate(startDate)
                .build();
    }

    @Bean
    public WeatherProcessor processor() {
        return new WeatherProcessor();
    }

    @Bean
    public Step fetchTemperatureStep(@Qualifier("stepExecutor") TaskExecutor taskExecutor, TemperatureReader temperatureReader) {
        return stepBuilderFactory.get("fetch-temperature-step").<DailyTemperature, DailyTemperature>chunk(10)
                .reader(temperatureReader)
                .processor(processor())
                .writer(temperatureWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job fetchTemperatureJob(Step fetchTemperatureStep) {
        return jobBuilderFactory.get("fetchTemperature")
                .flow(fetchTemperatureStep)
                .end()
                .build();
    }

    @Bean
    public TaskExecutor stepExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(Runtime.getRuntime().availableProcessors());
        return asyncTaskExecutor;
    }

    private static class WeatherProcessor implements ItemProcessor<DailyTemperature, DailyTemperature> {
        @Override
        public DailyTemperature process(DailyTemperature dailyTemperature) {
            return dailyTemperature;
        }
    }
}
