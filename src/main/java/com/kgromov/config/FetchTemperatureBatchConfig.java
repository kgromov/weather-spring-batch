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
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

import static com.kgromov.domain.City.ODESSA;


@Configuration
@RequiredArgsConstructor
public class FetchTemperatureBatchConfig {
    private final TemperatureWriter temperatureWriter;
    private final TemperatureService temperatureService;

    private final TemperatureExtractor temperatureExtractor;

    private final StepsDataHolder dataHolder;

    @Lazy
    @Bean
    public TemperatureReader temperatureReader() {
        DailyTemperature latestDateTemperature = temperatureService.getLatestDateTemperature();
        LocalDate startDate = latestDateTemperature.getDate().plusDays(1);
        dataHolder.put("syncStartDate", startDate);
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
    public Step fetchTemperatureStep(@Qualifier("stepExecutor") TaskExecutor taskExecutor,
                                     TemperatureReader temperatureReader,
                                     JobRepository jobRepository,
                                     PlatformTransactionManager transactionManager) {
        return new StepBuilder("fetch-temperature-step", jobRepository)
                .<DailyTemperature, DailyTemperature>chunk(10, transactionManager)
                .reader(temperatureReader)
                .processor(processor())
                .writer(temperatureWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job fetchTemperatureJob(Step fetchTemperatureStep, JobRepository jobRepository) {
        return new JobBuilder("fetchTemperature", jobRepository)
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
