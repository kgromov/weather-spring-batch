package com.kgromov.config;

import com.kgromov.batch.TemperatureReader;
import com.kgromov.batch.TemperatureWriter;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.service.TemperatureExtractor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

import static com.kgromov.domain.City.ODESSA;


@Configuration
@RequiredArgsConstructor
public class FetchTemperatureBatchConfig {
    private final TemperatureWriter temperatureWriter;
    private final TemperatureExtractor temperatureExtractor;

    @Bean
    @StepScope
    public TemperatureReader temperatureReader(@Value("#{jobParameters[syncStartDate]}") LocalDate syncStartDate) {
        return TemperatureReader.builder()
                .temperatureExtractor(temperatureExtractor)
                .city(ODESSA)
                .startDate(syncStartDate.plusDays(1))
                .build();
    }

    @Bean
    public Step fetchTemperatureStep(@Qualifier("stepExecutor") TaskExecutor taskExecutor,
                                     TemperatureReader temperatureReader,
                                     JobRepository jobRepository,
                                     /*JpaItemWriter<DailyTemperature> jpaItemWriter,*/
                                     PlatformTransactionManager transactionManager) {
        return new StepBuilder("fetch-temperature-step", jobRepository)
                .<DailyTemperature, DailyTemperature>chunk(10, transactionManager)
                .reader(temperatureReader)
//                .writer(jpaItemWriter)
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
}
