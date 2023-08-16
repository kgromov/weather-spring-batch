package com.kgromov.config;

import com.kgromov.batch.TemperatureReader;
import com.kgromov.batch.WriteToMongoProcessor;
import com.kgromov.domain.DailyTemperatureDocument;
import com.kgromov.dtos.DailyTemperatureDto;
import com.kgromov.repository.DailyTemperatureRepository;
import com.kgromov.service.TemperatureExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Optional;

import static com.kgromov.domain.City.ODESSA;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class PopulateTemperatureBatchConfig {
    private final TemperatureExtractor temperatureExtractor;
    private final DailyTemperatureRepository temperatureRepository;

    /* sync dates temperature step
     * reader       - fetch data to sync from source db
     * processor    - convert from Dto to Document
     * write        - write to target db (Mongo)
     */
    @Bean
    @StepScope
    public TemperatureReader temperatureReader(@Value("#{jobParameters[startDate]}") LocalDate syncStartDate,
                                               @Value("#{jobParameters[endDate]}") LocalDate syncEndDate) {
        LocalDate startDate = Optional.ofNullable(syncStartDate)
                .orElseGet(temperatureRepository::getLatestDateTemperature);
        LocalDate endDate = Optional.ofNullable(syncEndDate).orElseGet(LocalDate::now);
        return TemperatureReader.builder()
                .temperatureExtractor(temperatureExtractor)
                .city(ODESSA)
                .startDate(startDate)
                .endDate(endDate)
                .build();
    }

    @Bean
    public WriteToMongoProcessor toMongoProcessor() {
        return new WriteToMongoProcessor();
    }

    @Bean
    public MongoItemWriter<DailyTemperatureDocument> mongoItemWriter(MongoTemplate mongoTemplate) {
        return new MongoItemWriterBuilder<DailyTemperatureDocument>()
                .collection("weather_archive")
                .template(mongoTemplate)
                .build();
    }

    @Bean
    public Step addTemperatureStep(TemperatureReader temperatureReader,
                                   MongoItemWriter<DailyTemperatureDocument> mongoItemWriter,
                                   JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("sync-temperature-step", jobRepository)
                .<DailyTemperatureDto, DailyTemperatureDocument>chunk(10, transactionManager)
                .reader(temperatureReader)
                .processor(toMongoProcessor())
                .writer(mongoItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job populateTemperatureJob(Step addTemperatureStep,
                                      JobRepository jobRepository) {
        return new JobBuilder("sync-temperature-job", jobRepository)
                .start(addTemperatureStep)
                .build();
    }

    @Bean
    public TaskExecutor stepExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(Runtime.getRuntime().availableProcessors());
        return asyncTaskExecutor;
    }
}
