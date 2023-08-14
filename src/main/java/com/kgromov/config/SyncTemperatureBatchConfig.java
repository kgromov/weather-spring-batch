package com.kgromov.config;

import com.kgromov.batch.MongoSyncDatesReaderTasklet;
import com.kgromov.batch.TemperatureDatesReader;
import com.kgromov.batch.WriteToMongoProcessor;
import com.kgromov.domain.DailyTemperatureDocument;
import com.kgromov.dtos.DailyTemperatureDto;
import com.kgromov.service.TemperatureExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import static com.kgromov.domain.City.ODESSA;
import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureBatchConfig {
    private final TemperatureExtractor temperatureExtractor;

    @Bean
    @StepScope
    public MongoItemReader<DailyTemperatureDocument> syncDatesReader(MongoTemplate mongoTemplate,
                                                                     @Value("#{jobParameters[startDate]}") LocalDate syncStartDate,
                                                                     @Value("#{jobParameters[endDate]}") LocalDate syncEndDate) {
        String startSyncDate = Optional.ofNullable(syncStartDate)
                // current weather data source keep data for current and previous years only
                .orElseGet(() -> LocalDate.of(Year.now().getValue() - 1, 1, 1))
                .format(DateTimeFormatter.ISO_DATE);
        String endSyncDate = Optional.ofNullable(syncEndDate)
                .orElseGet(LocalDate::now)
                .format(DateTimeFormatter.ISO_DATE);
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-dates-to-sync-reader")
                .template(mongoTemplate)
                .collection("weather_archive")
                .jsonQuery("{date: {$gte: ISODate('" + startSyncDate + "'), $lte: ISODate('" + endSyncDate + "')}")
                .fields("{'date': 1, '_id': 0}")
                .sorts(Map.of("date", ASC))
                .targetType(DailyTemperatureDocument.class)
                .pageSize(1000)
                .build();
    }

    @Bean
    public MongoSyncDatesReaderTasklet syncDatesReaderTasklet(MongoItemReader<DailyTemperatureDocument> syncDatesReader) {
        return new MongoSyncDatesReaderTasklet(syncDatesReader);
    }


    @Bean
    public Step readDatesToSyncStep(MongoSyncDatesReaderTasklet syncDatesReaderTasklet,
                                    JobRepository jobRepository,
                                    PlatformTransactionManager transactionManage) {
        return new StepBuilder("read-dates-to-sync-step", jobRepository)
                .tasklet(syncDatesReaderTasklet, transactionManage)
                .build();
    }

    @Bean
    @StepScope
    public TemperatureDatesReader temperatureDatesReader() {
        return new TemperatureDatesReader(ODESSA, temperatureExtractor);
    }

    @Bean
    public Step writeTemperatureStep(TemperatureDatesReader temperatureDatesReader,
                                     WriteToMongoProcessor toMongoProcessor,
                                     MongoItemWriter<DailyTemperatureDocument> mongoItemWriter,
                                     JobRepository jobRepository,
                                     PlatformTransactionManager transactionManager,
                                     @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("sync-temperature-step", jobRepository)
                .<DailyTemperatureDto, DailyTemperatureDocument>chunk(10, transactionManager)
                .reader(temperatureDatesReader)
                .processor(toMongoProcessor)
                .writer(mongoItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job syncTemperatureJob(Step readDatesToSyncStep,
                                  Step writeTemperatureStep,
                                  JobRepository jobRepository) {
        return new JobBuilder("sync-temperature-job", jobRepository)
                .start(readDatesToSyncStep)
                .next(writeTemperatureStep)
                .build();
    }
}
