package com.kgromov.config;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.domain.DailyTemperatureDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Optional.ofNullable;
import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureBatchConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final StepsDataHolder dataHolder;

    @Bean
    @Lazy
    public JpaPagingItemReader<DailyTemperature> jpaPagingItemReader(EntityManagerFactory entityManagerFactory) {
        return new JpaPagingItemReaderBuilder<DailyTemperature>()
                .name("rdbms-paging-reader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select d from DailyTemperature d where d.date >= :syncStartDate order by d.date ASC")
                .parameterValues(Map.of("syncStartDate", dataHolder.getValue("syncStartDate")))
                .pageSize(1000)
                .build();
    }

    @Bean
    public WriteToMongoProcessor toMongoProcessor() {
        return new WriteToMongoProcessor(dataHolder);
    }

    @Bean
    public MongoItemWriter<DailyTemperatureDocument> mongoItemWriter(MongoTemplate mongoTemplate) {
        return new MongoItemWriterBuilder<DailyTemperatureDocument>()
                .collection("weather_archive_batch")
                .template(mongoTemplate)
                .build();
    }

    @Bean
    @Lazy
    public MongoItemReader<DailyTemperatureDocument> mongoLatestDatesReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-latest-date-reader")
                .template(mongoTemplate)
                .collection("weather_archive_batch")
                .jsonQuery("{date: {$gt: ISODate('"+ dataHolder.getValue("syncStartDate", LocalDate.class).format(ISO_DATE) +"')}}")
                // TODO: simply does not executed when using parameterValues resolver (aka PreparedStatement)
             /*   .jsonQuery("{date: {$gt: ?0}}")
                .parameterValues(dataHolder.getValue("syncStartDate", LocalDate.class).format(ISO_DATE_TIME))*/
                .fields("{'date': 1, '_id': 0}")
                .sorts(Map.of("date", ASC))
                .targetType(DailyTemperatureDocument.class)
                .pageSize(1000)
                .build();
    }

    @Bean
    public Step readLatestDatesFromMongoStep(MongoItemReader<DailyTemperatureDocument> mongoLatestDatesReader) {
        return stepBuilderFactory.get("read-latest-dates-from-mongo-step").<DailyTemperatureDocument, LocalDate>chunk(1000)
                .reader(mongoLatestDatesReader)
                .processor((ItemProcessor<DailyTemperatureDocument, LocalDate>) item -> {
                    log.info("Latest date in mongo : {}", item);
                    return item.getDate();
                })
                .writer(items -> {
                    log.info("Write items = {}", items);
                    dataHolder.put("latestDates", items);
                })
                .build();
    }

    @Bean
    public Step writeToMongoStep(JpaPagingItemReader<DailyTemperature> jpaPagingItemReader,
                                 MongoItemWriter<DailyTemperatureDocument> mongoItemWriter) {
        return stepBuilderFactory.get("write-to-mongo-step").<DailyTemperature, DailyTemperatureDocument>chunk(1000)
                .reader(jpaPagingItemReader)
                .processor(toMongoProcessor())
                .writer(mongoItemWriter)
//                .taskExecutor(taskExecutor)  // make no sense for not reactive driver
                .build();
    }

    @Bean
    public Job writeToMongoJob(Step writeToMongoStep) {
        return jobBuilderFactory.get("writeToMongoJob")
                .flow(writeToMongoStep)
                .end()
                .build();
    }

    @Bean
    public Job syncTemperatureJob(Step writeToMongoStep, Step readLatestDatesFromMongoStep, Step fetchTemperatureStep) {
        return jobBuilderFactory.get("writeToMongoJob")
                .start(fetchTemperatureStep)
                .next(readLatestDatesFromMongoStep)
                .next(writeToMongoStep)
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        log.info("Job configuration: instance name = {}", jobExecution.getJobInstance().getJobName());
                        log.info("Sync data: {}", dataHolder);
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        dataHolder.clear();
                    }
                })
                .build();
    }

    @RequiredArgsConstructor
    private static class WriteToMongoProcessor implements ItemProcessor<DailyTemperature, DailyTemperatureDocument> {
        private final StepsDataHolder dataHolder;

        @Override
        public DailyTemperatureDocument process(DailyTemperature entity) {
            List<String> latestDates = dataHolder.getValue("latestDates", List.class);
            if (!CollectionUtils.isEmpty(latestDates) && latestDates.contains(entity.getDate().plusDays(1))) {
                return null;
            }
            return DailyTemperatureDocument.builder()
                    .date(entity.getDate().plusDays(1))             // due to Mongo timezone diff
                    .morningTemperature(entity.getMorningTemperature())
                    .afternoonTemperature(entity.getAfternoonTemperature())
                    .eveningTemperature(entity.getEveningTemperature())
                    .nightTemperature(entity.getNightTemperature())
                    .build();
        }
    }
}
