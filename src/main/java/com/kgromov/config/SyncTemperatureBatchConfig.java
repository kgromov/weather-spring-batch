package com.kgromov.config;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.domain.DailyTemperatureDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.persistence.EntityManagerFactory;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureBatchConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    //    @Bean
    public JpaCursorItemReader<DailyTemperature> jpaCursorItemReader(EntityManagerFactory entityManagerFactory) {
        return new JpaCursorItemReaderBuilder<DailyTemperature>()
                .name("rdbms-cursor-reader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select d from DailyTemperature d order by d.date ASC")
                .build();
    }

    @Bean
    public JpaPagingItemReader<DailyTemperature> jpaPagingItemReader(EntityManagerFactory entityManagerFactory) {
        return new JpaPagingItemReaderBuilder<DailyTemperature>()
                .name("rdbms-paging-reader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select d from DailyTemperature d order by d.date ASC")
                .pageSize(1000)
                .build();
    }

    @Bean
    public WriteToMongoProcessor toMongoProcessor() {
        return new WriteToMongoProcessor();
    }

  /*  @Bean
    public ItemProcessor<DailyTemperature, DailyTemperature> syncTemperatureProcessor() {
        return item -> item;
    }*/

    @Bean
    public MongoItemWriter<DailyTemperatureDocument> mongoItemWriter(MongoTemplate mongoTemplate) {
        return new MongoItemWriterBuilder<DailyTemperatureDocument>()
                .collection("weather_archive_batch")
                .template(mongoTemplate)
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

    private static class WriteToMongoProcessor implements ItemProcessor<DailyTemperature, DailyTemperatureDocument> {

        @Override
        public DailyTemperatureDocument process(DailyTemperature entity) {
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
