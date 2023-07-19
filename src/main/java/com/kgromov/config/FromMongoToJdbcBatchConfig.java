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
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.Map;

import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class FromMongoToJdbcBatchConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    @Bean
    public MongoItemReader<DailyTemperatureDocument> mongoItemReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-item-reader")
                .template(mongoTemplate)
                .collection("weather_archive_batch")
                .jsonQuery("{}")
//                .fields("{'_id': 0, '_class': 0}")
                .sorts(Map.of("date", ASC))
                .targetType(DailyTemperatureDocument.class)
                .pageSize(1000)
                .build();
    }

    @Bean
    public ReadFromMongoProcessor fromMongoProcessor() {
        return new ReadFromMongoProcessor();
    }

    // JpaItemWriter sucks cause insert each and every row even with batch properties configured
    @Bean
    public JpaItemWriter<DailyTemperature> jpaItemWriter(EntityManagerFactory entityManagerFactory) {
        return new JpaItemWriterBuilder<DailyTemperature>()
                .entityManagerFactory(entityManagerFactory)
                .usePersist(true)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<DailyTemperature> jdbcBatchItemWriter() {
        return new JdbcBatchItemWriterBuilder<DailyTemperature>()
                .dataSource(dataSource)
                .sql("insert into DayTemperature_batch_read(date, morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature) " +
                        "values (:date, :morningTemperature, :afternoonTemperature, :eveningTemperature, :nightTemperature)")
                .beanMapped()
                .build();
    }

    @Bean
    public Step readToMongoStep(MongoItemReader<DailyTemperatureDocument> mongoItemReader,
                                JdbcBatchItemWriter<DailyTemperature> jdbcBatchItemWriter) {
        return stepBuilderFactory.get("read-from-mongo-step").<DailyTemperatureDocument, DailyTemperature>chunk(1000)
                .reader(mongoItemReader)
                .processor(fromMongoProcessor())
                .writer(jdbcBatchItemWriter)
//                .taskExecutor(taskExecutor)  // make no sense for not reactive driver
                .build();
    }

    @Bean
    public Job readToMongoJob(Step readToMongoStep) {
        return jobBuilderFactory.get("readFromMongoJob")
                .flow(readToMongoStep)
                .end()
                .build();
    }

    private static class ReadFromMongoProcessor implements ItemProcessor<DailyTemperatureDocument, DailyTemperature> {

        @Override
        public DailyTemperature process(DailyTemperatureDocument document) {
            return DailyTemperature.builder()
                    .date(document.getDate().minusDays(1))          // due to Mongo timezone diff
                    .morningTemperature(document.getMorningTemperature())
                    .afternoonTemperature(document.getAfternoonTemperature())
                    .eveningTemperature(document.getEveningTemperature())
                    .nightTemperature(document.getNightTemperature())
                    .build();
        }
    }
}
