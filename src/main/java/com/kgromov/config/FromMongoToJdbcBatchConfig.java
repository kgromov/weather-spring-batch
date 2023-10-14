package com.kgromov.config;

import com.kgromov.batch.TemperatureWriter;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.domain.DailyTemperatureDocument;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class FromMongoToJdbcBatchConfig {
    private final DataSource dataSource;
    private final @Qualifier("stepExecutor") TaskExecutor taskExecutor;
    private final TemperatureWriter temperatureWriter;

    @Bean
    public MongoItemReader<DailyTemperatureDocument> mongoItemReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-item-reader")
                .template(mongoTemplate)
                .collection("weather_archive")
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

    @Bean
    public AsyncItemProcessor<DailyTemperatureDocument, DailyTemperature> temperatureAsyncItemProcessor() {
        AsyncItemProcessor<DailyTemperatureDocument, DailyTemperature> itemProcessor = new AsyncItemProcessor<>();
        itemProcessor.setDelegate(fromMongoProcessor());
        itemProcessor.setTaskExecutor(taskExecutor);
        return itemProcessor;
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
                .sql(
                        """
                            insert into DayTemperature(date, morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature)
                            values (:date, :morningTemperature, :afternoonTemperature, :eveningTemperature, :nightTemperature)
                        """
                )
                .beanMapped()
                .build();
    }

    @Bean
    public AsyncItemWriter<DailyTemperature> temperatureAsyncItemWriter() {
        AsyncItemWriter<DailyTemperature> asyncItemWriter = new AsyncItemWriter<>();
//        asyncItemWriter.setDelegate(temperatureWriter);
        asyncItemWriter.setDelegate(jdbcBatchItemWriter());
        return asyncItemWriter;
    }

    @Bean
    public Step readFromMongoStep(MongoItemReader<DailyTemperatureDocument> mongoItemReader,
                                  AsyncItemProcessor<DailyTemperatureDocument, DailyTemperature> temperatureAsyncItemProcessor,
                                  AsyncItemWriter<DailyTemperature> temperatureAsyncItemWriter,
                                  JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager) {
        return new StepBuilder("read-from-mongo-step", jobRepository)
                .<DailyTemperatureDocument, DailyTemperature>chunk(1000, transactionManager)
                .reader(mongoItemReader)
                .processor((ItemProcessor) temperatureAsyncItemProcessor)
                .writer(temperatureAsyncItemWriter)
                .taskExecutor(taskExecutor)  // make no sense for not reactive driver
                .build();
    }

    @Bean
    public Job readFromMongoJob(Step readFromMongoStep, JobRepository jobRepository) {
        return new JobBuilder("read-from-mongo-job", jobRepository)
                .start(readFromMongoStep)
                .build();
    }

    @Slf4j
    private static class ReadFromMongoProcessor implements ItemProcessor<DailyTemperatureDocument, DailyTemperature> {
        private final AtomicInteger processedItems = new AtomicInteger();

        @Override
        public DailyTemperature process(DailyTemperatureDocument document) {
            if (processedItems.getAndIncrement() % 100 == 0) {
                log.info("{}: processed {} items", Thread.currentThread(), processedItems.get());
            }
            return DailyTemperature.builder()
                    .date(document.getDate().toLocalDate().minusDays(1))          // due to Mongo timezone diff
                    .morningTemperature(document.getMorningTemperature())
                    .afternoonTemperature(document.getAfternoonTemperature())
                    .eveningTemperature(document.getEveningTemperature())
                    .nightTemperature(document.getNightTemperature())
                    .build();
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class MongoReader implements ItemReader<DailyTemperatureDocument> {

        private final MongoItemReader<DailyTemperatureDocument> delegate;

        @Override
        public DailyTemperatureDocument read() throws Exception {
            log.info("Read data: thread = {}", Thread.currentThread().getName());
            return delegate.read();
        }
    }
}
