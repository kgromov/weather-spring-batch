package com.kgromov.config;

import com.kgromov.batch.MongoSyncDatesReaderTasklet;
import com.kgromov.batch.WriteToMongoProcessor;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.domain.DailyTemperatureDocument;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureBatchConfig {

    // step 1 - fetch data from input source - defined outside

    // step 2 - fetch data to sync in target db
    @Bean
    @StepScope
    public MongoItemReader<DailyTemperatureDocument> syncDatesReader(MongoTemplate mongoTemplate,
                                                                     @Value("#{jobParameters[syncStartDate]}") LocalDate syncStartDate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-dates-to-sync-reader")
                .template(mongoTemplate)
                .collection("weather_archive")
                .jsonQuery("{date: {$gt: ISODate('" + syncStartDate.format(ISO_DATE) + "')}}")
                // TODO: simply does not executed when using parameterValues resolver (aka PreparedStatement)
                /*   .jsonQuery("{date: {$gt: ?0}}")
                   .parameterValues(syncStartDate.format(ISO_DATE_TIME))*/
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

    /* step 3 - chunk step
     * reader       - fetch data to sync from source db
     * processor    - convert from Entity to Document; skip already synced dates
     * write        - write to target db (Mongo)
     */
    @Bean
    @StepScope
    public JpaPagingItemReader<DailyTemperature> jpaPagingItemReader(EntityManagerFactory entityManagerFactory,
                                                                     @Value("#{jobParameters[syncStartDate]}") LocalDate syncStartDate) {
        return new JpaPagingItemReaderBuilder<DailyTemperature>()
                .name("rdbms-paging-reader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select d from DailyTemperature d where d.date >= :syncStartDate order by d.date ASC")
                .parameterValues(Map.of("syncStartDate", syncStartDate))
                .pageSize(1000)
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
    public Step writeToMongoStep(JpaPagingItemReader<DailyTemperature> jpaPagingItemReader,
                                 MongoItemWriter<DailyTemperatureDocument> mongoItemWriter,
                                 JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager) {
        return new StepBuilder("write-to-mongo-step", jobRepository)
                .<DailyTemperature, DailyTemperatureDocument>chunk(1000, transactionManager)
                .reader(jpaPagingItemReader)
                .processor(toMongoProcessor())
                .writer(mongoItemWriter)
//                .taskExecutor(taskExecutor)  // make no sense for not reactive driver
                .build();
    }

    @Bean
    public Job syncTemperatureJob(Step fetchTemperatureStep,
                                  Step readDatesToSyncStep,
                                  Step writeToMongoStep,
                                  JobRepository jobRepository) {
        return new JobBuilder("writeToMongoJob", jobRepository)
                // seems 1st and 2nd can be added to Flow to be processed in parallel
                .start(fetchTemperatureStep)
                .next(readDatesToSyncStep)
                .next(writeToMongoStep)
                .build();
    }

    @Bean
    public Job fromMysqlToMongo(Step readDatesToSyncStep,
                                Step writeToMongoStep,
                                JobRepository jobRepository) {
        return new JobBuilder("writeToMongoJob", jobRepository)
                .start(readDatesToSyncStep)
                .next(writeToMongoStep)
                .build();
    }

    @Bean
    public Job syncTemperatureParallelJob(Step fetchTemperatureStep,
                                          Step readDatesToSyncStep,
                                          Step writeToMongoStep,
                                          JobRepository jobRepository,
                                          TaskExecutor taskExecutor) {
        Flow fetchFlow = new FlowBuilder<Flow>("fetch-flow")
                .start(fetchTemperatureStep)
                .build();
        Flow syncFlow = new FlowBuilder<Flow>("read-sync-flow")
                .start(readDatesToSyncStep)
                .build();
        Flow splitFlow = new FlowBuilder<Flow>("split-flow")
                .split(taskExecutor)
                .add(fetchFlow, syncFlow)
                .build();
        return new JobBuilder("sync-temperature-job", jobRepository)
                .start(splitFlow)
                .next(writeToMongoStep)
                .build()
                .build();
    }

}
