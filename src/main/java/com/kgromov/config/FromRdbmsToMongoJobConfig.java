package com.kgromov.config;

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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.ZoneId;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class FromRdbmsToMongoJobConfig {

    @Bean
    public JpaPagingItemReader<DailyTemperature> jpaReadAll(EntityManagerFactory entityManagerFactory) {
        return new JpaPagingItemReaderBuilder<DailyTemperature>()
                .name("rdbms-reader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select d from DailyTemperature d order by d.date ASC")
                .pageSize(1000)
                .build();
    }

    @Bean
    public ItemProcessor<DailyTemperature, DailyTemperatureDocument> convertToDailyDocument() {
        return entity -> DailyTemperatureDocument.builder()
                .date(entity.getDate().atStartOfDay(ZoneId.of("UTC")).toLocalDateTime())
                .morningTemperature(entity.getMorningTemperature())
                .afternoonTemperature(entity.getAfternoonTemperature())
                .eveningTemperature(entity.getEveningTemperature())
                .nightTemperature(entity.getNightTemperature())
                .build();
    }

    @Bean
    public Step exportFromRdbmsToMongoStep(JpaPagingItemReader<DailyTemperature> jpaReadAll,
                                           MongoItemWriter<DailyTemperatureDocument> mongoItemWriter,
                                           JobRepository jobRepository,
                                           PlatformTransactionManager transactionManager,
                                           @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("import-to-mongo-step", jobRepository)
                .<DailyTemperature, DailyTemperatureDocument>chunk(1000, transactionManager)
                .reader(jpaReadAll)
                .processor(convertToDailyDocument())
                .writer(mongoItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job exportFromRdbmsToMongoJob(Step exportFromRdbmsToMongoStep,
                                         JobRepository jobRepository) {
        return new JobBuilder("import-to-mongo-job", jobRepository)
                .start(exportFromRdbmsToMongoStep)
                .build();
    }
}
