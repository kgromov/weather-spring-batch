package com.kgromov.config;

import com.kgromov.batch.RemoveDuplicatesItemReader;
import com.kgromov.domain.DailyTemperatureDocument;
import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.springframework.data.domain.Sort.Direction.ASC;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class CorrelateDatesBatchConfig {
    private final DailyTemperatureRepository temperatureRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public MongoItemReader<DailyTemperatureDocument> readAll(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-correlate-time-reader")
                .template(mongoTemplate)
                .collection("weather_archive")
                .jsonQuery("{$expr: {$not: {$eq: ['00', {$dateToString: {date: '$date', format: '%H'}}] }}}")
                .sorts(Map.of("date", ASC))
                .targetType(DailyTemperatureDocument.class)
                .pageSize((int) temperatureRepository.count())   // sounds strange that pageSize = step.chunk does not work properly
                .build();
    }

    //    @Bean
    // Deterministic but very slow due to Mongo cluster save big chunk of data time
    public Step correlateTimeStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager) {
        return new StepBuilder("correlate-time-step", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                            log.info("Start time correlation step");
                            List<DailyTemperatureDocument> all = temperatureRepository.findAll();
                            List<DailyTemperatureDocument> toModify = all
                                    .stream()
                                    .filter(t -> t.getDate().getHour() != 0)
                                    .peek(t -> t.setDate(t.getDate().toLocalDate().atStartOfDay()))
                                    .collect(Collectors.toList());
                            temperatureRepository.saveAll(toModify);
                            log.info("Finish time correlation step");
                            return RepeatStatus.FINISHED;
                        },
                        transactionManager)
                .build();
    }

    @Bean
    public Step correlateTimeChunkStep(MongoTemplate mongoTemplate,
                                       @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        AtomicInteger correlatedItems = new AtomicInteger();
        return new StepBuilder("correlate-time-step", jobRepository)
                .<DailyTemperatureDocument, DailyTemperatureDocument>chunk(25, transactionManager)
                .reader(readAll(mongoTemplate))
                .processor(item -> {
                    item.setDate(item.getDate().toLocalDate().atStartOfDay());
                    return item;
                })
                .writer(chunk -> {
                    log.debug("Thread {}: save chunk of data size = {}", Thread.currentThread().getName(), chunk.size());
                    temperatureRepository.saveAll(chunk.getItems());
                    log.info("Correlated items = {}", correlatedItems.addAndGet(chunk.size()));
                })
                .taskExecutor(taskExecutor)
                .build();
    }

    // ==================== Parallel chunk ===================
    @Bean
    public Step saveDuplicatesStep() {
        return new StepBuilder("save-duplicates-step", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                            log.info("Start saving duplicates step");
                            List<DailyTemperatureDocument> temperatures = temperatureRepository.findAll(Sort.by(Sort.Direction.ASC, "date"));
                            Set<DailyTemperatureDocument> duplicates = new LinkedHashSet<>();
                            int notCorrelatedItems = 0;
                            for (int i = 0; i < temperatures.size() - 1; i++) {
                                DailyTemperatureDocument prevTemp = temperatures.get(i);
                                DailyTemperatureDocument nextTemp = temperatures.get(i + 1);
                                if (prevTemp.getDate().getHour() != 0) {
                                  notCorrelatedItems++;
                                }
                                if (prevTemp.getDate().toLocalDate().isEqual(nextTemp.getDate().toLocalDate())) {
                                    if (isAllMeasurementsZero(prevTemp)) {
                                        duplicates.add(prevTemp);
                                    } else {
                                        duplicates.add(nextTemp);
                                    }
                                }
                            }
                            if (notCorrelatedItems > 0) {
                                log.warn("Documents with not correlated time = {}", notCorrelatedItems);
                            }
                            ExecutionContext executionContext = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
                            executionContext.put("duplicates", List.of(duplicates.toArray()));
                            List<LocalDate> duplicatedDates = duplicates.stream().map(DailyTemperatureDocument::getDate).map(LocalDateTime::toLocalDate).distinct().sorted().toList();
                            log.info("Duplicates size = {} on dates = {}", duplicates.size(), duplicatedDates);
                            log.info("Finish saving duplicates step");
                            return RepeatStatus.FINISHED;
                        },
                        transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public RemoveDuplicatesItemReader removeDuplicatesItemReader() {
        return new RemoveDuplicatesItemReader();
    }

    @Bean
    public Step removeDuplicatesStep(RemoveDuplicatesItemReader removeDuplicatesItemReader,
                                     @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("remove-duplicates-step", jobRepository)
                .<DailyTemperatureDocument, DailyTemperatureDocument>chunk(10, transactionManager)
                .reader(removeDuplicatesItemReader)
                .writer(chunk -> {
                    log.debug("Thread {}: delete duplicates chunk size = {}", Thread.currentThread().getName(), chunk.size());
                    temperatureRepository.deleteAll(chunk.getItems());
                })
                .taskExecutor(taskExecutor)
                .build();
    }
    // =======================================

    @Bean
    public Job correlateMeasurementsJob(Step correlateTimeChunkStep,
                                        Step saveDuplicatesStep,
                                        Step removeDuplicatesStep) {
        return new JobBuilder("correlate-measurements-job", jobRepository)
                .start(correlateTimeChunkStep)
                .next(saveDuplicatesStep)
                .next(removeDuplicatesStep)
                .build();
    }

    private boolean isAllMeasurementsZero(DailyTemperatureDocument document) {
        Double zero = 0.0;
        return document.getMorningTemperature().compareTo(zero) == 0
                && document.getAfternoonTemperature().compareTo(zero) == 0
                && document.getEveningTemperature().compareTo(zero) == 0
                && document.getNightTemperature().compareTo(zero) == 0;
    }
}
