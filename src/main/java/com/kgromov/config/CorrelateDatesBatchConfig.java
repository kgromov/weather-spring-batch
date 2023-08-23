package com.kgromov.config;

import com.kgromov.domain.DailyTemperatureDocument;
import com.kgromov.repository.DailyTemperatureRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.domain.Sort.Direction.ASC;

// TODO: convert correlateTimeStep to chunk step with concurrent execution
// removeDuplicatesStep can be either tasklet with concurrency or split into 2 steps - 1 find duplicates, 2 - write to db
@Configuration
@Slf4j
@RequiredArgsConstructor
public class CorrelateDatesBatchConfig {
    private final DailyTemperatureRepository temperatureRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    // too slow, just keep as an example
    @Bean
    public Step correlateTimeTasletStep() {
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

    public MongoItemReader<DailyTemperatureDocument> readAll(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<DailyTemperatureDocument>()
                .name("mongo-correlate-time-reader")
                .template(mongoTemplate)
                .collection("weather_archive")
                .jsonQuery("{}")
                .sorts(Map.of("date", ASC))
                .targetType(DailyTemperatureDocument.class)
                .pageSize(1000)
                .build();
    }

    @Bean
    public Step correlateTimeChunkStep(MongoTemplate mongoTemplate,
                                       @Qualifier("stepExecutor") TaskExecutor taskExecutor) {
        return new StepBuilder("correlate-time-step", jobRepository)
                .<DailyTemperatureDocument, DailyTemperatureDocument>chunk(250, transactionManager)
                .reader(readAll(mongoTemplate))
                .processor(item -> item.getDate().getHour() == 0 ? item : null)
                .writer(chunk -> {
                    log.info("{}: save chunk of data size = {}", Thread.currentThread().getName(), chunk.size());
                    temperatureRepository.saveAll(chunk.getItems());
                })
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Step removeDuplicatesStep() {
        return new StepBuilder("remove-duplicates-step", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                            log.info("Start removing duplicates step");
                            List<DailyTemperatureDocument> temperatures = temperatureRepository.findAll(Sort.by(Sort.Direction.ASC, "date"));
                            Set<DailyTemperatureDocument> duplicates = new HashSet<>();
                            for (int i = 0; i < temperatures.size() - 1; i++) {
                                DailyTemperatureDocument prevTemp = temperatures.get(i);
                                DailyTemperatureDocument nextTemp = temperatures.get(i + 1);
                                if (prevTemp.getDate().toLocalDate().isEqual(nextTemp.getDate().toLocalDate())) {
                                    if (isAllMeasurementsZero(prevTemp)) {
                                        duplicates.add(prevTemp);
                                    } else {
                                        duplicates.add(nextTemp);
                                    }
                                }
                            }
                            List<LocalDate> duplicatedDates = duplicates.stream().map(DailyTemperatureDocument::getDate).map(LocalDateTime::toLocalDate).distinct().sorted().toList();
                            log.info("Duplicates size = {} on dates = {}", duplicates.size(), duplicatedDates);
                            // TODO: AsyncProcessor+Writer or master-slave with Partitioner
                            temperatureRepository.deleteAll(duplicates);
                            log.info("Finish removing duplicates step");
                            return RepeatStatus.FINISHED;
                        },
                        transactionManager)
                .build();
    }

    @Bean
    public Job correlateMeasurementsJob(Step correlateTimeChunkStep,
                                        Step removeDuplicatesStep) {
        return new JobBuilder("correlate-measurements-job", jobRepository)
                .start(correlateTimeChunkStep)
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
