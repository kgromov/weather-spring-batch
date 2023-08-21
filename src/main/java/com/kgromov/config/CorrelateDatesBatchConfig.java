package com.kgromov.config;

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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * TODO:
 * 1) update dates for start of the day;
 * 2) remove duplicates
 * Add tasklet steps based on repository? At least better to replace insert to upsert
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
public class CorrelateDatesBatchConfig {
    private final DailyTemperatureRepository temperatureRepository;


    @Bean
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
    public Step removeDuplicatesStep(JobRepository jobRepository,
                                     PlatformTransactionManager transactionManager) {
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
                            temperatureRepository.deleteAll(duplicates);
                            log.info("Finish removing duplicates step");
                            return RepeatStatus.FINISHED;
                        },
                        transactionManager)
                .build();
    }

    @Bean
    public Job correlateMeasurementsJob(Step correlateTimeStep,
                                        Step removeDuplicatesStep,
                                        JobRepository jobRepository) {
        return new JobBuilder("correlate-measurements-job", jobRepository)
//                .start(correlateTimeStep)
                .start(removeDuplicatesStep)
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
