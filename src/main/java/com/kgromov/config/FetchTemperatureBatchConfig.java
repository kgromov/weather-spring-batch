package com.kgromov.config;

import com.kgromov.batch.CustomPartitioner;
import com.kgromov.batch.TemperatureReader;
import com.kgromov.batch.TemperatureWriter;
import com.kgromov.domain.DailyTemperature;
import com.kgromov.service.TemperatureExtractor;
import com.kgromov.service.TemperatureService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDate;

import static com.kgromov.domain.City.ODESSA;


@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class FetchTemperatureBatchConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final TemperatureWriter temperatureWriter;
    private final TemperatureService temperatureService;

    private final TemperatureExtractor temperatureExtractor;

    private final StepsDataHolder dataHolder;

    @Lazy
    @Bean
    public TemperatureReader temperatureReader() {
        DailyTemperature latestDateTemperature = temperatureService.getLatestDateTemperature();
        LocalDate startDate = latestDateTemperature.getDate().plusDays(1);
        dataHolder.put("syncStartDate", startDate);
        return TemperatureReader.builder()
                .temperatureExtractor(temperatureExtractor)
                .city(ODESSA)
                .startDate(startDate)
                .build();
    }

    @Bean
    public WeatherProcessor processor() {
        return new WeatherProcessor();
    }

    @Bean
    public CustomPartitioner partitioner() {
        return CustomPartitioner.builder()
                .startIndex(1)
                .endIndex(10)
                .build();
    }

    @Bean
    public PartitionHandler partitionHandler(@Qualifier("stepExecutor") TaskExecutor taskExecutor, Step fetchTemperatureStep) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(Runtime.getRuntime().availableProcessors() - 1);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor);
        taskExecutorPartitionHandler.setStep(fetchTemperatureStep);
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step fetchTemperatureStep(TemperatureReader temperatureReader, WeatherProcessor processor) {
        return stepBuilderFactory.get("fetch-temperature-step").<DailyTemperature, DailyTemperature>chunk(10)
                .reader(temperatureReader)
                .processor(processor)
                .writer(temperatureWriter)
                .build();
    }

    @Bean
    public Step masterStep(Step fetchTemperatureStep, CustomPartitioner partitioner, PartitionHandler partitionHandler) {
        return stepBuilderFactory.get("master-step")
                .partitioner(fetchTemperatureStep.getName(), partitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public Job fetchTemperatureJob(Step masterStep) {
        return jobBuilderFactory.get("fetchTemperature")
                .flow(masterStep)
                .end()
                .build();
    }

    @Bean
    public TaskExecutor stepExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        int cpusPerJob = Runtime.getRuntime().availableProcessors() - 1;
        taskExecutor.setMaxPoolSize(cpusPerJob);
        taskExecutor.setCorePoolSize(cpusPerJob / 2);
        taskExecutor.setQueueCapacity(cpusPerJob * 2);
        return taskExecutor;
    }

    private static class WeatherProcessor implements ItemProcessor<DailyTemperature, DailyTemperature> {
        @Override
        public DailyTemperature process(DailyTemperature dailyTemperature) {
            return dailyTemperature;
        }
    }
}
