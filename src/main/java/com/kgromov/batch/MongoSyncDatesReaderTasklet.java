package com.kgromov.batch;

import com.kgromov.domain.DailyTemperatureDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.StopWatch;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
public class MongoSyncDatesReaderTasklet implements Tasklet, StepExecutionListener {
    private final MongoItemReader<DailyTemperatureDocument> delegate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Start step to fetch dates to sync");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Collect dates to sync");
        Set<LocalDate> datesToSync = new HashSet<>();
        DailyTemperatureDocument row;
        while((row = delegate.read()) != null) {
            datesToSync.add(row.getDate());
        }
        ExecutionContext executionContext = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
        executionContext.put("datesToSync", datesToSync);
        stopWatch.stop();
        log.info("Dates to sync = {}", datesToSync);
        log.info("Finish step to fetch dates to sync in {} ms", stopWatch.getLastTaskTimeMillis());
        return RepeatStatus.FINISHED;
    }

}
