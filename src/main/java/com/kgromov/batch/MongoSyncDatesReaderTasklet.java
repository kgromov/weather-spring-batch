package com.kgromov.batch;

import com.kgromov.domain.DailyTemperatureDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Optional.ofNullable;
import static org.springframework.data.domain.Sort.Direction.ASC;

@Slf4j
@RequiredArgsConstructor
public class MongoSyncDatesReaderTasklet implements Tasklet, StepExecutionListener {
    private MongoItemReader<DailyTemperatureDocument> delegate;
    private final MongoItemReaderBuilder<DailyTemperatureDocument> builder;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        LocalDate syncStartDate = ExecutionContextUtils.getDateValue(stepExecution, "syncStartDate");
        if (syncStartDate == null) {
            stepExecution.addFailureException(new IllegalStateException("Can't process sync job since no sync date provided"));
        }
        log.info("Configure MongoItemReader with latest startDate = {}", ofNullable(syncStartDate).map(date -> date.format(ISO_DATE)));
        this.delegate = builder
                .jsonQuery("{date: {$gt: ISODate('" + syncStartDate.format(ISO_DATE) + "')}}")
                .fields("{'date': 1, '_id': 0}")
                .sorts(Map.of("date", ASC))
                .build();
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Start step to fetch dates to sync");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Collect dates to sync");
        Set<LocalDate> datesToSync = new HashSet<>();
        DailyTemperatureDocument row = null;
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
