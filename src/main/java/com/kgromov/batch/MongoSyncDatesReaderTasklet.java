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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import static java.time.temporal.ChronoUnit.DAYS;

@Slf4j
@RequiredArgsConstructor
public class MongoSyncDatesReaderTasklet implements Tasklet, StepExecutionListener {
    private final MongoItemReader<DailyTemperatureDocument> delegate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Start step to fetch dates to sync");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Collect dates to sync");
        Set<LocalDate> missedDates = new HashSet<>();
        DailyTemperatureDocument row;
        LocalDate startRangeDate = null;
        while ((row = delegate.read()) != null) {
            LocalDate currentDate = row.getDate().toLocalDate();
            if (startRangeDate == null) {
                startRangeDate = currentDate;
            }
            int daysDiff = (int) DAYS.between(startRangeDate, currentDate);
            for(int i = 1; i < daysDiff; i++) {
                missedDates.add(startRangeDate.plusDays(i));
            }
            startRangeDate = currentDate;
        }
        ExecutionContext executionContext = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
        executionContext.put("datesToSync", missedDates);
        stopWatch.stop();
        log.info("Dates to sync = {}", missedDates);
        log.info("Finish step to fetch dates to sync in {} ms", stopWatch.getLastTaskTimeMillis());
        return RepeatStatus.FINISHED;
    }
}
