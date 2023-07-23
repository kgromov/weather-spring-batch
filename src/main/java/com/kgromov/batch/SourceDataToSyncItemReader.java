package com.kgromov.batch;

import com.kgromov.domain.DailyTemperature;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Optional.ofNullable;

@RequiredArgsConstructor
@Slf4j
// This reader fails on read due to EntityManager is null - so replaced with delegate
public class SourceDataToSyncItemReader implements ItemReader<DailyTemperature>, StepExecutionListener {
    // TODO: replace with JpaRepository
    private final JpaPagingItemReader<DailyTemperature> delegate;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        LocalDate syncStartDate = ExecutionContextUtils.getDateValue(stepExecution, "syncStartDate");
        log.info("Configure MongoItemReader with latest startDate = {}", ofNullable(syncStartDate).map(date -> date.format(ISO_DATE)));
        delegate.setParameterValues(Map.of("syncStartDate", syncStartDate));
    }

    @Override
    public DailyTemperature read() throws Exception {
        log.info("Read data from source to sync");
        return delegate.read();
    }
}
