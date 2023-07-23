package com.kgromov.batch;

import com.kgromov.domain.DailyTemperature;
import com.kgromov.domain.DailyTemperatureDocument;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.util.Set;

@RequiredArgsConstructor
public class WriteToMongoProcessor implements ItemProcessor<DailyTemperature, DailyTemperatureDocument> {
    private Set<LocalDate> datesToSync;

    @BeforeStep
    public void readDatesToSync(StepExecution stepExecution) {
        this.datesToSync = (Set<LocalDate>) stepExecution.getJobExecution().getExecutionContext().get("datesToSync");
    }

    @Override
    public DailyTemperatureDocument process(DailyTemperature entity) {
        if (!CollectionUtils.isEmpty(datesToSync) && datesToSync.contains(entity.getDate().plusDays(1))) {
            return null;
        }
        return DailyTemperatureDocument.builder()
                .date(entity.getDate().plusDays(1))             // due to Mongo timezone diff
                .morningTemperature(entity.getMorningTemperature())
                .afternoonTemperature(entity.getAfternoonTemperature())
                .eveningTemperature(entity.getEveningTemperature())
                .nightTemperature(entity.getNightTemperature())
                .build();
    }
}
