package com.kgromov.batch;

import com.kgromov.domain.DailyTemperatureDocument;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

// TODO: combine with TemperatureDatesReader - add abstract QueueItemReader class
@Slf4j
@RequiredArgsConstructor
public class RemoveDuplicatesItemReader extends AbstractItemCountingItemStreamItemReader<DailyTemperatureDocument> {
    private Queue<DailyTemperatureDocument> duplicatesQueue;
    @PostConstruct
    private void setContextName() {
        this.setName(RemoveDuplicatesItemReader.class.getSimpleName());
    }

    @BeforeStep
    public void initQueue(StepExecution stepExecution) {
        log.info("Read duplicates before step execution");
        List<DailyTemperatureDocument> duplicates = (List<DailyTemperatureDocument>) stepExecution.getJobExecution().getExecutionContext().get("duplicates");
        if (!CollectionUtils.isEmpty(duplicates)) {
            this.duplicatesQueue = new ArrayBlockingQueue<>(duplicates.size(), true, duplicates);
        }
        log.info("Read duplicates = {} before step execution", duplicates.size());
    }

    @Override
    protected DailyTemperatureDocument doRead() {
        String threadName = Thread.currentThread().getName();
        log.debug("Thread {}: Read daily temperature", threadName);
        if (CollectionUtils.isEmpty(duplicatesQueue)) {
            return null;
        }
        DailyTemperatureDocument item = duplicatesQueue.poll();
        log.debug("Thread {}: temperature item = {}", threadName, item);
        return item;
    }

    @Override
    protected void doOpen() {

    }

    @Override
    protected void doClose() {

    }
}
