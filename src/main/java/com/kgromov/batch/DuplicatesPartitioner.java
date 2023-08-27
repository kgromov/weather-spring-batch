package com.kgromov.batch;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

@Log4j2
@RequiredArgsConstructor
public class DuplicatesPartitioner implements Partitioner {
    private final int min;
    private final int max;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int chunkSize = (max - min) / gridSize + 1;
        log.info("chunkSize : " + chunkSize);
        Map<String, ExecutionContext> partitions = new HashMap<>();
        int iteration = 0;
        int start = min;
        int end = start + chunkSize - 1;
        while (start <= max) {
            ExecutionContext context = new ExecutionContext();
            partitions.put("partition" + iteration, context);
            if (end >= max) {
                end = max;
            }
            context.putInt("page", iteration);
            context.putInt("minValue", start);
            context.putInt("maxValue", end);
            start += chunkSize;
            end += chunkSize;
            iteration++;
        }
        log.info("partitions = " + partitions);
        return partitions;
    }
}
