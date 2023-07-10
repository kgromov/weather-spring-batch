package com.kgromov.batch;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class CustomPartitioner implements Partitioner {
    private final int startIndex;
    private final int endIndex;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int targetSize = (endIndex - startIndex) / gridSize + 1;
        log.info("startIndex = {}, endIndex = {}, gridSize = {}, targetSize = {}", startIndex, endIndex, gridSize, targetSize);
        Map<String, ExecutionContext> partitions = new HashMap<>();

        int number = 0;
        int start = startIndex;
        int end = start + targetSize - 1;
        while (start <= endIndex) {
            ExecutionContext value = new ExecutionContext();
            partitions.put("partition" + number, value);
            if (end >= endIndex) {
                end = endIndex;
            }
            value.putInt("startIndexValue", start);
            value.putInt("endIndexValue", end);
            start += targetSize;
            end += targetSize;
            number++;
        }
        log.info("partitions: {}", partitions);
        return partitions;
    }
}
