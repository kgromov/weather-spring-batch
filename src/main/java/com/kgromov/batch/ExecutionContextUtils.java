package com.kgromov.batch;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;

import java.time.LocalDate;

public class ExecutionContextUtils {

    public static <T> T getValue(StepExecution stepExecution, String key, Class<T> clazz) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        if (executionContext.containsKey(key)) {
            return clazz.cast(executionContext.get(key));
        }
        return null;
    }

    public static LocalDate getDateValue(StepExecution stepExecution, String key) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        if (executionContext.containsKey(key)) {
            Object value = executionContext.get(key);
            return (value  instanceof LocalDate) ? (LocalDate) value : LocalDate.parse(String.valueOf(value));
        }
        return null;
    }
}
