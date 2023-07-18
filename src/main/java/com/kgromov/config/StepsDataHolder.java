package com.kgromov.config;

import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ToString
public class StepsDataHolder {
//    private final Map<String, Map<String, Object>> jobDataAcrossSteps = new ConcurrentHashMap<>();
    private final Map<String, Object> dataAcrossSteps = new ConcurrentHashMap<>();

   /* public void addJobParam(JobInstance job, String key, Object value) {
        this.jobDataAcrossSteps.computeIfAbsent(job.getJobName(), params -> new ConcurrentHashMap<>()).put(key, value);
    }*/

    public void put(String key, Object value) {
        this.dataAcrossSteps.put(key, value);
    }

    public Object getValue(String key) {
        return this.dataAcrossSteps.get(key);
    }

    public  <T> T getValue(String key, Class<T> clazz) {
        return (T) this.dataAcrossSteps.get(key);
    }

    public void clear() {
        this.dataAcrossSteps.clear();
    }
}
