package com.kgromov.command;

import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.shell.command.CommandExceptionResolver;
import org.springframework.shell.command.CommandHandlingResult;
import org.springframework.shell.command.annotation.Command;
import org.springframework.shell.command.annotation.Option;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.time.LocalDate;

import static java.util.Objects.nonNull;

@Command(group = "Temperature Commands")
@Log4j2
@RequiredArgsConstructor
public class TemperatureCommands {
    private final JobLauncher jobLauncher;
    private final Job syncTemperatureJob;
    private final Job populateTemperatureJob;
    private final Job correlateMeasurementsJob;

    @Command(command = {"add"},
            description = """
                       Add temperature daily measurements starting from date.
                       If no starting date provided - last saved date is used.
                    """
    )
    public void addDailyTemperatures(@Option(longNames = {"from"}, shortNames = {'f'}, description = "Populate data from") String from,
                                     @Option(longNames = {"to"}, shortNames = {'t'}, description = "Populate data to") String to) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        log.info("Start populate temperature job ...");
        var jobParameters = new JobParametersBuilder()
                .addLong("startedAt", System.currentTimeMillis());
        this.addDateJobParameters(jobParameters, from, to);
        jobLauncher.run(populateTemperatureJob, jobParameters.toJobParameters());
        log.info("Finish populate temperature job ...");
    }

    @Command(command = {"sync"},
            description = """
                       Add missed temperature measurements starting from date.
                       If no starting date provided - last saved date is used.
                    """
    )
    public void syncTemperatures(@Option(longNames = {"from"}, shortNames = {'f'}, description = "Populate data from") String from,
                                 @Option(longNames = {"to"}, shortNames = {'t'}, description = "Populate data to") String to) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        log.info("Start sync temperature job ...");
        var jobParameters = new JobParametersBuilder()
                .addLong("startedAt", System.currentTimeMillis());
        this.addDateJobParameters(jobParameters, from, to);
        jobLauncher.run(syncTemperatureJob, jobParameters.toJobParameters());
        log.info("Finish sync temperature job ...");
    }

    @Command(command = {"correlate"},
            description = """
                       Update measurements date with time as midnight.
                       Find and remove duplicated dates measurement.
                    """
    )
    public void correlateTemperatures(@Option(longNames = {"from"}, shortNames = {'f'}, description = "Correlate data from") String from,
                                     @Option(longNames = {"to"}, shortNames = {'t'}, description = "Correlate data to") String to) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        log.info("Start correlation temperature job ...");
        var jobParameters = new JobParametersBuilder()
                .addLong("startedAt", System.currentTimeMillis());
        this.addDateJobParameters(jobParameters, from, to);
        jobLauncher.run(correlateMeasurementsJob, jobParameters.toJobParameters());
        log.info("Finish sync correlation job ...");
    }

    @Bean
    @Qualifier("customExceptionResolver")
    CommandExceptionResolver customExceptionResolver() {
        return ex -> CommandHandlingResult.of("Incorrect command format:\n" + ex.getMessage(), 1);
    }

    private void validateDateOptions(String from, String to) {
        LocalDate startDate = null;
        LocalDate endDate = null;
        if (nonNull(from)) {
            startDate = LocalDate.parse(from);
        }
        if (nonNull(to)) {
            endDate = LocalDate.parse(to);
        }
        if (nonNull(startDate) && (nonNull(endDate)) && endDate.isBefore(startDate)) {
            throw new IllegalArgumentException("Invalid dates range - [" + from + '-' + to + "]");
        }
    }

    private void addDateJobParameters(JobParametersBuilder builder, String from, String to) {
        this.validateDateOptions(from, to);
        if (nonNull(from)) {
            builder.addLocalDate("startDate", LocalDate.parse(from));
        }
        if (nonNull(to)) {
            builder.addLocalDate("endDate", LocalDate.parse(to));
        }
    }
}
