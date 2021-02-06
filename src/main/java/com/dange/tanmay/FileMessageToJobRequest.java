package com.dange.tanmay;

import lombok.Setter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.integration.annotation.Transformer;

import org.springframework.messaging.Message;

import java.io.File;
import java.util.Date;

@Setter
public class FileMessageToJobRequest {

    private Job job;
    private String fileParameterName;

    @Transformer
    public JobLaunchRequest toRequest(Message<File> message) {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(fileParameterName, message.getPayload().getAbsolutePath());
        jobParametersBuilder.addDate("dummy", new Date());
        return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
    }
}
