package com.dange.tanmay;

import com.dange.tanmay.model.Result;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
    public static final String QUERY_SQL = "SELECT fname, lname, physicsMarks+mathsMarks+artsMarks+bioMarks as total from GRADE";

    @Autowired
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterJob(JobExecution jobExecution){
        if (jobExecution.getStatus() == BatchStatus.COMPLETED){
            log.info("JOB Finished Summarizing the results");
            final Double[] totalValuation = {0.0};
            jdbcTemplate.query(QUERY_SQL,
                    (rs, row) -> new Result(rs.getString(1) ,
                            rs.getString(2), rs.getDouble(3)
                    )).forEach(r-> log.info("Total Marks :"+ r));
        }
    }
}
