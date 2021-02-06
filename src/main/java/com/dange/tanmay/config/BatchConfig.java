package com.dange.tanmay.config;

import com.dange.tanmay.JobCompletionNotificationListener;
import com.dange.tanmay.model.Grade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import javax.sql.DataSource;
import java.sql.SQLException;

@Slf4j
@EnableBatchProcessing
@Configuration
public class BatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    DataSource dataSource;


    @Bean
    public Job processDataFileJob(JobCompletionNotificationListener listener){
        return jobBuilderFactory.get("processDataFileJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(readAndParseDataFileStep())
                .build();
    }

    @Bean
    public Step readAndParseDataFileStep() {
        return stepBuilderFactory.get("readAndParseDataFileStep")
                .<String, String> chunk(5)
                .reader(itemReader(null))
                .writer(i->i.stream().forEach(j->createGrade(j)))
                .build();
    }

    private void createGrade(String j) {
        String[] val=j.split(",");
                Grade grade = new Grade(val[0],val[1], Integer.valueOf(val[2]),
                        Integer.valueOf(val[3]),Integer.valueOf(val[4]),Integer.valueOf(val[5]));
                String sql=  "INSERT INTO GRADE (fname, lname, physicsMarks,mathsMarks,artsMarks,bioMarks) VALUES (\'"
                        +  grade.getFname()+"\',\'"+ grade.getLname()+"\',"+grade.getPhysicsMarks()+","
                        +grade.getMathsMarks()+","+grade.getArtsMarks()+","+grade.getBioMarks()+")";

        try {
            dataSource.getConnection().createStatement().execute(sql);
        } catch (SQLException throwables) {
            log.error("Error Occurred in Databse execution" + throwables);
        }
    }

    @Bean
    @StepScope
    public FlatFileItemReader<String> itemReader(@Value("#{jobParameters[file_path]}") String filePath) {
        FlatFileItemReader<String> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }


}
