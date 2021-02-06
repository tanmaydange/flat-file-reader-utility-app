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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
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
    public Job processDataFileJob(JobCompletionNotificationListener listener, JdbcBatchItemWriter<Grade> writer){
        return jobBuilderFactory.get("processDataFileJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(readAndParseDataFileStep(writer))
                .build();
    }

    @Bean
    public Step readAndParseDataFileStep(JdbcBatchItemWriter<Grade> writer) {
        return stepBuilderFactory.get("readAndParseDataFileStep")
                .<Grade, Grade> chunk(5)
                .reader(itemReader(null))
                .writer(writer)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Grade> writer(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Grade>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO GRADE (fname, lname, physicsMarks, mathsMarks, artsMarks, bioMarks) VALUES (:fname, :lname, :physicsMarks, :mathsMarks, :artsMarks, :bioMarks)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Grade> itemReader(@Value("#{jobParameters[file_path]}") String filePath) {
        FlatFileItemReader<Grade> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));

        DefaultLineMapper<Grade> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
        lineMapper.setFieldSetMapper(new GradeFieldSetCSVMapper());
        reader.setLineMapper(lineMapper);
        reader.open(new ExecutionContext());
        return reader;
    }


}
