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
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import javax.sql.DataSource;

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


    /**
     * Spring Batch Job to process the CSV File and load into H2 Database
     * @param listener
     * @param writer
     * @return
     */
  //  @Bean
    public Job processCsvFileJob(JobCompletionNotificationListener listener, JdbcBatchItemWriter<Grade> writer){
        return jobBuilderFactory.get("processCsvFileJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(readAndParseCsvFileStep(writer))
                .build();
    }

    /**
     * Spring Batch Step to process the CSV File and load into H2 Database
     * @param writer
     * @return
     */
    @Bean
    public Step readAndParseCsvFileStep(JdbcBatchItemWriter<Grade> writer) {
        return stepBuilderFactory.get("readAndParseCsvFileStep")
                .<Grade, Grade> chunk(5)
                .reader(csvReader(null))
                .writer(writer)
                .build();
    }

    /**
     * Common Writer to persist the Object into H2 Database
     * @param dataSource
     * @return
     */
    @Bean
    public JdbcBatchItemWriter<Grade> writer(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Grade>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO GRADE (fname, lname, physicsMarks, mathsMarks, artsMarks, bioMarks) VALUES (:fname, :lname, :physicsMarks, :mathsMarks, :artsMarks, :bioMarks)")
                .dataSource(dataSource)
                .build();
    }

    /**
     * Function to parse CSV File into Java Object and return ItemReader
     * @param filePath
     * @return
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Grade> csvReader(@Value("#{jobParameters[file_path]}") String filePath) {
        FlatFileItemReader<Grade> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));

        DefaultLineMapper<Grade> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
        lineMapper.setFieldSetMapper(new GradeFieldSetMapper());
        reader.setLineMapper(lineMapper);
        reader.open(new ExecutionContext());
        return reader;
    }

    /**
     * Spring Batch Job to process Fixed Length Flat file and persist into H2 Database
     * @param listener
     * @param writer
     * @return
     */
    @Bean
    public Job processFixedLengthFileJob(JobCompletionNotificationListener listener, JdbcBatchItemWriter<Grade> writer){
        return jobBuilderFactory.get("processFixedLengthFileJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(readAndParseFixedLengthFileStep(writer))
                .build();
    }

    /**
     * Spring Batch Job Step to process Fixed Length Flat file and persist into H2 Database
     * @param writer
     * @return
     */
    @Bean
    public Step readAndParseFixedLengthFileStep(JdbcBatchItemWriter<Grade> writer) {
        return stepBuilderFactory.get("readAndParseFixedLengthFileStep")
                .<Grade, Grade> chunk(5)
                .reader(fixedWithFileReader(null))
                .writer(writer)
                .build();
    }


    /**
     * Function to parse Fixed Width Flat file into Java Object and return ItemReader
     * @param filePath
     * @return
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Grade> fixedWithFileReader(@Value("#{jobParameters[file_path]}") String filePath) {
        FlatFileItemReader<Grade> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(filePath));

        DefaultLineMapper<Grade> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(fixedLengthTokenizer());
        lineMapper.setFieldSetMapper(new GradeFieldSetMapper());
        reader.setLineMapper(lineMapper);
        reader.open(new ExecutionContext());
        return reader;
    }

    /**
     * Function to return the tokenizer which helps identifying different fields of flat file.
     * @return tokenizer object
     */
    @Bean
    public FixedLengthTokenizer fixedLengthTokenizer(){
        FixedLengthTokenizer tokenizer= new FixedLengthTokenizer();

        tokenizer.setNames("fname" , "lname" , "physicsMarks" ,"mathsMarks" ,"artsMarks", "bioMarks");
        tokenizer.setColumns(new Range(1,6),
                new Range(7,13),
                new Range(14,15),
                new Range(16,17),
                new Range(18,19),
                new Range(20,21)
                );
        return tokenizer;
    }


}
