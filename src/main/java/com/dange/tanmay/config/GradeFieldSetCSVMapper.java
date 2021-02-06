package com.dange.tanmay.config;

import com.dange.tanmay.model.Grade;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class GradeFieldSetCSVMapper implements FieldSetMapper<Grade> {
    @Override
    public Grade mapFieldSet(FieldSet fieldSet) throws BindException {
        Grade grade = new Grade();

        grade.setFname(fieldSet.readString(0));
        grade.setLname(fieldSet.readString(1));
        grade.setPhysicsMarks(fieldSet.readInt(2));
        grade.setMathsMarks(fieldSet.readInt(3));
        grade.setArtsMarks(fieldSet.readInt(4));
        grade.setBioMarks(fieldSet.readInt(5));

        return grade;
    }
}
