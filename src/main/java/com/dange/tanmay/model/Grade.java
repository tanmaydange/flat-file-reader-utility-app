package com.dange.tanmay.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Grade {
    private String fname;
    private String lname;
    private int physicsMarks;
    private int mathsMarks;
    private int artsMarks;
    private int bioMarks;

}
