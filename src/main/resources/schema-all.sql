DROP TABLE GRADE IF EXISTS;

CREATE TABLE GRADE(
id BIGINT IDENTITY NOT NULL PRIMARY KEY,
fname varchar(20),
lname varchar(20),
physicsMarks int,
mathsMarks int,
artsMarks int,
bioMarks int
);