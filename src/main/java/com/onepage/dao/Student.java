package com.onepage.dao;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Year;
import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class Student implements Serializable {
	/**
		 * 
		 */
	private static final long serialVersionUID = 6196696051045384129L;
	private Integer id;
	private String name;
	private Time time;
	private Date date;
	private Year year;
	private Timestamp datetime;
	private Timestamp timestamp;
	
 
}
