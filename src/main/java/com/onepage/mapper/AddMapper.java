package com.onepage.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.onepage.dao.Student;

@Mapper
public interface AddMapper {
	void add(Student student);

	Integer selectMax();
}
