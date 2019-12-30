package com.onepage.controller;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.onepage.service.KafkaConnection;
@RestController
@RequestMapping("/kafka")
public class DoTestController {
@Resource
private KafkaConnection connection;
@RequestMapping("/demo")
	public boolean tes() {
	return connection.isConnection("ss", 3306);
		
	}
}
