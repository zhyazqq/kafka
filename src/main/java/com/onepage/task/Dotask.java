package com.onepage.task;

import javax.annotation.Resource;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.onepage.service.KafkaConnection;
@Component
public class Dotask {
	@Resource
	private KafkaConnection kafkaConnection;
//	@Scheduled(cron = "0/10 * * * * ?")
	public void doTest() {
		kafkaConnection.isConnection("0", 5566);
	}

}
