package com.onepage.service.Impl;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONObject;
import com.onepage.dao.Student;
import com.onepage.mapper.AddMapper;
import com.onepage.service.KafkaConnection;

@Service
public class KafkaConnectionImpl implements KafkaConnection {
	// 启动zookeeper
	// 启动kafka命令 在安装目录 bin文件夹那一层 不要进入bin文件 .\bin\windows\kafka-server-start.bat
	// .\config\server.properties
//查看消息命令 在安装目录 bin文件夹那一层 不要进入bin文件 shift+右键开启shell 输入  .\bin\windows\kafka-console-consumer.bat --bootstrap-server 192.168.10.104:9092 --topic test_topic_1 --from-beginning
	@Resource
	private AddMapper add;

	private static int ID = 1;

	@Override
	public boolean isConnection(String IP, Integer port) {
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put("zookeeper.connect", "192.168.10.104:2181");
		props.put("bootstrap.servers", "192.168.10.104:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		String topic = "test_topic_1";
		Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
		Long time = System.currentTimeMillis();
		ID=add.selectMax();
		ID++;
		
		Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
		Student student = Student.builder().id(ID).name("ceshi").timestamp(timestamp).date(new Date())
				.time(new Time(time)).datetime(timestamp).year(Year.now()).build();
		add.add(student);
		
		
		producer.send(new ProducerRecord<String, Object>(topic, "meeage", "" + JSONObject.toJSONString(student) + ""));
		producer.close();
		Properties posProperties = new Properties();
//		posProperties.put("zookeeper.connect", "192.168.10.104:2181");
		posProperties.put("bootstrap.servers", "192.168.10.104:9092");
		posProperties.put("group.id", "test-consumer-group");
		posProperties.put("enable.auto.commit", "false");
		posProperties.put("auto.commit.interval.ms", "1000");
		posProperties.put("auto.offset.reset", "none");
		posProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		posProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//		ConsumerConfig consumerConfig = new ConsumerConfig(posProperties);
	
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(posProperties);
		// 订阅消息
		consumer.subscribe(Arrays.asList(topic));
		ConsumerRecords<String, String> records = consumer.poll(100);
		for (ConsumerRecord<String, String> record : records) {
			if (record.value().equals(time + "")) {
				System.out.println(record.key());
				System.out.printf("offset = %d, key = %s, value = %s%n,timestamp=%s,timestampType = %s",
						record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());

				System.out.print("添加成功");
				consumer.close();
				return true;
			}
		}

		consumer.close();
		return false;
	}
}
