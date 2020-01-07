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

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
	// 启动kafka命令 在安装目录 bin文件夹那一层 不要进入bin文件 .\bin\windows\kafka-server-start.bat .\config\server.properties
	// 
//查看消息命令 在安装目录 bin文件夹那一层 不要进入bin文件 shift+右键开启shell 输入  .\bin\windows\kafka-console-consumer.bat --bootstrap-server 192.168.10.104:9092 --topic test_topic_1 --from-beginning
	@Resource
	private AddMapper add;

	private static int ID = 1;

	@Override
	public boolean isConnection(String IP, Integer port) {
		//生产者
		HashMap<String, Object> props = new HashMap<String, Object>();
//		props.put("zookeeper.connect", "192.168.10.104:2181");
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
		producer.send(new ProducerRecord<String, Object>(topic, "message", "" + JSONObject.toJSONString(student) + ""));
		producer.close();
		
		
		//消费者
		Properties posProperties = new Properties();
//		posProperties.put("zookeeper.connect", "192.168.10.104:2181");
		posProperties.put("bootstrap.servers", "192.168.10.104:9092");
		posProperties.put("group.id", "test-consumer-group");
		posProperties.put("enable.auto.commit", "false");//如果改为false 则迭代循环获取不到数据
		posProperties.put("auto.commit.interval.ms", "1000");
		/*earliest
		 * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 
		 * latest
		 * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
		 *  none
		 * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
		 */
		posProperties.put("auto.offset.reset", "earliest");// none 但只有一个分区时 不能为none 
		posProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		posProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

	
		KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(posProperties);
		// 订阅消息
		consumer.subscribe(Arrays.asList(topic));
		System.out.println("订阅消息");
		ConsumerRecords<String, Object> records = consumer.poll(1000);
		
//		System.out.println("---------------->>>>>>>接受消息+==========>>>"+records.records(topic));
		
		for (ConsumerRecord<String, Object> record : records) {
//			System.out.println(record.value()+"-------------->>>>>>>>");
//			System.out.println(record.key()+"-------------->>>>>>>>");
			if (record.topic().equals(topic)) {
				System.out.println("主题------->>--------->"+record.topic());//topic 
				System.out.println("键------->>--------->"+record.key());//key
				System.out.println("分区------->>--------->"+record.partition());//分区
				System.out.println("时间戳------->>--------->"+record.timestamp());//创建时间
				System.out.println("偏移------->>--------->"+record.offset());//偏移
				System.out.println("值------->>--------->"+record.value());//消息体
				System.out.println("类型------->>--------->"+record.timestampType());//时间类型？？
//				System.out.printf("offset = %d, key = %s, value = %s%n,timestamp=%s,timestampType = %s",
//						record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());
//				System.out.print("添加成功");
				consumer.close();
				return true;
			}
		}
		consumer.close();
		return false;
	}
}
