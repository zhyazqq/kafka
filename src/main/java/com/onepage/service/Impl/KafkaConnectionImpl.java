package com.onepage.service.Impl;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import org.thymeleaf.expression.Sets;

import com.alibaba.fastjson.JSONObject;
import com.onepage.dao.Student;
import com.onepage.mapper.AddMapper;
import com.onepage.service.KafkaConnection;

@Service
public class KafkaConnectionImpl implements KafkaConnection {
	// 启动zookeeper
	// 启动kafka命令 在安装目录 bin文件夹那一层 不要进入bin文件 .\bin\windows\kafka-server-start.bat
	// .\config\server.properties
	//
//查看消息命令 在安装目录 bin文件夹那一层 不要进入bin文件 shift+右键开启shell 输入  .\bin\windows\kafka-console-consumer.bat --bootstrap-server 192.168.10.104:9092 --topic test_topic_1 --from-beginning
	@Resource
	private AddMapper add;

	private static int ID = 1;

	@Override
	public boolean isConnection(String IP, Integer port) {
		// 生产者
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put("zookeeper.connect", "192.168.10.104:2181");
		props.put("bootstrap.servers", "192.168.10.104:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		String topic = "test_topic_2";
		Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
		Long time = System.currentTimeMillis();
		ID = add.selectMax();
		ID++;
		Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
		Student student = Student.builder().id(ID).name("ceshi").timestamp(timestamp).date(new Date())
				.time(new Time(time)).datetime(timestamp).year(Year.now()).build();
		add.add(student);
//		producer.send(
//				new ProducerRecord<String, Object>(topic, 0, "message", "" + JSONObject.toJSONString(student) + ""));
//		producer.close();

		// 消费者
		Properties posProperties = new Properties();
//		posProperties.put("zookeeper.connect", "192.168.10.104:2181");
		posProperties.put("bootstrap.servers", "192.168.10.104:9092");
		posProperties.put("group.id", "test-consumer-group3");
		posProperties.put("enable.auto.commit", "true");// 如果改为false 则迭代循环获取不到数据
		posProperties.put("auto.commit.interval.ms", "1000");
		posProperties.put("max.poll.records", 500);// 最大一次性拉取的条数
		/*
		 * earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 latest
		 * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 none
		 * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
		 */
		posProperties.put("auto.offset.reset", "latest");// none 但只有一个分区时 不能为none
		posProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		posProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(posProperties);

		TopicPartition topicPartition = new TopicPartition(topic, 0);

		consumer.assign(Arrays.asList(topicPartition));
		consumer.seek(topicPartition, 10);
		
		Map<String, List<PartitionInfo>> map = consumer.listTopics();
		
		Set<String> key = map.keySet();
		for (String s : key) {
			System.out.println("topic=======*********>>>>>>>" + s);
			List<PartitionInfo> lsit = map.get(s);
			System.out.println("value----------->>>>>>>");
			lsit.forEach(x -> {
				System.out.println("topic------" + x.topic());
				System.out.println("partition--------" + x.partition());
				System.out.println("inSyncReplicas-------" + x.inSyncReplicas());
				System.out.println("offlineReplicas-----------" + x.offlineReplicas());
				System.out.println("leader------------" + x.leader());
				System.out.println("replicas----------" + x.replicas());
			});
		}
//		System.out.println(map);
		System.out.println("===============-----------******000******---------==============");

		// 订阅消息
//		consumer.subscribe(Arrays.asList(topic));
//		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
//
//			@Override
//			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//				// TODO Auto-generated method stub
////				System.out.println("here");
////				System.out.println(partitions);
//			}
//
//			@Override
//			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//				// TODO Auto-generated method stub
////				System.out.println("get here");
//
////				consumer.beginningOffsets(partitions);
//				consumer.seekToBeginning(partitions);
////				System.out.println(partitions);
////				System.out.println("get here");
//			}
//		});
//		
//		System.out.println("订阅消息");

//		ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));

		for (;;) {
//		System.out.println("---------------->>>>>>>接受消息+==========>>>"+records.records(topic));
			ConsumerRecords<String, Object> records = consumer.poll(100);
//			System.out.println(records);
//			System.out.println(records.isEmpty());
//			System.out.println(records.partitions().isEmpty());
//			for (TopicPartition partition : records.partitions()) {
//				List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
			for (ConsumerRecord<String, Object> record : records) {
//			System.out.println(record.value()+"-------------->>>>>>>>");
//			System.out.println(record.key()+"-------------->>>>>>>>");
				TopicPartition topicPartition1 = new TopicPartition(topic, 0);
				records.records(topicPartition1);
				if (record.topic().equals(topic)) {
					System.out.println("主题------->>--------->" + record.topic());// topic
					System.out.println("键------->>--------->" + record.key());// key
					System.out.println("分区------->>--------->" + record.partition());// 分区
					System.out.println("时间戳------->>--------->" + record.timestamp());// 创建时间
					System.out.println("偏移------->>--------->" + record.offset());// 偏移
					System.out.println("值------->>--------->" + record.value());// 消息体
					System.out.println("类型------->>--------->" + record.timestampType());// 时间类型？？
//				System.out.printf("offset = %d, key = %s, value = %s%n,timestamp=%s,timestampType = %s",
//						record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());
//				System.out.print("添加成功");
//						consumer.close();
//						return true;
				}
//					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
		}
//		consumer.close();
//		return false;
	}
}
