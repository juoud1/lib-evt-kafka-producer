package com.dobatii.synanto.lrnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class LibEvtProducerAutoCreatingConfig {
	
	@Value("${spring.kafka.topic.name}")
	public String topicName;
	
	@Value("${spring.kafka.topic.nombre-partitions}")
	public Integer nbrePartitions;
	
	@Value("${spring.kafka.topic.nombre-replicas}")
	public Integer nbreReplicas;
	
	@Bean
	public NewTopic libEvtsTopic() {
		
		IO.println("Nom de topic = " + topicName);
		
		return TopicBuilder
				.name(topicName)
				.partitions(nbrePartitions)
				.replicas(nbreReplicas)
				.build();
	}
}
