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
	
	@Value("${spring.kafka.topic}")
	public String topicName;
	
	@Bean
	public NewTopic libEvtsTopic() {
		
		IO.println("Nom de topic = " + topicName);
		
		return TopicBuilder
				.name(topicName)
				.partitions(3)
				.replicas(1)
				.build();
	}
}
