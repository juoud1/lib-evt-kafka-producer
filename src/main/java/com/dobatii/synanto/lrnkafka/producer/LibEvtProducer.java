package com.dobatii.synanto.lrnkafka.producer;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.dobatii.synanto.lrnkafka.domain.LibEvt;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Lib Event component responsible for sending or producing the events to kafka
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

@Component
@Slf4j
public class LibEvtProducer {
	
	@Value("${spring.kafka.topic.name}")
	public String topicName;
	
	@Value("${spring.kafka.topic.nombre-partitions}")
	public Integer nbrePartitions;
	
	@Value("${spring.kafka.producer-record.header.key}")
	public String recordHeaderKey;
	
	@Value("${spring.kafka.producer-record.header.value}")
	public String recordHeaderValue;
	
	private final KafkaTemplate<Integer, String> kafkaTemplate;
	
	private final ObjectMapper objectMapper;
	
	public LibEvtProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}
	
	public CompletableFuture<SendResult<Integer, String>> sendLibEvt(LibEvt libEvent) {
		
		IO.println("Envoi du nouvel évenément "+ libEvent + " au broker encours ...");
		
		// Il faut valider les données avant leur traitement ....
		var intId = libEvent.libEvtId();
		Integer evtKey = intId != null ? libEvent.libEvtId().intValue() : Integer.MIN_VALUE; 
		var evtValue = objectMapper.writeValueAsString(libEvent);
		
		//Trace log
		IO.println("Lib evt value byte serialized = "+ Arrays.toString(objectMapper.writeValueAsBytes(libEvent)) +"\n ");
		IO.println("Lib evt value string serialized = "+ objectMapper.writeValueAsString(libEvent) +"\n ");
		
		//Comment fonctionne cette routine en arrière plan :
		// 1- Blocking call : get metadata about the kafka cluster
		// 2 - If successs : Send message happens and return a COmpletableFuture
		var completableFutureResult = kafkaTemplate.send(topicName, evtKey, evtValue);
		
		return completableFutureResult.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handleFailureSendingEvt(evtKey, evtValue, throwable);
				IO.println("Envoi du nouvel évenément au broker a échoué!");
			} else {
				handleSuccessSendingEvt(evtKey, evtValue, sendResult);
				IO.println("Envoi du nouvel évenément au broker avec succès!");
			}
		});
	}
	
public CompletableFuture<SendResult<Integer, String>> sendLibEvt_withPrducerRecord(LibEvt libEvent) {
		
		IO.println("Envoi du nouvel évenément "+ libEvent + " au broker encours ...");
		
		// Il faut valider les données avant leur traitement ....
		var intId = libEvent.libEvtId();
		Integer evtKey = intId != null ? libEvent.libEvtId().intValue() : Integer.MIN_VALUE; 
		var evtValue = objectMapper.writeValueAsString(libEvent);
		
		//Trace log
		IO.println("Lib evt value byte serialized = "+ Arrays.toString(objectMapper.writeValueAsBytes(libEvent)) +"\n ");
		IO.println("Lib evt value string serialized = "+ objectMapper.writeValueAsString(libEvent) +"\n ");
		
		// Getting producer record
		var producerRecord = buildProducerRecord_withRandomPartitionAndRecordHeaders(evtKey, evtValue, recordHeaderKey, recordHeaderValue);
		IO.println("ProducerRecord avec dummy liste de headers= " + producerRecord.toString());
		
		//Comment fonctionne cette routine en arrière plan :
		// 1- Blocking call : get metadata about the kafka cluster
		// 2 - If successs : Send message happens and return a COmpletableFuture
		
		//var completableFutureResult = kafkaTemplate.send(topicName, evtKey, evtValue);
		var completableFutureResult = kafkaTemplate.send(producerRecord);
		
		return completableFutureResult.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handleFailureSendingEvt(evtKey, evtValue, throwable);
				IO.println("Envoi du nouvel évenément au broker a échoué!");
			} else {
				handleSuccessSendingEvt(evtKey, evtValue, sendResult);
				IO.println("Envoi du nouvel évenément au broker avec succès!");
			}
		});
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord_withRandomPartitionAndRecordHeaders (Integer key, String value) {
		var timestamp = Timestamp.valueOf(LocalDateTime.now());
		var currentTimeMillis = System.currentTimeMillis();
		IO.println("Timestamp de l'évt = " + timestamp + " \n timestamp.getTime() = " + timestamp.getTime() + "\n sys.currenttime = " + currentTimeMillis);
		
		var electedPartition = getRandomPartitionUsingThreadLocalRandom(0, nbrePartitions);
		IO.println("Partition élue pour le stockage de l'évt = " + electedPartition);
		
		return new ProducerRecord<>(topicName, electedPartition, timestamp.getTime(), key, value);
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord_withRandomPartitionAndRecordHeaders (Integer producerRecordKey, String producerRecordValue, String dummyHeaderKey, String dummyHeaderValue) {
		
		// Building dummy list of headers
		var header = buildProducerRecordHeader(dummyHeaderKey, dummyHeaderValue);
		var headers = buildProducerRecordHeaders(header);
		IO.println("Dummy liste des headers = " + headers.toString());
		
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord_withRandomPartitionAndRecordHeaders(producerRecordKey, producerRecordValue);
		IO.println("ProducerRecord sans liste des headers = " + producerRecord.toString());
		
		ProducerRecord<Integer, String> producerRecordWithHeaders = new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(), producerRecord.value(), headers);
		IO.println("ProducerRecord avec dummy liste des headers = " + producerRecordWithHeaders.toString());
		
		return producerRecordWithHeaders;
	}
	
	private Header buildProducerRecordHeader (String headerKey, String headerValue) {
		IO.println("Création de dummy header encours..., \n clé=" + recordHeaderKey + "\n valeur=" + recordHeaderValue);
		Header header = new RecordHeader(headerKey, headerValue.getBytes());
		IO.println("Dummy header créé : " + header.toString());
		IO.println("Création de dummy header avec succès.");
		
		return header;
	}
	
	private List<Header> buildProducerRecordHeaders (Header header) {
		IO.println("Création de la liste des dummies headers encours...");
		
		List<Header> headers = Collections.emptyList();
		if (Objects.nonNull(header)) {
			headers = List.of(header);
		}
		
		IO.println("Création de la liste des dummies headers avec succès.");
		return headers;
	}
	
	private Integer getRandomPartitionUsingThreadLocalRandom (int numPartitionMin, int numPartitionMax) {
		if (numPartitionMin­ > numPartitionMax) {
			IO.println("ERREUR, LA PARTITION MINIMALE MAL-DÉFINIE!");
			throw new IllegalArgumentException("EREUR, LA PARTITION MINIMALE NE PEUT PAS ÊTRE PLUS GRANDE QUE LA PARTITION MAXIMALE.");
		}
		
		return ThreadLocalRandom.current().nextInt(numPartitionMin, numPartitionMax);
	}
	
	private void handleFailureSendingEvt(Integer evtKey, String evtValue, Throwable throwable) {
		IO.println("Erreur lors de l'envoi du message et l'exception est " + throwable.getMessage());
	}
	
	private void handleSuccessSendingEvt(Integer evtKey, String evtValue, SendResult<Integer, String> sendResult) {
		
		IO.println("Explorer l'objet SendResult et sa méthode getRecordMetadata() :");
		//IO.println();
		IO.println("L'envoi de message au broker effectué avec succès, clé = " + evtKey + " ; valeur = " + evtValue + " et la partition est " + sendResult.getRecordMetadata().partition() + "\n \n");
		
		//Explorer l'objet SendResult
		IO.println("Explorer l'objet SendResult et sa méthode getProducerRecord() :");
		//IO.println();
		IO.println("L'envoi de message au broker effectué avec succès, objet ProducerRecord = " + sendResult.getProducerRecord().toString());
		IO.println();
		
		/*
		IO.println("L'envoi de message au broker effectué avec succès, topic = " + sendResult.getProducerRecord().topic() + 
				"\n Header du record = " + sendResult.getProducerRecord().partition().toString() +
				"\n Header du record = " + sendResult.getProducerRecord().headers().toString() +
				"\n Header du record = " + sendResult.getProducerRecord().value() +
				"\n Header du record = " + sendResult.getProducerRecord().timestamp());
		*/
	}
}
