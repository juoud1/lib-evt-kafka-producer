package com.dobatii.synanto.lrnkafka.producer;

import java.util.concurrent.CompletableFuture;

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
	
	@Value("${spring.kafka.topic}")
	public String topicName;
	
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
		
		IO.println("Lib evt value byte serialized = "+ objectMapper.writeValueAsBytes(libEvent));
		
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
	
	private void handleFailureSendingEvt(Integer evtKey, String evtValue, Throwable throwable) {
		IO.println("Erreur lors de l'envoi du message et l'exception est " + throwable.getMessage());
	}
	
	private void handleSuccessSendingEvt(Integer evtKey, String evtValue, SendResult<Integer, String> sendResult) {
		IO.println("L'envoi de message au broker effectué avec succès, clé = " + evtKey + " ; valeur = " + evtValue + " et la partition est " + sendResult.getRecordMetadata().partition());
	}
}
