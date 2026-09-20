package com.dobatii.synanto.lrnkafka.service;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.dobatii.synanto.lrnkafka.domain.Author;
import com.dobatii.synanto.lrnkafka.domain.Book;
import com.dobatii.synanto.lrnkafka.domain.LibEvt;
import com.dobatii.synanto.lrnkafka.domain.LibEvtType;
import com.dobatii.synanto.lrnkafka.producer.LibEvtProducer;

import lombok.extern.slf4j.Slf4j;

/**
 * Lib Event component responsible for traiting, validating, crating, updating and getting record in kafka
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

@Service
@Slf4j
public class LibEvtService {
	
	private final LibEvtProducer libEvtProducer;
	
	public LibEvtService(LibEvtProducer libEvtProducer) {
		this.libEvtProducer = libEvtProducer;
	}
	
	public Optional<LibEvt> createLibEvt (LibEvt libEvt){
		
		IO.println("Traitements et validation du nouvel évenément "+ libEvt + " avant création");
		//IO.println("Evt recu d'appel REST : " + libEvt);
		
		//Invoke the kafka producer
		libEvtProducer.sendLibEvt(libEvt);
	
		
		IO.println("Événement créé avec succès!");
		return Optional.ofNullable(libEvt);
	}
	
	public List<LibEvt> getLibEvts (){
		
		IO.println("Traitements et validation avant récupération des événements");
		
		//Invoke the kafka producer
		
		
		var libevts = getDummyLibEvts();
		
		IO.println("Événements récupérés avec succès!");
		return new ArrayList<>(libevts);
	}
	
	private List<LibEvt> getDummyLibEvts(){
		
		var levt1 = new LibEvt(BigInteger.valueOf(2020), LibEvtType.OTHER, null);
		
		var book = new Book(BigInteger.valueOf(7), "Abbyéé, bb ti Dongongo", new Author(BigInteger.valueOf(1), "Dongongo baba ti Abby"), LocalDate.of(2024, 1, 17));
		var levt = new LibEvt(BigInteger.valueOf(2020), LibEvtType.NEW, book);
		
		return List.of(levt, levt1);
	}
	
}
