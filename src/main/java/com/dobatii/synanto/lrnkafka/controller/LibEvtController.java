package com.dobatii.synanto.lrnkafka.controller;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.dobatii.synanto.lrnkafka.domain.LibEvt;
import com.dobatii.synanto.lrnkafka.service.LibEvtService;

import lombok.extern.slf4j.Slf4j;

/**
 * Lib Event endpoint that exposes all services to do with record in kafka
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */
@RestController
@Slf4j
public class LibEvtController {
	
	private final LibEvtService libEvtService;
	
	public LibEvtController (LibEvtService libEvtService) {
		this.libEvtService = libEvtService;
	}
	
	@PostMapping("/v1/libevent")
	public ResponseEntity<Optional<LibEvt>> createLibEvt (@RequestBody LibEvt libEvt){
		
		IO.println("Création d'un nouvel évenément encours ...");
		IO.println("Evt recu d'appel REST : " + libEvt);
		
		//Invoke the service respinsable of creating record in kafka
		var levt = libEvtService.createLibEvt(libEvt);
		
		IO.println("Création de nouvel événement avec succès!");
		return ResponseEntity.status(HttpStatus.CREATED).body(levt);
	}
	
	@GetMapping("/v1/libevent")
	public ResponseEntity<List<LibEvt>> getLibEvts (){
		
		IO.println("Récupération des évenément encours ...");
		
		//Invoke the service respinsable of traiting and getting record in kafka
		var levts = libEvtService.getLibEvts();
		
		IO.println("Récupération des évenément avec succès!");
		
		return ResponseEntity.status(HttpStatus.FOUND).body(levts);
	}


}
