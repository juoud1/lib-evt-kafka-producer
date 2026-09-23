package com.dobatii.synanto.lrnkafka.util;

import java.math.BigInteger;
import java.time.LocalDate;

import com.dobatii.synanto.lrnkafka.domain.Author;
import com.dobatii.synanto.lrnkafka.domain.Book;
import com.dobatii.synanto.lrnkafka.domain.LibEvt;
import com.dobatii.synanto.lrnkafka.domain.LibEvtType;
import com.fasterxml.jackson.core.JsonProcessingException;

import tools.jackson.databind.ObjectMapper;

public class TestUtil {
	
	public static LibEvt libEvtRecord_withLibEvtId() {
		return new  LibEvt(BigInteger.valueOf(2020), LibEvtType.NEW, bookRecord());
	}
	
	public static LibEvt libEvtRecord_noLibEvtId() {
		return new  LibEvt(null, LibEvtType.NEW, bookRecord());
	}
	
	public static LibEvt libEvt_withInvaliBook() {
		return new  LibEvt(null, LibEvtType.NEW, bookRecord_withInvalidValues());
	}
	
	public static LibEvt libEvtRecordUpdate() {
		return new  LibEvt(BigInteger.ONE, LibEvtType.UPDATE, bookRecord());
	}
	
	public static LibEvt parseLibEvtRecord(ObjectMapper objectMapper, String json) throws JsonProcessingException {
		return objectMapper.readValue(json, LibEvt.class);
	}
	
	public static Author authorRecord () {
		return new Author(BigInteger.valueOf(30), "JOU Dobatia");
	}
	
	public static Author authorRecord_withInvalidValue () {
		return new Author(null, "");
	}
	
	public static Book bookRecord () {		
		return new Book(BigInteger.TEN, "Abby 7/1 tome 1", authorRecord(), LocalDate.of(2024, 1, 19));
	}
	
	public static Book bookRecord_withInvalidValues() {
		return new Book(null, "", authorRecord_withInvalidValue(), null);
	}

}
