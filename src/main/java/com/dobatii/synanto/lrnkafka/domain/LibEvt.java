package com.dobatii.synanto.lrnkafka.domain;

import java.math.BigInteger;

/**
 * Library event DTO
 * 
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

public record LibEvt(BigInteger libEvtId, LibEvtType libEvtType, Book book) {

}
