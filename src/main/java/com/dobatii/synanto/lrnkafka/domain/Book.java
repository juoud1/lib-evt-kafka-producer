package com.dobatii.synanto.lrnkafka.domain;

import java.math.BigInteger;
import java.time.LocalDate;

/**
 *  Book DTO
 *  
 * @author juoud
 * @since 2026
 * @version 1.0 
 */

public record Book(BigInteger bookId, String bookName, Author bookAuthor, LocalDate pubDate) {

}
