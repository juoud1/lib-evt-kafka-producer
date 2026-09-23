package com.dobatii.synanto.lrnkafka.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import com.dobatii.synanto.lrnkafka.domain.LibEvt;
import com.dobatii.synanto.lrnkafka.util.TestUtil;

//@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@EmbeddedKafka(topics = "lib-evts-topic")
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
	"spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers"})
class LibEvtControllerIntegrationTest {
	
	@Autowired
	private TestRestTemplate restTemplate;
	
	
//	@BeforeAll
//	static void setUpBeforeClass() throws Exception {
//		
//	}

//	@AfterAll
//	static void tearDownAfterClass() throws Exception {
//	}

//	@BeforeEach
//	void setUp(WebApplicationContext context) throws Exception {
////		restClient = RestTestClient.bindToApplicationContext(context);
////		restClient =RestTestClient.bindToServer()
////						.baseUrl("http://localhost:8181")
////					//	.bindToController(new LibEvtController())
////						.baseUrl("/producer")
////						.build();
	
//	}
//
//	@AfterEach
//	void tearDown() throws Exception {
//	}
	
	//@Disabled
	@Test
	void testCreateLibEvt() {
		
		//Given
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
				
		HttpEntity<LibEvt> httpEntity = new HttpEntity<LibEvt>(TestUtil.libEvtRecord_withLibEvtId(), httpHeaders);
				
		//When 
		//https://docs.spring.io/spring-framework/reference/testing/resttestclient.html
//				RestTestClient.ResponseSpec spec = restClient.post().uri("/v1/libevent")
//					.body(httpEntity)
//					.exchange(); //.expectBody(LibEvt.class);
					//.isEqualTo(TestUtil.libEvtRecord_noLibEvtId());
					
		ResponseEntity<LibEvt> responseEntity = restTemplate.exchange("/v1/libevent", HttpMethod.POST, httpEntity, LibEvt.class);
				
		IO.println("responseEntity = " + responseEntity.toString());
				
		//Then
//		RestTestClientResponse response = RestTestClientResponse.from(spec);
//		assertThat(response).hasStatus2xxSuccessful();
				
				
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
//		assertEquals(HttpStatusCode, responseEntity.get);
				
		//fail("Not yet implemented");
	}

}
