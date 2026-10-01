package com.dobatii.synanto.lrnkafka.unit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.dobatii.synanto.lrnkafka.controller.LibEvtController;
import com.dobatii.synanto.lrnkafka.domain.LibEvt;
import com.dobatii.synanto.lrnkafka.service.LibEvtService;
import com.dobatii.synanto.lrnkafka.util.TestUtil;

import tools.jackson.databind.ObjectMapper;

@WebMvcTest(LibEvtController.class)
//@AutoConfigureTestRestTemplate
class LibEvtControllerTest {
	
	@Autowired
	MockMvc mockMvc;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@MockitoBean
	LibEvtService libEvtServiceMock;
	
	@Test
	void testCreateLibEvt() throws Exception {
		//GIVEN
		var libEvt_withId = TestUtil.libEvtRecord_withLibEvtId();
		var inputJson = objectMapper.writeValueAsString(libEvt_withId);
		
		//WHEN
		when(libEvtServiceMock.createLibEvt(isA(LibEvt.class)))
				.thenReturn(Optional.ofNullable(libEvt_withId));
		
		mockMvc
			.perform(MockMvcRequestBuilders.post("/v1/libevent")
			.content(inputJson)
			.contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().is2xxSuccessful());
			
		
		//THEN
		
	}

}
