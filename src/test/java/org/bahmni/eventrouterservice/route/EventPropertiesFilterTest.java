package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.util.json.JsonObject;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.FilterBy;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.RouteDescription;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventPropertiesFilterTest {

    @Test
    public void givenFilterConditions_whenApplied_thenShouldReturnAMatch() throws IOException {

        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnProperties = new LinkedHashMap<>();
        filterOnProperties.put("confirmedPatient", "true");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getEventProperties()).thenReturn(filterOnProperties);

        EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(new ObjectMapper(), routeDescription);

        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        File routeConfigurationFile = new FileSystemResource("src/test/resources/test-patient.json").getFile();
        JsonObject payload = new ObjectMapper().readValue(routeConfigurationFile, JsonObject.class);
        when(message.getBody(String.class)).thenReturn(payload.toJson());

        boolean matches = eventPropertiesFilter.matches(exchange);

        Assertions.assertTrue(matches);
    }

    @Test
    public void givenFilterConditions_whenApplied_thenShouldReturnANOMatch() {

        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnProperties = new LinkedHashMap<>();
        filterOnProperties.put("uuid1", "patientUuid");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getEventProperties()).thenReturn(filterOnProperties);

        EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(new ObjectMapper(), routeDescription);

        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(String.class)).thenReturn("{\"uuid\":\"patientUuid\"}");

        boolean matches = eventPropertiesFilter.matches(exchange);

        Assertions.assertFalse(matches);
    }

    @Test
    public void givenNoFilterConditions_whenApplied_thenShouldReturnAMatch() {

        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getEventProperties()).thenReturn(Maps.newLinkedHashMap());

        EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(new ObjectMapper(), routeDescription);

        Exchange exchange = mock(Exchange.class);

        boolean matches = eventPropertiesFilter.matches(exchange);

        Assertions.assertTrue(matches);
    }

    @Test
    public void givenFilterConditions_whenAppliedOnInvalidPayload_thenShouldThrowException() {

        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnProperties = new LinkedHashMap<>();
        filterOnProperties.put("uuid", "patientUuid");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getEventProperties()).thenReturn(filterOnProperties);

        EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(new ObjectMapper(), routeDescription);

        Exchange exchange = mock(Exchange.class);
        Message message = mock(Message.class);
        when(exchange.getIn()).thenReturn(message);
        when(message.getBody(String.class)).thenReturn("{uuid\":\"patientUuid\"}");

        assertThrows(RuntimeException.class, () -> eventPropertiesFilter.matches(exchange));
    }
}