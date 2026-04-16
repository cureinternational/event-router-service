package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.camel.Exchange;
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

import static org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.DerivedPropertiesKey.PATIENT_UUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PatientPropertiesFilterTest {

    @Test
    public void givenFilterConditions_whenApplied_thenShouldReturnAMatch() throws IOException {

        BahmniAPIGateway bahmniAPIGateway = mock(BahmniAPIGateway.class);
        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnPatientProperties = new LinkedHashMap<>();
        filterOnPatientProperties.put("confirmedPatient", "true");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getPatientProperties()).thenReturn(filterOnPatientProperties);

        PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(new ObjectMapper(), routeDescription, bahmniAPIGateway);

        Exchange exchange = mock(Exchange.class);
        String patientUUID = "5c7506d6-0e42-43f4-a770-c525ccce7796";
        when(exchange.getProperty(PATIENT_UUID.getValue(), String.class)).thenReturn(patientUUID);
        File routeConfigurationFile = new FileSystemResource("src/test/resources/test-patient.json").getFile();
        JsonObject payload = new ObjectMapper().readValue(routeConfigurationFile, JsonObject.class);
        when(bahmniAPIGateway.getPatient(patientUUID)).thenReturn(payload.toJson());

        boolean matches = patientPropertiesFilter.matches(exchange);

        Assertions.assertTrue(matches);
    }

    @Test
    public void givenFilterConditions_whenAppliedOnOldPatientFormat_thenShouldReturnAMatch() throws IOException {

        BahmniAPIGateway bahmniAPIGateway = mock(BahmniAPIGateway.class);
        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnPatientProperties = new LinkedHashMap<>();
        filterOnPatientProperties.put("display", "confirmedPatient = true");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getPatientProperties()).thenReturn(filterOnPatientProperties);

        PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(new ObjectMapper(), routeDescription, bahmniAPIGateway);

        Exchange exchange = mock(Exchange.class);
        String patientUUID = "25447fd7-1082-46ef-aa39-38914475f52e";
        when(exchange.getProperty(PATIENT_UUID.getValue(), String.class)).thenReturn(patientUUID);
        String oldFormatPatientJson = "{\"uuid\":\"25447fd7-1082-46ef-aa39-38914475f52e\","
                + "\"person\":{\"attributes\":[{\"display\":\"confirmedPatient = true\","
                + "\"value\":true,\"attributeType\":{\"display\":\"confirmedPatient\"}}]}}";
        when(bahmniAPIGateway.getPatient(patientUUID)).thenReturn(oldFormatPatientJson);

        boolean matches = patientPropertiesFilter.matches(exchange);

        Assertions.assertTrue(matches);
    }

    @Test
    public void givenFilterConditions_whenApplied_thenShouldReturnANOMatch() {

        BahmniAPIGateway bahmniAPIGateway = mock(BahmniAPIGateway.class);
        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnPatientProperties = new LinkedHashMap<>();
        filterOnPatientProperties.put("display", "confirmedPatient = true");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getPatientProperties()).thenReturn(filterOnPatientProperties);

        PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(new ObjectMapper(), routeDescription, bahmniAPIGateway);

        Exchange exchange = mock(Exchange.class);
        String patientUUID = "25447fd7-7777-46ef-aa39-38914475f52e";
        when(exchange.getProperty(PATIENT_UUID.getValue(), String.class)).thenReturn(patientUUID);
        when(bahmniAPIGateway.getPatient(patientUUID)).thenReturn("{\"display\":\"confirmedPatient = false\"}");

        boolean matches = patientPropertiesFilter.matches(exchange);

        Assertions.assertFalse(matches);
    }

    @Test
    public void givenNoFilterConditions_whenApplied_thenShouldReturnAMatch() {

        BahmniAPIGateway bahmniAPIGateway = mock(BahmniAPIGateway.class);
        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getPatientProperties()).thenReturn(Maps.newLinkedHashMap());

        PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(new ObjectMapper(), routeDescription, bahmniAPIGateway);

        Exchange exchange = mock(Exchange.class);

        boolean matches = patientPropertiesFilter.matches(exchange);

        Assertions.assertTrue(matches);
    }

    @Test
    public void givenFilterConditionsAndPatientInfoNotPresent_whenApplied_thenShouldThrowException() {

        BahmniAPIGateway bahmniAPIGateway = mock(BahmniAPIGateway.class);
        RouteDescription routeDescription = mock(RouteDescription.class);
        FilterBy filterBy = mock(FilterBy.class);
        LinkedHashMap<String, String> filterOnPatientProperties = new LinkedHashMap<>();
        filterOnPatientProperties.put("display", "confirmedPatient = true");
        when(routeDescription.getFilterBy()).thenReturn(filterBy);
        when(filterBy.getPatientProperties()).thenReturn(filterOnPatientProperties);

        PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(new ObjectMapper(), routeDescription, bahmniAPIGateway);

        Exchange exchange = mock(Exchange.class);
        when(exchange.getProperty(PATIENT_UUID.getValue(), String.class)).thenReturn(null);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> patientPropertiesFilter.matches(exchange));

        Assertions.assertEquals(exception.getMessage(), "Patient Details not found");
    }
}