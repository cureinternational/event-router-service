package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.RouteDescription;

import static org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.DerivedPropertiesKey.PATIENT_UUID;
import static org.bahmni.eventrouterservice.route.EventHeaderKey.PAYLOAD_ID;

@Slf4j
class PatientPropertiesFilter extends PropertiesFilter implements Predicate {

    private final RouteDescription routeDescription;
    private final BahmniAPIGateway bahmniAPIGateway;

    public PatientPropertiesFilter(ObjectMapper objectMapper, RouteDescription routeDescription, BahmniAPIGateway bahmniAPIGateway) {
        super(objectMapper);
        this.routeDescription = routeDescription;
        this.bahmniAPIGateway = bahmniAPIGateway;
    }

    @Override
    public boolean matches(Exchange exchange) {
        log.info("Checking Patient Filters conditions : " + routeDescription.getFilterBy().getPatientProperties());
        if(routeDescription.getFilterBy().getPatientProperties().isEmpty()) {
            log.info("Empty Patient Filters conditions");
            return true;
        }

        String patientUuid = exchange.getProperty(PATIENT_UUID.getValue(), String.class);
        if(patientUuid == null) {
            log.warn("Patient UUID not found in payload — dropping message. eventType: {}",
                     exchange.getIn().getHeader("eventType"));
            return false;
        }
        String patientPayloadAsJson = bahmniAPIGateway.getPatient(patientUuid);
        return super.matches(patientPayloadAsJson, routeDescription.getFilterBy().getPatientProperties());
    }
}