package org.bahmni.eventrouterservice.configuration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bahmni.eventrouterservice.exception.NoDestinationConfiguredForEventType;
import org.bahmni.eventrouterservice.exception.FailedToLoadConfiguration;
import org.bahmni.eventrouterservice.model.Queue;
import org.bahmni.eventrouterservice.model.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Component
@Slf4j
public class RouteDescriptionLoader {
    private List<RouteDescription> routeDescriptions = new ArrayList<>();

    @Autowired
    public RouteDescriptionLoader(@Value("${route.description.file.location}") String routeConfigurationFileLocation,
                                  ObjectMapper objectMapper) {
        loadConfiguration(routeConfigurationFileLocation, objectMapper);
    }

    private void loadConfiguration(String routeConfigurationFileLocation, ObjectMapper objectMapper) {

        try {
            File routeConfigurationFile = new FileSystemResource(routeConfigurationFileLocation).getFile();
            this.routeDescriptions = objectMapper.readValue(routeConfigurationFile, new TypeReference<>() {});
        } catch (IOException exception) {
            log.error("Failed to load configuration for file : " + routeConfigurationFileLocation);
            throw new FailedToLoadConfiguration(routeConfigurationFileLocation, exception);
        }
    }

    public List<RouteDescription> getRouteDescriptions() {
        return this.routeDescriptions;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RouteDescription {
        private Source source;
        private List<Destination> destinations;
        private ErrorDestination errorDestination;
        private List<AdditionalProperty> additionalProperties;
        private LinkedHashMap<String, String> derivedProperties = new LinkedHashMap<>(0);
        private Destination healthCheckDestination;
        private FilterBy filterBy;

        public Destination getDestinationBasedOn(String eventType) {
            BahmniEventType bahmniEventType = BahmniEventType.valueOf(eventType.toUpperCase());
            return destinations.stream()
                    .filter(destination -> destination.forEventType(bahmniEventType))
                    .findFirst()
                    .orElseThrow(() -> new NoDestinationConfiguredForEventType(bahmniEventType));
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    public static class FilterBy {
        private LinkedHashMap<String, String> eventProperties = new LinkedHashMap<>(0);
        private LinkedHashMap<String, String> patientProperties = new LinkedHashMap<>(0);
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    public static class Source {
        private Topic topic;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Destination {
        private BahmniEventType onEventType;
        private Topic topic;
        private Queue queue;

        public boolean forEventType(BahmniEventType bahmniEventType) {
            return onEventType.equals(bahmniEventType);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    public static class ErrorDestination extends Destination {
        private Integer maxRetryDelivery;
        private Long retryDeliveryDelayInMills;
        private String cronExpressionForRetryStart;
        private String cronExpressionForRetryStop;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AdditionalProperty {
        private String parentPath;
        private String filterKeyPath;
        private List<AdditionalPropertyFilter> additionalPropertyFilters;
        private LinkedHashMap<String, String> staticProperties = new LinkedHashMap<>(0);
        private LinkedHashMap<String, String> dynamicProperties = new LinkedHashMap<>(0);
        private LinkedHashMap<String, SplitPatternConfiguration> splitPatternConfigurations = new LinkedHashMap<>(0);
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AdditionalPropertyFilter {
        private String keyPath;
        private String value;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SplitPatternConfiguration {
        private String splitPattern;
        private Integer splitIndex;
    }

    public enum BahmniEventType {
        BAHMNI_PATIENT_CREATED, BAHMNI_PATIENT_UPDATED, BAHMNI_APPOINTMENT_CREATED, BAHMNI_APPOINTMENT_UPDATED, BAHMNI_ENCOUNTER_CREATED, BAHMNI_ENCOUNTER_UPDATED, BAHMNI_VISIT_CREATED;
    }

    @Getter
    public enum DerivedPropertiesKey {
        PATIENT_UUID("patientUuid");
        private final String value;
        DerivedPropertiesKey(String patientUuid) {
            this.value = patientUuid;
        }
    }
}
