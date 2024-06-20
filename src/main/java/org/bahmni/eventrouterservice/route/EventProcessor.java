package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.RouteDescription;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.AdditionalProperty;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.AdditionalPropertyFilter;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.SplitPatternConfiguration;

import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
class EventProcessor implements Processor {

    private final RouteDescription routeDescription;

    public EventProcessor(RouteDescription routeDescription) {
        this.routeDescription = routeDescription;
    }

    @Override
    public void process(Exchange exchange) {
        if(routeDescription.getAdditionalProperties().isEmpty()) {
            log.info("Empty additional properties");
            return;
        }
        String payloadAsJsonString = exchange.getIn().getBody(String.class);
        String updatedPayloadAsJson = addAdditionalProperties(payloadAsJsonString, routeDescription.getAdditionalProperties());
        exchange.getIn().setBody(updatedPayloadAsJson);

        String destinationTopic = getDestination(exchange.getIn().getHeader("eventType"));
        exchange.setProperty("destination", destinationTopic);
    }

    private String getDestination(Object eventType) {
        String eventTypeAsString = (String) eventType;
        return routeDescription.getDestinationBasedOn(eventTypeAsString).getTopic().getName();
    }

    private String addAdditionalProperties(String payloadAsJsonString, List<AdditionalProperty> additionalProperties) {
        try {
            DocumentContext[] contextRef = {JsonPath.parse(payloadAsJsonString)};

            for (AdditionalProperty obj : additionalProperties) {
                String parentPath = obj.getParentPath();
                List<AdditionalPropertyFilter> additionalPropertyFilters = obj.getAdditionalPropertyFilters();
                LinkedHashMap<String, SplitPatternConfiguration> splitPatternConfigurations = obj.getSplitPatternConfigurations();
                if (obj.getStaticProperties() != null && obj.getStaticProperties().size() > 0) {
                    obj.getStaticProperties().entrySet().forEach(entry -> {
                        contextRef[0].put(JsonPath.compile(parentPath), entry.getKey(), entry.getValue());
                    });
                }

                if (obj.getDynamicProperties() != null && obj.getDynamicProperties().size() > 0) {
                    obj.getDynamicProperties().entrySet().forEach(entry -> {
                        Object parentObj = JsonPath.read(payloadAsJsonString, parentPath);
                        if (parentObj instanceof List && !((List<?>) parentObj).isEmpty()) {
                            contextRef[0] = contextRef[0].map(parentPath, (currentValue, configuration) -> {
                                DocumentContext obsContext = JsonPath.parse(currentValue);
                                try {
                                    Object valueObj = JsonPath.read(currentValue, entry.getValue());
                                    if (valueObj != null) {
                                        if (splitPatternConfigurations != null && splitPatternConfigurations.get(entry.getKey()) != null) {
                                            valueObj = applySplitPattern(valueObj, splitPatternConfigurations.get(entry.getKey()));
                                        } 
                                        if (additionalPropertyFilters == null || additionalPropertyFilters.size() == 0 || applyFilter(currentValue, additionalPropertyFilters)) {
                                            obsContext.put(JsonPath.compile("$"), entry.getKey(), valueObj);
                                        }
                                    }
                                } catch (PathNotFoundException e) {
                                    log.info("Path not found: " + entry.getValue());
                                }
                                return obsContext.json();
                            });
                        }
                    });
                }
            }
            return contextRef[0].jsonString();
        } catch (Exception exception) {
            log.info("Failed to process payload for additional properties : " + exception.getMessage());
            throw new RuntimeException(exception);
        }
    }

    private boolean applyFilter(Object value, List<AdditionalPropertyFilter> additionalPropertyFilters) {
        for (AdditionalPropertyFilter filter : additionalPropertyFilters) {
            if (filter.getKeyPath() != null && filter.getValue() != null) {
                Object filterObj = JsonPath.read(value, filter.getKeyPath());
                if (filterObj == null || (filterObj != null && !filterObj.toString().contains(filter.getValue()))) {
                    return false;
                }
            }
        }
        return true;
    }

    private Object applySplitPattern(Object value, SplitPatternConfiguration splitConfig) {
        if (value instanceof String) {
            String[] parts = ((String) value).split(splitConfig.getSplitPattern());
            Integer index = splitConfig.getSplitIndex();
            if (index >= 0 && index < parts.length) {
                return parts[index];
            }
        }
        return value;
    }

}
