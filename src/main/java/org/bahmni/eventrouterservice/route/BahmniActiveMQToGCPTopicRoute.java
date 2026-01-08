package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static org.apache.camel.LoggingLevel.ERROR;
import static org.apache.camel.LoggingLevel.INFO;

@Component
@ConditionalOnProperty(name = "bahmni.activemqToGCP.route-enabled", havingValue = "true")
public class BahmniActiveMQToGCPTopicRoute extends RouteBuilder {

    private final RouteDescriptionLoader routeDescriptionLoader;
    private final String googlePubSubProjectId;
    private final String serviceName;
    private final ObjectMapper objectMapper;
    private final BahmniAPIGateway bahmniAPIGateway;

    public BahmniActiveMQToGCPTopicRoute(CamelContext context,
                                         RouteDescriptionLoader routeDescriptionLoader,
                                         @Value("${google-pubsub.project-id}") String googlePubSubProjectId,
                                         @Value("${service.name}") String serviceName,
                                         ObjectMapper objectMapper, BahmniAPIGateway bahmniAPIGateway) {
        super(context);
        this.routeDescriptionLoader = routeDescriptionLoader;
        this.googlePubSubProjectId = googlePubSubProjectId;
        this.serviceName = serviceName;
        this.objectMapper = objectMapper;
        this.bahmniAPIGateway = bahmniAPIGateway;
    }

    @Override
    public void configure() {

        routeDescriptionLoader.getRouteDescriptions().forEach(routeDescription -> {

            EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(objectMapper, routeDescription);
            PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(objectMapper, routeDescription, bahmniAPIGateway);
            EventProcessor eventProcessor = new EventProcessor(routeDescription);
            DerivedPropertiesGenerator derivedPropertiesGenerator = new DerivedPropertiesGenerator(routeDescription);

            String sourceTopic = routeDescription.getSource().getTopic().getName();
            String uniqueClientId = serviceName + ":" + sourceTopic;

            from("activemq:topic:" + sourceTopic + "?durableSubscriptionName=" + sourceTopic + "&clientId=" + uniqueClientId)
                .log(INFO, "Received message from ActiveMQ with headers : ${headers}")
                .onException(Exception.class)
                    .handled(true)
                    .log(ERROR, "Error while processing message from ActiveMQ topic : " + sourceTopic + " with exception as : ${exception.message}")
                    .useOriginalMessage()
                    .redeliveryDelay(routeDescription.getErrorDestination().getRetryDeliveryDelayInMills())
                    .maximumRedeliveries(routeDescription.getErrorDestination().getMaxRetryDelivery())
                    .toD("activemq:queue:" + routeDescription.getErrorDestination().getQueue().getName() + "?timeToLive=3888000000")
                    .log("Message sent to ActiveMQ failed message queue: "+routeDescription.getErrorDestination().getQueue().getName())
                .end()
                .filter(eventPropertiesFilter)
                .process(derivedPropertiesGenerator)
                .filter(patientPropertiesFilter)
                .process(eventProcessor)
                .toD("google-pubsub:"+googlePubSubProjectId+":"+"${exchangeProperty.destination}")
                .log("Message sent to Google PubSub on topic : "+"${exchangeProperty.destination}");
        });
    }
}