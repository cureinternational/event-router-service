package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader;
import org.bahmni.eventrouterservice.route.ExpirationTimeProcessor;
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
    private final Long messageTtlInMillis;

    public BahmniActiveMQToGCPTopicRoute(CamelContext context,
                                         RouteDescriptionLoader routeDescriptionLoader,
                                         @Value("${google-pubsub.project-id}") String googlePubSubProjectId,
                                         @Value("${service.name}") String serviceName,
                                         ObjectMapper objectMapper,
                                         BahmniAPIGateway bahmniAPIGateway,
                                         @Value("${bahmni.activemq.message-ttl-in-millis}") Long messageTtlInMillis) {
        super(context);
        this.routeDescriptionLoader = routeDescriptionLoader;
        this.googlePubSubProjectId = googlePubSubProjectId;
        this.serviceName = serviceName;
        this.objectMapper = objectMapper;
        this.bahmniAPIGateway = bahmniAPIGateway;
        this.messageTtlInMillis = messageTtlInMillis;
    }

    @Override
    public void configure() {

        routeDescriptionLoader.getRouteDescriptions().forEach(routeDescription -> {

            EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(objectMapper, routeDescription);
            PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(objectMapper, routeDescription, bahmniAPIGateway);
            EventProcessor eventProcessor = new EventProcessor(routeDescription);
            DerivedPropertiesGenerator derivedPropertiesGenerator = new DerivedPropertiesGenerator(routeDescription);
            ExpirationTimeProcessor expirationTimeProcessor = new ExpirationTimeProcessor(messageTtlInMillis);

            String sourceTopic = routeDescription.getSource().getTopic().getName();
            String uniqueClientId = serviceName + ":" + sourceTopic;

            from("activemq:topic:" + sourceTopic + "?durableSubscriptionName=" + sourceTopic + "&clientId=" + uniqueClientId)
                .log(INFO, "Received message from ActiveMQ with headers : ${headers}")
                .onException(Exception.class)
                    .handled(true)
                    .log(ERROR, "========== EXCEPTION OCCURRED IN MAIN ROUTE ==========")
                    .log(ERROR, "Source Topic: " + sourceTopic)
                    .log(ERROR, "Exception Type: ${exception.class}")
                    .log(ERROR, "Error while processing message from ActiveMQ topic : " + sourceTopic + " with exception as : ${exception.message}")
                    .log(ERROR, "====================================================")
                    // Log route description values
                    .process(exchange -> {
                        System.out.println("========== ROUTE DESCRIPTION VALUES (MAIN) ==========");
                        System.out.println("Source Topic: " + sourceTopic);
                        System.out.println("Service Name: " + serviceName);
                        System.out.println("Google PubSub Project ID: " + googlePubSubProjectId);
                        System.out.println("Route Description: " + routeDescription);
                        if (routeDescription != null) {
                            System.out.println("Error Destination: " + routeDescription.getErrorDestination());
                            if (routeDescription.getErrorDestination() != null) {
                                System.out.println("Error Queue: " + routeDescription.getErrorDestination().getQueue().getName());
                                System.out.println("Retry Delivery Delay (ms): " +
                                    routeDescription.getErrorDestination().getRetryDeliveryDelayInMills());
                                System.out.println("Max Retry Delivery: " +
                                    routeDescription.getErrorDestination().getMaxRetryDelivery());
                            } else {
                                System.out.println("ERROR DESTINATION IS NULL!");
                            }
                        } else {
                            System.out.println("ROUTE DESCRIPTION IS NULL!");
                        }
                        System.out.println("Message TTL in Millis: " + messageTtlInMillis);
                        System.out.println("=====================================================");
                    })
                    .useOriginalMessage()
                    .redeliveryDelay(routeDescription.getErrorDestination().getRetryDeliveryDelayInMills())
                    .maximumRedeliveries(routeDescription.getErrorDestination().getMaxRetryDelivery())
                    // LOG BEFORE expirationTimeProcessor
                    .process(exchange -> {
                        System.out.println("========== BEFORE expirationTimeProcessor (MAIN) ==========");
                        var msg = exchange.getIn();
                        System.out.println("Message ID: " + msg.getMessageId());
                        System.out.println("JMSExpiration Before: " + msg.getHeader("JMSExpiration"));
                        System.out.println("===========================================================");
                    })
                    .process(expirationTimeProcessor)
                    // LOG AFTER expirationTimeProcessor
                    .process(exchange -> {
                        System.out.println("========== AFTER expirationTimeProcessor (MAIN) ==========");
                        var msg = exchange.getIn();
                        System.out.println("Message ID: " + msg.getMessageId());
                        Object expiration = msg.getHeader("JMSExpiration");
                        System.out.println("JMSExpiration After: " + expiration);
                        System.out.println("Expiration Type: " + (expiration != null ? expiration.getClass().getName() : "NULL"));

                        if (expiration != null) {
                            try {
                                long exp = Long.parseLong(expiration.toString());
                                long now = System.currentTimeMillis();
                                long ttl = exp - now;
                                System.out.println("TTL (seconds): " + (ttl / 1000));
                            } catch (Exception e) {
                                System.out.println("ERROR parsing expiration: " + e.getMessage());
                            }
                        }
                        System.out.println("===========================================================");
                    })
                    .to("activemq:queue:" + routeDescription.getErrorDestination().getQueue().getName())
                    .log("Message sent to ActiveMQ failed message queue: "+routeDescription.getErrorDestination().getQueue().getName()+" with TTL")
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