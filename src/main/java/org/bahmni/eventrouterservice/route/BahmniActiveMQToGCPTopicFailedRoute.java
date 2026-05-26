package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.routepolicy.quartz.CronScheduledRoutePolicy;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.RouteDescription;
import org.bahmni.eventrouterservice.model.Topic;
import org.bahmni.eventrouterservice.route.ExpirationTimeProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static org.apache.camel.LoggingLevel.ERROR;
import static org.apache.camel.LoggingLevel.INFO;

@Component
@ConditionalOnProperty(name = "bahmni.activemqToGCP.failed-route-enabled", havingValue = "true")
public class BahmniActiveMQToGCPTopicFailedRoute extends RouteBuilder {

    private final RouteDescriptionLoader routeDescriptionLoader;
    private final String googlePubSubProjectId;
    private final String serviceName;
    private final ObjectMapper objectMapper;
    private final BahmniAPIGateway bahmniAPIGateway;
    private final Long messageTtlInMillis;

    public BahmniActiveMQToGCPTopicFailedRoute(CamelContext context,
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

            CronScheduledRoutePolicy routePolicyForFailedRoute = new CronScheduledRoutePolicy();
            routePolicyForFailedRoute.setRouteStopTime(routeDescription.getErrorDestination().getCronExpressionForRetryStop());

            EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(objectMapper, routeDescription);
            PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(objectMapper, routeDescription, bahmniAPIGateway);
            EventProcessor eventProcessor = new EventProcessor(routeDescription);
            DerivedPropertiesGenerator derivedPropertiesGenerator = new DerivedPropertiesGenerator(routeDescription);
            ExpirationTimeProcessor expirationTimeProcessor = new ExpirationTimeProcessor(messageTtlInMillis);

            String sourceTopic = routeDescription.getErrorDestination().getQueue().getName();

            if(routeDescription.getHealthCheckDestination() != null) {
                configureHealthCheckRoute(routeDescription);
            } else {
                routePolicyForFailedRoute.setRouteStartTime(routeDescription.getErrorDestination().getCronExpressionForRetryStart());
            }

            from("activemq:queue:" + sourceTopic)
                .routeId(sourceTopic)
                .routePolicy(routePolicyForFailedRoute)
                .noAutoStartup()
                .onException(Exception.class)
                    .handled(true)
                    .log(ERROR, "========== EXCEPTION OCCURRED ==========")
                    .log(ERROR, "Exception Type: ${exception.class}")
                    .log(ERROR, "Exception Message: ${exception.message}")
                    .log(ERROR, "Following exception occurred : ${exception.message} for processing the payload")
                    .log(ERROR, "=========================================")
                    // Log route description values
                    .process(exchange -> {
                        System.out.println("========== ROUTE DESCRIPTION VALUES ==========");
                        System.out.println("Source Topic: " + sourceTopic);
                        System.out.println("Service Name: " + serviceName);
                        System.out.println("Google PubSub Project ID: " + googlePubSubProjectId);
                        System.out.println("Route Description: " + routeDescription);
                        if (routeDescription != null) {
                            System.out.println("Error Destination: " + routeDescription.getErrorDestination());
                            if (routeDescription.getErrorDestination() != null) {
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
                        System.out.println("==============================================");
                    })
                    .useOriginalMessage()
                    .redeliveryDelay(routeDescription.getErrorDestination().getRetryDeliveryDelayInMills())
                    .maximumRedeliveries(routeDescription.getErrorDestination().getMaxRetryDelivery())
                    // LOG BEFORE expirationTimeProcessor
                    .process(exchange -> {
                        System.out.println("========== BEFORE expirationTimeProcessor ==========");
                        var msg = exchange.getIn();
                        System.out.println("Message ID: " + msg.getMessageId());
                        System.out.println("JMSExpiration Before: " + msg.getHeader("JMSExpiration"));
                        System.out.println("Header Keys: " + msg.getHeaders().keySet());
                        System.out.println("====================================================");
                    })
                    .process(expirationTimeProcessor)
                    // LOG AFTER expirationTimeProcessor
                    .process(exchange -> {
                        System.out.println("========== AFTER expirationTimeProcessor ==========");
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
                                System.out.println("Current Time: " + now);
                                System.out.println("Expiration Time: " + exp);
                                System.out.println("TTL (ms): " + ttl);
                                System.out.println("TTL (seconds): " + (ttl / 1000));
                            } catch (Exception e) {
                                System.out.println("ERROR parsing expiration: " + e.getMessage());
                            }
                        } else {
                            System.out.println("WARNING: JMSExpiration is NULL after processor!");
                        }
                        System.out.println("===================================================");
                    })
                    .to("activemq:queue:"+serviceName+"-dlq")
                    .log(ERROR, "Failed Message sent to "+serviceName+"-dlq with TTL")
                .end()
                .log(INFO, "Received failed message from ActiveMQ queue : " + sourceTopic)
                .filter(eventPropertiesFilter)
                .process(derivedPropertiesGenerator)
                .filter(patientPropertiesFilter)
                .process(eventProcessor)
                .toD("google-pubsub:"+googlePubSubProjectId+":"+"${exchangeProperty.destination}")
                .log("Message sent to Google PubSub on topic : "+"${exchangeProperty.destination}");
            });
    }

    private void configureHealthCheckRoute(RouteDescription routeDescription) {

        String sourceTopic = routeDescription.getErrorDestination().getQueue().getName();
        Topic healthCheckTopic = routeDescription.getHealthCheckDestination().getTopic();

        CronScheduledRoutePolicy routePolicyForHealthCheck = new CronScheduledRoutePolicy();
        routePolicyForHealthCheck.setRouteStartTime(routeDescription.getErrorDestination().getCronExpressionForRetryStart());
        routePolicyForHealthCheck.setRouteStopTime(routeDescription.getErrorDestination().getCronExpressionForRetryStop());

        String routeId = "healthCheck-" + sourceTopic;
        from("timer:healthCheckGCP?repeatCount=1")
            .routeId(routeId)
            .routePolicy(routePolicyForHealthCheck)
            .noAutoStartup()
            .log("Checking GCP pub sub connectivity by sending test event to topic : "+ healthCheckTopic.getName() +"...")
            .onException(Exception.class)
                .handled(true)
                .log("GCP Connection unsuccessful...Skip processing failed events from : "+ sourceTopic)
            .end()
            .to("google-pubsub:"+googlePubSubProjectId+":"+ healthCheckTopic.getName())
            .log("GCP Connection successful...Start processing events from : "+ sourceTopic)
            .to("controlbus:route?routeId="+ sourceTopic +"&action=start");
    }
}
