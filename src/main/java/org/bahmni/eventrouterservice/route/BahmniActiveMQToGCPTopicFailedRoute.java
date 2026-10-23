package org.bahmni.eventrouterservice.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.routepolicy.quartz.CronScheduledRoutePolicy;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader;
import org.bahmni.eventrouterservice.configuration.RouteDescriptionLoader.RouteDescription;
import org.bahmni.eventrouterservice.model.Topic;
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

    public BahmniActiveMQToGCPTopicFailedRoute(CamelContext context,
                                               RouteDescriptionLoader routeDescriptionLoader,
                                               @Value("${google-pubsub.project-id}") String googlePubSubProjectId,
                                               @Value("${service.name}") String serviceName,
                                               ObjectMapper objectMapper,
                                               BahmniAPIGateway bahmniAPIGateway) {
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

            CronScheduledRoutePolicy routePolicyForFailedRoute = new CronScheduledRoutePolicy();
            routePolicyForFailedRoute.setRouteStopTime(routeDescription.getErrorDestination().getCronExpressionForRetryStop());

            EventPropertiesFilter eventPropertiesFilter = new EventPropertiesFilter(objectMapper, routeDescription);
            PatientPropertiesFilter patientPropertiesFilter = new PatientPropertiesFilter(objectMapper, routeDescription, bahmniAPIGateway);
            EventProcessor eventProcessor = new EventProcessor(routeDescription);
            DerivedPropertiesGenerator derivedPropertiesGenerator = new DerivedPropertiesGenerator(routeDescription);

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
                    .log(ERROR, "Following exception occurred : ${exception.message} for processing the payload")
                    .useOriginalMessage()
                    .maximumRedeliveries(0)
                    .to("activemq:queue:"+serviceName+"-dlq?timeToLive=300000") // 5 minutes TTL
                    .log("Failed Message sent to "+serviceName+"-dlq")
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
