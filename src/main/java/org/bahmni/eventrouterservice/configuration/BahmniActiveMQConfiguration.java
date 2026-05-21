package org.bahmni.eventrouterservice.configuration;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import jakarta.jms.ConnectionFactory;

@ConditionalOnExpression("${bahmni.activemqToGCP.failed-route-enabled:true} || ${bahmni.activemqToGCP.route-enabled:true}")
@Configuration
@Profile("!test")
public class BahmniActiveMQConfiguration {

    @Bean
    public ConnectionFactory ConnectionFactory(@Value("${bahmni.activemq.broker-url}") String brokerUrl) {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(brokerUrl);
        return activeMQConnectionFactory;
    }
}