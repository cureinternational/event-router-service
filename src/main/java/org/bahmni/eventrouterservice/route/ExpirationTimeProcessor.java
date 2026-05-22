package org.bahmni.eventrouterservice.route;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

/**
 * Processor that explicitly sets the JMS expiration time for messages.
 *
 * This is a more reliable way to set TTL compared to the ?timeToLive= URL parameter,
 * as it directly sets the JMSExpiration header that ActiveMQ respects.
 *
 * The JMSExpiration header is checked by ActiveMQ during expireMessagesPeriod checks,
 * and messages past their expiration time are automatically deleted.
 */
@Slf4j
public class ExpirationTimeProcessor implements Processor {
    private final Long messageTimeToLiveInMillis;

    public ExpirationTimeProcessor(Long messageTimeToLiveInMillis) {
        this.messageTimeToLiveInMillis = messageTimeToLiveInMillis;
    }

    @Override
    public void process(Exchange exchange) {
        System.out.println("========== INSIDE ExpirationTimeProcessor ==========");
        System.out.println("Method called successfully");
        System.out.println("Message ID: " + exchange.getIn().getMessageId());
        System.out.println("messageTimeToLiveInMillis: " + messageTimeToLiveInMillis);

        if (messageTimeToLiveInMillis != null && messageTimeToLiveInMillis > 0) {
            long currentTimeMillis = System.currentTimeMillis();
            long expirationTime = currentTimeMillis + messageTimeToLiveInMillis;

            System.out.println("Current Time: " + currentTimeMillis);
            System.out.println("Calculated Expiration Time: " + expirationTime);
            System.out.println("TTL Duration (ms): " + messageTimeToLiveInMillis);
            System.out.println("TTL Duration (seconds): " + (messageTimeToLiveInMillis / 1000));

            try {
                // Set the JMS Expiration header
                // This tells ActiveMQ when this message should expire
                exchange.getIn().setHeader("JMSExpiration", expirationTime);
                System.out.println("✓ Successfully set JMSExpiration header");
                System.out.println("✓ Value set: " + exchange.getIn().getHeader("JMSExpiration"));
                System.out.println("✓ Value type: " + exchange.getIn().getHeader("JMSExpiration").getClass().getName());

                // Also log for debugging
                log.info("Set message expiration. Current time: {}, TTL: {} ms, Expiration time: {}",
                        currentTimeMillis, messageTimeToLiveInMillis, expirationTime);
            } catch (Exception e) {
                System.out.println("✗ ERROR setting header: " + e.getMessage());
                e.printStackTrace();
                log.error("Error setting JMSExpiration header: {}", e.getMessage(), e);
            }
        } else {
            System.out.println("✗ WARNING: Message TTL not configured or is 0. Message will not expire.");
            System.out.println("✗ messageTimeToLiveInMillis: " + messageTimeToLiveInMillis);
            log.warn("Message TTL not configured or is 0. Message will not expire.");
        }
        System.out.println("=====================================================");
    }
}
