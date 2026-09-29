package org.example.notifyservice.consumer;

import org.example.notifyservice.service.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class RentalCreatedConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RentalCreatedConsumer.class);

    private final EmailService emailService;

    public RentalCreatedConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "${notification.kafka.rental-created-topic}")
    public void consume(String email) {
        if (!StringUtils.hasText(email)) {
            LOGGER.warn("Ignoring rental-created event with blank email");
            return;
        }
        emailService.sendRentalCreatedEmail(email.trim());
    }
}
