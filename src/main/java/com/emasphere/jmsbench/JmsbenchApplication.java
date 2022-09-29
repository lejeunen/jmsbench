package com.emasphere.jmsbench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;

import static java.time.LocalTime.now;

@SpringBootApplication
@RestController
@EnableAsync
public class JmsbenchApplication {
    private static final Logger logger = LoggerFactory.getLogger(JmsbenchApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(JmsbenchApplication.class, args);
    }

    private final JmsTemplate jmsTemplate;
    private final Random random = new Random();

    private LocalTime startPublishing;

    public JmsbenchApplication(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @GetMapping(value = "/temp/jms")
    @Async
    public void sendMessages(@RequestParam(required = false, defaultValue = "10") int count) {
        logger.info("Publishing {} messages", count);
        startPublishing = now();
        for (int i = 0; i < count; i++) {
            jmsTemplate.convertAndSend("nle.test", String.format("%05d - ", i).concat(tenUuids()));

            // uncomment to build the gruyère
            //jmsTemplate.convertAndSend("nle.storage", String.format("%05d - ", i).concat(tenUuids()));

            logger.info("Published {} messages", i);
        }
        logger.info("Published all messages in {} ms", Duration.between(startPublishing, now()).toMillis());
    }

    @JmsListener(destination = "nle.test", concurrency = "5")
    public void read(String message) throws InterruptedException {
        Thread.sleep(random.nextInt(200)); // tune this
        if (startPublishing != null) {
            logger.info("Read message [{}] after {} ms", message, Duration.between(startPublishing, now()).toMillis());
        } else {
            logger.info("Read message [{}]", message);
        }
    }

    private String tenUuids() {
        return UUID.randomUUID().toString()
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString())
                .concat(UUID.randomUUID().toString());
    }

}
