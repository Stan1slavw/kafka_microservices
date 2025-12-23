package org.example.springboot.service;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.example.springboot.handler.WikimediaChangesHandler;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void SendMessage() throws InterruptedException {
        String topic = "wikimedia_recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);

        Headers headers = new Headers.Builder()
                .add("User-Agent", "kafka-wikimedia-producer/0.0.1 (contact: your-email@example.com)")
                .add("Accept", "text/event-stream")
                .build();

        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url))
                .headers(headers)
                .build();

        eventSource.start();
        LOGGER.info("Wikimedia Changes Producer started...");

        TimeUnit.MINUTES.sleep(10);
    }
}
