package org.example.springboot.service;

import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WikimediaChangesProducer {

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void SendMessage(){
        String topic = "wikimedia_recentchange";


    }
}
