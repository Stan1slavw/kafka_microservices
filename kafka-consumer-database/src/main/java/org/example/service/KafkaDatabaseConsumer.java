package org.example.service;

import org.example.entity.WikimediaData;
import org.example.repository.WikimediaDataRepo;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private final WikimediaDataRepo wikimediaDataRepo;
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    public KafkaDatabaseConsumer(WikimediaDataRepo wikimediaDataRepo) {
        this.wikimediaDataRepo = wikimediaDataRepo;
    }

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMassage){
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMassage);
        wikimediaDataRepo.save(wikimediaData);
        LOGGER.info("Data saved to database");
        LOGGER.info(String.format("Event message received -> %s", eventMassage));

    }
}
