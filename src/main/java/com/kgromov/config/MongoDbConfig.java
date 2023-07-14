package com.kgromov.config;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;

@Configuration
public class MongoDbConfig extends AbstractReactiveMongoConfiguration {
    @Override
    protected String getDatabaseName() {
        return "test";
    }

    @Bean
    public MongoClient mongoClient(MongoProperties mongoProperties) {
        return MongoClients.create(mongoProperties.getUri());
    }
}
