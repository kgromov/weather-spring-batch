package com.kgromov.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

@Configuration
//@EnableMongoRepositories
public class MongoDbConfig extends AbstractMongoClientConfiguration {
    @Override
    protected String getDatabaseName() {
        return "test";
    }

    @Bean
    public MongoClient mongoClient(MongoProperties mongoProperties) {
        ConnectionString connectionString = new ConnectionString(mongoProperties.getUri());
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
//        return MongoClients.create(mongoProperties.getUri());
        return MongoClients.create(mongoClientSettings);
    }

   /* @Bean
    public MongoTemplate mongoTemplate(MongoClient mongoClient) throws Exception {
        return new MongoTemplate(mongoClient, this.getDatabaseName());
    }*/

    @Override
    protected Collection<String> getMappingBasePackages() {
        return List.of("com.kgromov.domain");
    }
}
