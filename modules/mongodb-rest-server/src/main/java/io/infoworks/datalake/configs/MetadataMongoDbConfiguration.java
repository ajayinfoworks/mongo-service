package io.infoworks.datalake.configs;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;

/**
 * Configurations used by Spring Data MongoDb to connect to the MongoDb database.
 */
@Configuration
public class MetadataMongoDbConfiguration extends AbstractMongoConfiguration {

    @Value("${metadata.mongodb.host}")
    private String mongoHost;

    @Value("${metadata.mongodb.port}")
    private Integer mongoPort;

    @Value("${metadata.mongodb.database.name}")
    private String mongoDb;

    @Override
    protected String getDatabaseName() {
        return mongoDb;
    }

    @Override
    public Mongo mongo() throws Exception {
        return new MongoClient(mongoHost, mongoPort);
    }
}