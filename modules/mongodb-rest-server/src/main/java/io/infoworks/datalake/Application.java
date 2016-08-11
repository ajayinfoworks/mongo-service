package io.infoworks.datalake;

import io.infoworks.datalake.ingestion.IngestionManager;
import io.infoworks.datalake.ingestion.MongoDbIngestionManager;
import io.infoworks.datalake.ingestion.MongoDbIngestor;
import io.infoworks.datalake.metadata.MongoDbSourceManager;
import io.infoworks.datalake.metadata.SourceManager;
import io.infoworks.datalake.metadata.SourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;

@ComponentScan
@EnableAutoConfiguration
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SourceRepository repository;

    @Bean
    public SourceManager getSourceManager() {

        // Factory pattern will allow us to store metadata (such as source information) on any storage
        // such as, MongoDb, MySQL etc.
        String metadataType = context.getEnvironment().getProperty("metadata.store.type");
        if (metadataType.equalsIgnoreCase("MongoDb")) {
            return new MongoDbSourceManager();
        }
        return null;
    }

    @Bean(name="IngestionManager")
    @Scope("prototype")
    public IngestionManager getIngestionManager(String sourceType) {
        if (sourceType.equalsIgnoreCase("mongodb")) {
            return new MongoDbIngestionManager();
        }
        return null;
    }

    @Bean(name="MongoDbIngestor")
    @Scope("prototype")
    public MongoDbIngestor getMongoDbInjestor() {
        return new MongoDbIngestor();
    }
}