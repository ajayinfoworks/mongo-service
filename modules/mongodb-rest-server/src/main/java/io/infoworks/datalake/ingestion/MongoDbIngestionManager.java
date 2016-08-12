package io.infoworks.datalake.ingestion;

import io.infoworks.datalake.metadata.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoDbIngestionManager implements IngestionManager{

    @Autowired
    private ApplicationContext context;

    @Override
    public String ingest(Source source, String collectionName) {

        // Start the Map Reduce job in a different thread & exit.
        ExecutorService executor = Executors.newSingleThreadExecutor();

        MongoDbIngestor ingestor = (MongoDbIngestor) context.getBean("MongoDbIngestor");
        ingestor.setSource(source);
        ingestor.setCollection(collectionName);

        // Submit in a different thread
        executor.submit(ingestor);

        return String.format("Ingestion Started for Source (%s)", source.getSourceName());
    }
}
