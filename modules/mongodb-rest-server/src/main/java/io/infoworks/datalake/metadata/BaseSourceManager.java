package io.infoworks.datalake.metadata;

import io.infoworks.datalake.ingestion.IngestionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

public abstract class BaseSourceManager implements SourceManager {

    @Autowired
    private ApplicationContext context;

    @Override
    public String ingest(String sourceName, String table) {
        Source source = getSource(sourceName);
        if (source == null) {
            return String.format("Ingestion FAILED!!! Souce with name (%s) was not found!", sourceName);
        }

        if (source.getIngestionStatus() != null &&
                source.getIngestionStatus().equalsIgnoreCase("In Progress")) {
            return String.format("Ingestion FAILED!!! Souce with name (%s) was not found!", sourceName);
        }

        IngestionManager manager = (IngestionManager) context.getBean("IngestionManager", source.getSourceType());
        return manager.ingest(source, table);
    }

    @Override
    public List<String> getCollectionsInSource(String sourceName) {
        throw new NotImplementedException();
    }

    @Override
    public Source addCollection(String sourceName, String collectionName) {
        throw new NotImplementedException();
    }

    @Override
    public <T> T addSource(String name, String type, String hiveSchema, String hdfsDir) {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getAllSourceNames() {
        throw new NotImplementedException();
    }

    @Override
    public String deleteSource(String name) {
        throw new NotImplementedException();
    }

    @Override
    public Source updateSource(Source source) {
        throw new NotImplementedException();
    }

    @Override
    public Source getSource(String name) {
        throw new NotImplementedException();
    }
}
