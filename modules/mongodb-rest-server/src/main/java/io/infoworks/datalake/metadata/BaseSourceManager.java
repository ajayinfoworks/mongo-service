package io.infoworks.datalake.metadata;

import io.infoworks.datalake.ingestion.IngestionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Set;

public abstract class BaseSourceManager implements SourceManager {

    @Autowired
    protected ApplicationContext context;

    @Autowired
    protected SourceRepository repository;

    @Override
    public String ingest(String sourceName, String collectionName) {
        Source source = getSource(sourceName);
        if (source == null) {
            return String.format("Ingestion FAILED!!! Source with name (%s) was not found!", sourceName);
        }

        Collection collection = getCollectionBySourceCollectionName(source, collectionName);
        if (collection == null) {
            return String.format("Ingestion FAILED!!! Collection with name (%s) was not found in this Source(%s)!",
                    collectionName, sourceName);
        }

        String status = getIngestionStatus(sourceName, collectionName);
        if (status != null && status.equalsIgnoreCase("In Progress")) {
            return String.format("Ingestion is in progress for Source(%s), Collection(%s). Please wait until " +
                            "it completes!!!", sourceName, collectionName);
        }

        IngestionManager manager = (IngestionManager) context.getBean("IngestionManager", source.getSourceType());
        return manager.ingest(source, collectionName);
    }

    @Override
    public Collection getCollectionBySourceCollectionName(Source source, String collectionName) {
        if (source != null) {
            for (Collection collection : source.getCollections()) {
                if (collection.getCollectionName().equalsIgnoreCase(collectionName)) {
                    return collection;
                }
            }
        }
        return null;
    }

    @Override
    public Set<String> getCollectionsInDb(String sourceName) {
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

    @Override
    public String getIngestionStatus(String sourceName, String collectionName) {
        throw new NotImplementedException();
    }

    @Override
    public Boolean setIngestionStatus(Source source, String collectionName, String status) {
        throw new NotImplementedException();
    }
}
