package io.infoworks.datalake.metadata;

import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

@EnableMongoRepositories
public class MongoDbSourceManager extends BaseSourceManager {

    @SuppressWarnings("unchecked")
    @Override
    public String addSource(String name, String type, String hiveSchema, String hdfsDir) {
        if (repository.findBySourceName(name) != null) {
            return null;
        }
        return repository.save(new Source(name, type, hiveSchema, hdfsDir)).getId();
    }

    @Override
    public List<String> getAllSourceNames() {
        List<String> sources = new ArrayList<>();
        for (Source source : repository.findAll()) {
            sources.add(source.getSourceName());
        }
        return sources;
    }

    @Override
    public String deleteSource(String name) {
        Source source = repository.findBySourceName(name);
        if (source == null) {
            return String.format("Source with name %s was not found.", name);
        }
        repository.delete(source);
        return String.format("Source with name %s was deleted successfully!", name);
    }

    @Override
    public Source updateSource(Source source) {
        Source existing = repository.findBySourceName(source.getSourceName());
        if (existing == null) {
            return null;
        }

        existing.setUserName(source.getUserName());
        existing.setPassword(source.getPassword());
        existing.setHostPorts(source.getHostPorts());
        existing.setDbName(source.getDbName());
        return repository.save(existing);
    }

    @Override
    public Source getSource(String name) {
        return repository.findBySourceName(name);
    }

    @Override
    public List<String> getCollectionsInSource(String sourceName) {
        throw new NotImplementedException();
    }

    @Override
    public Source addCollection(String sourceName, String collectionName) {
        Source existingSource = repository.findBySourceName(sourceName);
        if (existingSource == null) {
            return null;
        }

        // If collection with this name already exists for this source, return. Don't add.
        if (getCollectionBySourceCollectionName(existingSource, collectionName) != null) {
            return null;
        }

        Collection collection = new Collection(collectionName);
        existingSource.addCollection(collection);
        return repository.save(existingSource);
    }

    @Override
    public String getIngestionStatus(String sourceName, String collectionName) {
        Source existingSource = repository.findBySourceName(sourceName);
        if (existingSource != null) {
            Collection collection = getCollectionBySourceCollectionName(existingSource, collectionName);
            if (collection != null) {
                return collection.getIngestionStatus();
            }
        }
        return null;
    }

    @Override
    public Boolean setIngestionStatus(Source source, String collectionName, String status) {
        try {
            Collection collection = getCollectionBySourceCollectionName(source, collectionName);
            if (collection != null) {
                collection.setIngestionStatus(status);
                repository.save(source);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
