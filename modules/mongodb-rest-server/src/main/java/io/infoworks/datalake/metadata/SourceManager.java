package io.infoworks.datalake.metadata;

import java.util.List;

public interface SourceManager {
    <T extends Object> T addSource(String name, String type, String hiveSchema, String hdfsDir);
    List<String> getAllSourceNames();
    String deleteSource(String name);
    Source updateSource(Source source);
    Source getSource(String name);
    String ingest(String sourceName, String table);
    List<String> getCollectionsInSource(String sourceName);
    Source addCollection(String sourceName, String collectionName);
    String getIngestionStatus(String sourceName, String collectionName);
    Boolean setIngestionStatus(Source source, String collectionName, String status);
    Collection getCollectionBySourceCollectionName(Source source, String collectionName);
}
