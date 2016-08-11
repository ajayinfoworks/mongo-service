package io.infoworks.datalake.metadata;

import java.util.List;

public interface SourceManager {
    public <T extends Object> T addSource(String name, String type, String hiveSchema, String hdfsDir);
    public List<String> getAllSourceNames();
    public String deleteSource(String name);
    public Source updateSource(Source source);
    public Source getSource(String name);
    public String ingest(String sourceName, String table);
    List<String> getCollectionsInSource(String sourceName);
    Source addCollection(String sourceName, String collectionName);
}
