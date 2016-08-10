package io.infoworks.datalake.ingestion;

import io.infoworks.datalake.metadata.Source;

public interface IngestionManager {
    public String ingest(Source source, String table);
}
