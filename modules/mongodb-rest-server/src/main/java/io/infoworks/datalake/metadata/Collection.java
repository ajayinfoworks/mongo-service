package io.infoworks.datalake.metadata;

public class Collection implements java.io.Serializable {
    private String collectionName;
    private String ingestionStatus;

    public Collection(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getIngestionStatus() {
        return ingestionStatus;
    }

    public void setIngestionStatus(String ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }
}
