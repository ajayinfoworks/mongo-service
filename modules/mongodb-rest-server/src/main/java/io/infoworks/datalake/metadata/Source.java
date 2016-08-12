package io.infoworks.datalake.metadata;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Document
public class Source implements java.io.Serializable {

    @Id
    private String id;

    private String sourceName;
    private String sourceType;

    private String userName;
    private String password;
    private String hostPorts = "127.0.0.1:27017";
    private String dbName;
    private String dbOptions;
    private List<Collection> collections = new ArrayList<>();

    private String targetHdfsLocation;
    private String targetHiveSchema;

    private String schemaType = "Schema-less";
    private String columns;
    private String ingestionType = "Full load";

    public Source() {
    }

    public Source(String sourceName, String sourceType, String targetHiveSchema, String targetHdfsLocation) {
        this.sourceName = sourceName;
        this.sourceType = sourceType;
        this.targetHiveSchema = targetHiveSchema;
        this.targetHdfsLocation = targetHdfsLocation;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHostPorts() {
        return hostPorts;
    }

    public void setHostPorts(String hostPorts) {
        this.hostPorts = hostPorts;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbOptions() {
        return dbOptions;
    }

    public void setDbOptions(String dbOptions) {
        this.dbOptions = dbOptions;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getIngestionType() {
        return ingestionType;
    }

    public void setIngestionType(String ingestionType) {
        this.ingestionType = ingestionType;
    }

    public String getTargetHdfsLocation() {
        return targetHdfsLocation;
    }

    public void setTargetHdfsLocation(String targetHdfsLocation) {
        this.targetHdfsLocation = targetHdfsLocation;
    }

    public String getTargetHiveSchema() {
        return targetHiveSchema;
    }

    public void setTargetHiveSchema(String targetHiveSchema) {
        this.targetHiveSchema = targetHiveSchema;
    }

    public List<Collection> getCollections() {
        return collections;
    }

    public void setCollections(List<Collection> collections) {
        this.collections = collections;
    }

    public void addCollection(Collection collection) {
        this.collections.add(collection);
    }
}
