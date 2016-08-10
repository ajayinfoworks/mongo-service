package io.infoworks.datalake.controllers;

import io.infoworks.datalake.metadata.Source;
import io.infoworks.datalake.metadata.SourceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SourceController {

    @Autowired
    private SourceManager sourceManager;

    @RequestMapping("/source/add")
    public String addSource(@RequestParam(value="name", defaultValue="mysource")  String name,
                            @RequestParam(value="type", defaultValue="mongodb")  String type,
                            @RequestParam(value="hiveSchema")  String hiveSchema,
                            @RequestParam(value="hdfsDir")  String hdfsDir) {
        String id = sourceManager.addSource(name, type, hiveSchema, hdfsDir);
        if (id == null) {
            return String.format("This source name (%s) already exists. It wasn't added", name);
        } else {
            return String.format("Source with name (%s) was added successfully. It's Id is: %s", name, id);
        }
    }

    @RequestMapping("/getcollections")
    public List<String> getCollections(@RequestParam(value="name")  String sourceName) {
        return sourceManager.getCollectionsInSource(sourceName);
    }

    @RequestMapping("/sources")
    public List<String> getSources() {
        return sourceManager.getAllSourceNames();
    }

    @RequestMapping("/source/delete")
    public String deleteSource(@RequestParam(value="name") String name) {
        return sourceManager.deleteSource(name);
    }

    @RequestMapping(value = "/source/update")
    public Source updateSource(@RequestParam(value="name") String sourceName,
                               @RequestParam(value="uname") String uname,
                               @RequestParam(value="pw") String password,
                               @RequestParam(value="hp") String hostPorts,
                               @RequestParam(value="db") String dbName) {
        Source source = new Source();
        source.setSourceName(sourceName);
        source.setUserName(uname);
        source.setPassword(password);
        source.setHostPorts(hostPorts);
        source.setDbName(dbName);
        return sourceManager.updateSource(source);
    }

    @RequestMapping(value = "/source/addcollection")
    public Source addCollection(@RequestParam(value="name") String sourceName,
                               @RequestParam(value="col") String collectionName) {
        return sourceManager.addCollection(sourceName, collectionName);
    }

    @RequestMapping("/ingest")
    public String ingest(@RequestParam(value="name") String name,
                         @RequestParam(value="collection") String collection) {
        return sourceManager.ingest(name, collection);
    }

}
