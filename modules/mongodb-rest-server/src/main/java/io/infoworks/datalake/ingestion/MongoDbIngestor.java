package io.infoworks.datalake.ingestion;

import io.infoworks.datalake.metadata.Source;
import io.infoworks.datalake.metadata.SourceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Random;
import java.util.concurrent.Callable;

public class MongoDbIngestor implements Callable<String> {

    private Random random = new Random();

    private Source source;
    private String collection;

    @Value("${app.temp.dir}")
    private String appTempDir;

    @Value("${mongodb.import.shell}")
    private String shellScriptLocation;

    @Autowired
    private SourceManager sourceManager;

    public void setSource(Source source) {
        this.source = source;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    @Override
    public String call() throws Exception {
        try {
            source.setIngestionStatus("In Progress");
            sourceManager.updateSource(source);

            String tmpDirName = String.format("%s/%s_%d", appTempDir, source.getSourceName(), random.nextInt(1000));
            String mongoURI = String.format("mongodb://%s:%s@%s/%s.%s", source.getUserName(), source.getPassword(),
                    source.getHostPorts(), source.getDbName(), collection);
            String targetDir = source.getTargetHdfsLocation() + "/" + collection;

            System.out.println("tmpDirName = " + tmpDirName);
            System.out.println("mongoURI = " + mongoURI);
            System.out.println("shellScriptLocation = " + shellScriptLocation);

            String[] cmd = { shellScriptLocation, mongoURI, tmpDirName, targetDir };
            System.out.println("executing command... " + cmd.toString());
            Process process = Runtime.getRuntime().exec(cmd);
            System.out.println("executed command... " + cmd.toString());
            process.waitFor();
            System.out.println("done waiting... " + cmd.toString());
            if (process.exitValue() == 0) {
                source.setIngestionStatus("SUCCESS");
            } else {
                System.out.println("ERROR: " + convertStreamToString(process.getErrorStream()));
                source.setIngestionStatus("ERROR");
            }
        } catch (Exception e) {
            e.printStackTrace();
            source.setIngestionStatus("ERROR");
        } finally {
            sourceManager.updateSource(source);
        }
        return source.getIngestionStatus();
    }

    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

}
