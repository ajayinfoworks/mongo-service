package io.infoworks.datalake.ingestion;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class MongoDbMRJob extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(MongoDbMRJob.class);

    public int run(String[] args) throws Exception {
        String mongoURI = args[0];
        String tempOutputPath = args[1];
        String targetPath = args[2];

        try {
            final Configuration conf = super.getConf();

            log.info("mongoURI = " + mongoURI);

            MongoConfigUtil.setInputURI(conf, mongoURI);
            MongoConfigUtil.setCreateInputSplits(conf, false);

            Job job = new Job(conf);
            Path out = new Path(tempOutputPath);

            FileOutputFormat.setOutputPath(job, out);
            job.setJarByClass(MongoDbMRJob.class);
            job.setMapperClass(ReadTablesFromMongo.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(MongoInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setNumReduceTasks(0);
            int rc = job.waitForCompletion(true) ? 0 : 1;
            if (rc == 0) {
                Path path = new Path(targetPath);
                FileSystem fs = FileSystem.get(conf);
                if (fs.exists(path)) {
                    fs.delete(path, true);
                }
                Path tempPath = new Path(tempOutputPath);
                fs.rename(tempPath, path);
            }
            return rc;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    public static class ReadTablesFromMongo extends Mapper<Object, BSONObject, Text, Text> {
        @Override
        public void map(Object key, BSONObject value, Context context) throws IOException,
                InterruptedException {
            String bsonString = value.toString();
            JSONObject jsonObj = null;
            String keyVal = value.get("_id").toString();
            try {
                jsonObj = new JSONObject(bsonString);
                System.out.println(jsonObj.toString());
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            context.write(new Text(keyVal), new Text(jsonObj.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(String.format("Starting MongoDb Map Reduce Ingestion job with arguments: %s, %s, %s",
                args[0], args[1], args[2]));
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new MongoDbMRJob(), args);
        System.exit(res);
    }
}
