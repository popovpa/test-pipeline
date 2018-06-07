import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRStarter {

    static Logger logger = LoggerFactory.getLogger(MRStarter.class);

    public static void main(String[] args) {
        JobConf config = new JobConf();
        config.setNumReduceTasks(0);

        config.setOutputKeyClass(NullWritable.class);
        config.setOutputValueClass(NullWritable.class);

        try {
            FileInputFormat.setInputPaths(config, "data/");

            Job job = Job.getInstance(config);

            OrcOutputFormat.setOutputPath(job, new Path("data/orc.orc"));
            job.setMapperClass(TSVMapper.class);
            job.setInputFormatClass(TextInputFormat.class);

            if (!job.waitForCompletion(true)) {
                logger.error(job.getStatus().getFailureInfo());
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            ex.printStackTrace();
        }
    }
}