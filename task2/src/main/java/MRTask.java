import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ETLUtils;
import utils.PipelineTask;
import utils.exceptions.ETLException;

import java.io.File;
import java.io.IOException;

public class MRTask implements PipelineTask {

    private Logger logger = LoggerFactory.getLogger(MRTask.class);

    private String in;
    private String out;

    public MRTask(String in, String out) {
        this.in = in;
        this.out = out;
    }

    @Override
    public String taskName() {
        return "ORC writer";
    }

    @Override
    public boolean canRun() {
        return ETLUtils.isSuccess(in);
    }

    @Override
    public boolean canSkip() {
        return ETLUtils.isSuccess(out);
    }

    @Override
    public void run() throws ETLException {
        JobConf config = new JobConf();
        config.setNumReduceTasks(0);

        config.setMapOutputKeyClass(NullWritable.class);
        config.setMapOutputValueClass(Writable.class);

        try {
            FileInputFormat.setInputPaths(config, in);

            Job job = Job.getInstance(config);

            OrcOutputFormat.setOutputPath(job, new Path(out));
            job.setMapperClass(TSVMapper.class);
            job.setInputFormatClass(TextInputFormat.class);

            cleanupOutput();
            if (!job.waitForCompletion(true)) {
                logger.error(job.getStatus().getFailureInfo());
            }
        } catch (Exception ex) {
            throw new ETLException(ex);
        }
    }

    @Override
    public void complete() {
        //nop
        //complete during only-map job
    }

    private void cleanupOutput() {
        File outDir = new File(out);
        if (outDir.exists() && outDir.isDirectory()) {
            try {
                FileUtils.deleteDirectory(outDir);
                logger.info("Output successful prepared");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}