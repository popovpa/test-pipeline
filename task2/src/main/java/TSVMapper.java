import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class TSVMapper extends Mapper<LongWritable, Text, NullWritable, Writable> {

    private String delimiter = "\t";

    private TypeDescription schema = TypeDescription.fromString("struct<id:string,first_name:string,last_name:string,account_number:string,email:string>");
    private OrcStruct record = (OrcStruct) OrcStruct.createValue(schema);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String[] tokens = StringUtils.split(value.toString(), delimiter);
        for (int i = 0; i < tokens.length; i++) {
            record.setFieldValue(i, new Text(tokens[i]));
        }
        context.write(NullWritable.get(), record);
    }
}