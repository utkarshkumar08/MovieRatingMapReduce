import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
    	
    	String title = "";
    	String rating = "";
        String[] gen;

        for (Text value: values) {

            String parts[] = value.toString().split("#");
         
                title = parts[0];
                rating = parts[1];
                gen = parts[2].split("\\|");
                for(int i=0;i<gen.length;i++) {
                String outputValue = title + "#" + rating+  "#" + gen[i] + "\n";
                Text output = new Text(outputValue);
                context.write(key, output);
                }
        }

    }
}
