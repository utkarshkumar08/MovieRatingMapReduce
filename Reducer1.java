import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        double avgRating = 0;
        int numValues = 0;
        String title = "";
        String gen = "";

        for (Text value: values) {
            String parts[] = value.toString().split("#");

            if (parts[0].equals("title")) {

                title = parts[1];
                gen = parts[2];
            }
            else {

                double ratingValue = Double.parseDouble(parts[1]);
                avgRating += ratingValue;
                numValues++;
            }
        }


        avgRating /= numValues;

        String outputValue = title + "#" + avgRating + "#" + gen + "\n";
        Text output = new Text(outputValue);

        context.write(key, output);
    }
}
