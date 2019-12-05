import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        double maxRating = 0.0;
        String title = "";

        for (Text value: values) {
            String parts[] = value.toString().split("#");

                double ratingValue = Double.parseDouble(parts[1]);
                if(ratingValue > maxRating) {
                	maxRating = ratingValue;
                	title = parts[0];
                }
        }


        String outputValue = title + "#" + maxRating + "\n";
        Text output = new Text(outputValue);
        context.write(key, output);
    }
}
