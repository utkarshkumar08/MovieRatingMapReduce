import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String line = value.toString();
        String[] ratingPieces = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        if (ratingPieces.length >= 2) {

            int movieId = Integer.parseInt(ratingPieces[1]);
            String rating = ratingPieces[2];


            LongWritable mapKey = new LongWritable(movieId);
            Text mapValue = new Text("rating#" + rating);


            context.write(mapKey, mapValue);
        }
    }
}