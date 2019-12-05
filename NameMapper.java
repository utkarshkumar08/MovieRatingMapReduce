import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class NameMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String line = value.toString();
        String[] moviePieces = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        if (moviePieces.length >= 2) {
            int movieId = Integer.parseInt(moviePieces[0]);
            String title = moviePieces[1];
            String gen = moviePieces[2];

           
            LongWritable mapKey = new LongWritable(movieId);
            Text mapValue = new Text("title#" + title + "#" + gen);

            context.write(mapKey, mapValue);
        }
    }
}