import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	 String line = value.toString();
    	 String[] ratingPieces = line.split("#");
    	 
    	 if (ratingPieces.length >= 2) {
    		 String title = ratingPieces[1];
    		 String rating = ratingPieces[2];
    		 String genre = ratingPieces[3];
    		Text mapKey = new Text(genre);
    		 Text mapValue = new Text(title + "#"+ rating);
    		 context.write(mapKey, mapValue);
          
    		
         }
    }
}