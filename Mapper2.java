import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	 String line = value.toString();
    	 String[] ratingPieces = line.split("#");
    	 
    	 if (ratingPieces.length >= 2) {
    		 double rating = Double.parseDouble(ratingPieces[2]);
    		 if(ratingPieces[2].equalsIgnoreCase("NaN")) {
    			 rating = 0.0;
    		 }

    		 String title = ratingPieces[1];
    		 String gen = ratingPieces[3];
    		 Text mapValue = new Text(title + "#"+ rating+"#" + gen);
    		 context.write(key, mapValue);

         }
    }
}