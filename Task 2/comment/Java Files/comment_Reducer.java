package comment;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class comment_Reducer 
	extends Reducer<Text, IntWritable, Text, LongWritable>{
	    	

			@Override
	public void reduce(Text video_id, Iterable<IntWritable> counts, Context context)
	         throws IOException,  InterruptedException {
            
            long comment=0;
          
           
            
	      for (IntWritable count  : counts) {
	    	  comment+=count.get();
              
	      }
	      context.write(video_id,  new LongWritable(comment));
	}
}
