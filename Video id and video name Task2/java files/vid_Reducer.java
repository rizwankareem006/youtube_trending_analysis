package views;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class vid_Reducer 
	extends Reducer<Text, Text, Text, Text>{
	    	
	Text textValue = new Text();
			@Override
	public void reduce(Text videoid, Iterable<Text> values, Context context)
	         throws IOException,  InterruptedException {
           
				 for (Text value : values) {

				        
				   
				        textValue.set(value.toString());

				        
				    }
				 context.write(videoid, textValue);
			}
}