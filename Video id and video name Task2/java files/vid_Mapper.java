package views;


import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class vid_Mapper 
	extends Mapper<Object ,Text , Text, Text> {
	
	
	
	@Override
	  public void map(Object offset, Text record_txt, Context context)
	      throws IOException, InterruptedException {
		  try {
			  	String record = record_txt.toString();
				String tuple[] = record.split(",");
				if (tuple.length == 7) {
					
					if (tuple[2].length() != 20) {
						return;
					}
					String publishedAt_st = tuple[2].substring(0, 10);
					LocalDate publishedAt = LocalDate.parse(publishedAt_st);
					LocalDate lower_limit = LocalDate.of(2020, 9, 1);
					LocalDate upper_limit = LocalDate.of(2021, 9, 1);
					if ((publishedAt.isAfter(lower_limit) || publishedAt.isEqual(lower_limit))
							&& (publishedAt.isBefore(upper_limit) || publishedAt.isEqual(upper_limit)))
					{
						String videoid= tuple[0];
						String videoname= tuple[1];
					
						context.write(new Text(videoid),new Text(videoname));
						
                           
					}
				}
		  }
		  catch(DateTimeParseException dtpe) {
			  return;
		  }
	  }
}