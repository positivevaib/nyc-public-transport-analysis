import java.io.*;
import java.util.*;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line_array = value.toString().split(",");
        
        if (line_array.length < 8) {
            return;
        }

        String controlArea = line_array[0];
        String remoteUnit = line_array[1];
        String subunitChannelPosition = line_array[2];

        int number_of_datapoints = (line_array.length - 3) / 5;

        for (int i = 0; i < number_of_datapoints; i++) {
            String date = line_array[i * 5 + 3];
            String time = line_array[i * 5 + 4];
            String description = line_array[i * 5 + 5];
            String entries = line_array[i * 5 + 6];
            String exits = line_array[i * 5 + 7];
            context.write(new Text(remoteUnit + ""), new Text(controlArea + "," + remoteUnit + "," + subunitChannelPosition + "," 
            + date + "," + time + "," + description + "," + entries + "," + exits)); 
        }
    }
}
