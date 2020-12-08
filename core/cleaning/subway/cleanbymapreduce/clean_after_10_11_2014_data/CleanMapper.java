import java.io.*;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    final static String DATE_FORMAT = "MM/dd/yyyy";

    public static boolean isDateValid(String date) 
    {
        try {
            DateFormat df = new SimpleDateFormat(DATE_FORMAT);
            df.setLenient(false);
            df.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");

        try {
            if (line.length < 11) {
                return;
            }

            String controlArea = line[0].trim();
            if ("".equals(controlArea)) {
                return;  
            }   

            String remoteUnit = line[1].trim();
            if ("".equals(remoteUnit)) {
                return;  
            }   

            String subunitChannelPosition = line[2].trim();
            if ("".equals(subunitChannelPosition)) {
                return; 
            }   

            if (!isDateValid(line[6])) {
                return;
            }

            int year = Integer.parseInt(line[6].split("/")[2]); 
            int month = Integer.parseInt(line[6].split("/")[0]);
            int day = Integer.parseInt(line[6].split("/")[1]);
            
            int hour = Integer.parseInt(line[7].split(":")[0]);
            if (hour < 0 || hour > 23) {
                return; 
            }

            int entries = Integer.parseInt(line[9].trim());
            if (entries < 0){
                return; 
            }   

            int exits = Integer.parseInt(line[10].trim());
            if (exits < 0){
                return; 
            }   
            
            context.write(new Text(remoteUnit + ""), new Text(controlArea + "," + remoteUnit + "," + subunitChannelPosition + "," 
                + year + "," + month + "," + day + "," + hour + "," + entries + "," + exits)); 
        }
        catch(NumberFormatException e) {}
    }
}