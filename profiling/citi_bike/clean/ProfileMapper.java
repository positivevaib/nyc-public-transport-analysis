import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");

        int tripDuration = Integer.parseInt(line[0]);
        int tripDurationBin = (tripDuration % 600) * 600;
        
        String startYear = line[1];
        String startMonth = line[2];
        String startDay = line[3];
        String startHour = line[4];
        String userType = line[5];
        String birthYear = line[6];
        String gender = line[7];

        context.write(new Text("cleanCount"), new IntWritable(1));
        context.write(new Text("tripDuration: " + tripDuration), new IntWritable(1));
        context.write(new Text("tripDurationBin: " + tripDurationBin), new IntWritable(1));
        context.write(new Text("startYear: " + startYear), new IntWritable(1));
        context.write(new Text("startMonth: " + startMonth), new IntWritable(1));
        context.write(new Text("startDay: " + startDay), new IntWritable(1));
        context.write(new Text("startHour: " + startHour), new IntWritable(1));
        context.write(new Text("userType: " + userType), new IntWritable(1));
        context.write(new Text("birthYear: " + birthYear), new IntWritable(1));
        context.write(new Text("gender: " + gender), new IntWritable(1));
    }
}

