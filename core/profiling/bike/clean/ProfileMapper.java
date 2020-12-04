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

        String startYear = line[0];
        String startMonth = line[1];
        String startDay = line[2];
        String startHour = line[3];
        String stationId = line[4];
        String gridId = line[7];
        String userType = line[8];
        String birthYear = line[9];
        String gender = line[10];

        context.write(new Text("cleanCount"), new IntWritable(1));
        context.write(new Text("startYear: " + startYear), new IntWritable(1));
        context.write(new Text("startMonth: " + startMonth), new IntWritable(1));
        context.write(new Text("startDay: " + startDay), new IntWritable(1));
        context.write(new Text("startHour: " + startHour), new IntWritable(1));
        context.write(new Text("stationId: " + stationId), new IntWritable(1));
        context.write(new Text("gridId: " + stationId), new IntWritable(1));
        context.write(new Text("userType: " + userType), new IntWritable(1));
        context.write(new Text("birthYear: " + birthYear), new IntWritable(1));
        context.write(new Text("gender: " + gender), new IntWritable(1));
    }
}

