import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
    
        boolean doWrite = true;
        try {
            String startTime = line[1].replaceAll("\"", "").toLowerCase();
            
            String date = startTime.split("\\s+")[0];
            String time = startTime.split("\\s+")[1];

            Integer startHour = Integer.parseInt(time.split(":")[0]);
            Integer startHourBin = (Integer.parseInt(time.split(":")[0])/4) * 4;

            if (startHour < 0 || startHour > 23)
                doWrite = false;

            int startYear, startMonth, startDay;
            if (startTime.contains("-")) {
                startYear = Integer.parseInt(date.split("-")[0]);
                startMonth = Integer.parseInt(date.split("-")[1]);
                startDay = Integer.parseInt(date.split("-")[2]);
            }
            else {
                startYear = Integer.parseInt(date.split("/")[2]);
                startMonth = Integer.parseInt(date.split("/")[0]);
                startDay = Integer.parseInt(date.split("/")[1]);
            }

            if (startYear < 2013 || startYear > 2020 || startMonth < 1 || startMonth > 12 || startDay < 1)
                doWrite = false;

            if ((startYear % 4 == 0 && startMonth == 2 && startDay > 29) || (startMonth == 2 && startDay > 28))
                doWrite = false;
            else if (startMonth <= 7 && ((startMonth % 2 != 0 && startDay > 31) || (startMonth % 2 == 0 && startDay > 30)))
                doWrite = false;
            else if ((startMonth % 2 != 0 && startDay > 30) || (startMonth % 2 == 0 && startDay > 31))
                doWrite = false;

            int stationId = Integer.parseInt(line[3].replaceAll("\"", ""));
            if (stationId <= 0)
                doWrite = false;

            double startLatitude = Double.parseDouble(line[5].replaceAll("\"", ""));
            double startLongitude = Double.parseDouble(line[6].replaceAll("\"", ""));
            if (startLatitude < 40.69715 || startLatitude > 40.862752 || startLongitude < -74.022208 || startLongitude > -73.924361)
                doWrite = false;

            int startGridRow = (int)((startLatitude - 40.69715)/0.003);
            int startGridCol = (int)((startLongitude - (-74.022208))/0.003);
            String startGridId = startGridRow + "_" + startGridCol;

            String userType = line[12].replaceAll("\"", "").toLowerCase();
            if (userType.equals("subscriber"))
                userType = "0";
            else if (userType.equals("customer"))
                userType = "1";
            else
                doWrite = false;

            int birthYear = Integer.parseInt(line[13].replaceAll("\"", ""));
            if (birthYear < 1943 || birthYear > 1998)
                doWrite = false;

            int gender = Integer.parseInt(line[14].replaceAll("\"", ""));
            if (gender != 0 && gender != 1 && gender != 2)
                doWrite = false;

            if (doWrite)
                context.write(new Text(startYear + ""), new Text("placeholder" + "," + startYear + "," + startMonth + "," + startDay + "," + startHour + "," + stationId + "," + startLatitude + "," + startLongitude + "," + startGridId + "," + startHourBin));
        }
        catch(Exception e) {}
    }
}

