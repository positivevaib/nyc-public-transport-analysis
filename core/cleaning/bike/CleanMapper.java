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

            Integer startHour = (Integer.parseInt(time.split(":")[0])/4) * 4;

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

            double latitude = Double.parseDouble(line[5].replaceAll("\"", ""));
            double longitude = Double.parseDouble(line[6].replaceAll("\"", ""));
            if (latitude < 40.69715 || latitude > 40.862752 || longitude < -74.022208 || longitude > -73.924361)
                doWrite = false;

            double[][] coordinates = {{-74.0090959591, 40.7031731656, -74.0024602764, 40.7113354232, 87},
                                      {-74.002849, 40.724272, -73.991463, 40.732949, 114},
                                      {-73.9978828447, 40.7343701499, -73.984042023, 40.7460750268, 234},
                                      {-73.96741, 40.772905, -73.949289, 40.787938, 236},
                                      {-73.98883, 40.777528, -73.969199, 40.789407, 239},
                                      {-73.955741, 40.782909, -73.9356, 40.798096, 75},
                                      {-73.992688, 40.72149, -73.977956, 40.734541, 79},
                                      {-73.993468, 40.749782, -73.984087, 40.757252, 100},
                                      {-73.984297, 40.760304, -73.971339, 40.767774, 163},
                                      {-73.970537, 40.801094, -73.950625, 40.817901, 166}};

            int gridId = 0;
            for (int i = 0; i < coordinates.length; i++)
                if (latitude > coordinates[i][1] && latitude < coordinates[i][3] && longitude > coordinates[i][0] && longitude < coordinates[i][2]) {
                    gridId = (int)coordinates[i][4];
                    break;
                }

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
                context.write(new Text(startYear + ""), new Text("placeholder" + "," + startYear + "," + startMonth + "," + startDay + "," + startHour + "," + stationId + "," + latitude + "," + longitude + "," + gridId + "," + userType + "," + birthYear + "," + gender));
        }
        catch(Exception e) {}
    }
}

