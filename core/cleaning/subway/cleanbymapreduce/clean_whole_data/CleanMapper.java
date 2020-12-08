import java.io.*;
import java.util.*;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {   
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");

        try {
            if (line.length < 12) {
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

            String station = line[3].trim();
            if ("".equals(station)) {
                return;  
            }

            int year = Integer.parseInt(line[4].trim()); 
            if (year < 2012 && year > 2019) {
                return; 
            }

            int month = Integer.parseInt(line[5].trim());
            int day = Integer.parseInt(line[6].trim());
            
            int hour = Integer.parseInt(line[7].trim());
            if (hour < 7 && hour > 1) {
                return; 
            }

            int netEntries = Integer.parseInt(line[8].trim());
            if (netEntries > 14400 || netEntries < 0){
                return; 
            }   

            int netExits = Integer.parseInt(line[9].trim());
            if (netExits > 14400 || netExits < 0){
                return; 
            }   

            double latitude = Double.parseDouble(line[10].trim());
            double longitude = Double.parseDouble(line[11].trim());

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
            for (int i = 0; i < coordinates.length; i++) {
                if (latitude > coordinates[i][1] && latitude < coordinates[i][3] && longitude > coordinates[i][0] && longitude < coordinates[i][2]) {
                    gridId = (int)coordinates[i][4];
                    break;
                }
            }
                            
            context.write(new Text(remoteUnit + ""), new Text(controlArea + "," + remoteUnit + "," + subunitChannelPosition + "," 
            + station + "," + year + "," + month + "," + day + "," + hour + "," + netEntries + "," + netExits + "," + latitude + "," + longitude + "," + gridId)); 
        }
        catch (NumberFormatException e){}
    }
}