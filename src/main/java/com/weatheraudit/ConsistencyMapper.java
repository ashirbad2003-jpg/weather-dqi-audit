package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ConsistencyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String city = fields[1].trim();
        if (city.isEmpty()) return;

        int violations = 0;
        int checks = 5;

        try {
            double temp = Double.parseDouble(fields[7].trim());
            if (temp < -90 || temp > 60) violations++;
        } catch (Exception e) { violations++; }

        try {
            double humidity = Double.parseDouble(fields[18].trim());
            if (humidity < 0 || humidity > 100) violations++;
        } catch (Exception e) { violations++; }

        try {
            double pressure = Double.parseDouble(fields[14].trim());
            if (pressure < 870 || pressure > 1084) violations++;
        } catch (Exception e) { violations++; }

        try {
            double precip = Double.parseDouble(fields[16].trim());
            if (precip < 0) violations++;
        } catch (Exception e) { violations++; }

        try {
            double wind = Double.parseDouble(fields[10].trim());
            if (wind < 0) violations++;
        } catch (Exception e) { violations++; }

        double score = ((double)(checks - violations) / checks) * 100.0;
        context.write(new Text(city), new Text("CONSISTENCY:" + String.format("%.2f", score)));
    }
}
