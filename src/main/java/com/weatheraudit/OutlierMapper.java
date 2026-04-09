package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String city = fields[1].trim();
        if (city.isEmpty()) return;

        int outliers = 0;
        int checks = 4;

        try {
            double pm25 = Double.parseDouble(fields[31].trim());
            if (pm25 > 150) outliers++;
        } catch (Exception e) { outliers++; }

        try {
            double co = Double.parseDouble(fields[27].trim());
            if (co > 10000) outliers++;
        } catch (Exception e) { outliers++; }

        try {
            double temp = Double.parseDouble(fields[7].trim());
            if (temp > 55 || temp < -60) outliers++;
        } catch (Exception e) { outliers++; }

        try {
            double precip = Double.parseDouble(fields[16].trim());
            if (precip > 500) outliers++;
        } catch (Exception e) { outliers++; }

        double score = ((double)(checks - outliers) / checks) * 100.0;
        context.write(new Text(city), new Text("OUTLIER:" + String.format("%.2f", score)));
    }
}
