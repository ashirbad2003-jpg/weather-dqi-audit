package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AnomalyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String city = fields[1].trim();
        if (city.isEmpty()) return;

        try {
            double temp     = Double.parseDouble(fields[7].trim());
            double humidity = Double.parseDouble(fields[18].trim());
            double precip   = Double.parseDouble(fields[16].trim());
            double pm25     = Double.parseDouble(fields[31].trim());
            double wind     = Double.parseDouble(fields[10].trim());

            // Emit all 5 values per city for stddev calculation
            String vals = temp + ":" + humidity + ":" + precip + ":" + pm25 + ":" + wind;
            context.write(new Text(city), new Text(vals));

        } catch (Exception e) {
            // skip unparseable rows
        }
    }
}
