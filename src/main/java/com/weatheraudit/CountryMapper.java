package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String country = fields[0].trim();
        String city    = fields[1].trim();
        if (country.isEmpty() || city.isEmpty()) return;

        try {
            double temp     = Double.parseDouble(fields[7].trim());
            double humidity = Double.parseDouble(fields[18].trim());
            double precip   = Double.parseDouble(fields[16].trim());
            double pm25     = Double.parseDouble(fields[31].trim());

            String val = city + ":" + temp + ":" + humidity + ":" + precip + ":" + pm25;
            context.write(new Text(country), new Text(val));

        } catch (Exception e) {
            // skip
        }
    }
}
