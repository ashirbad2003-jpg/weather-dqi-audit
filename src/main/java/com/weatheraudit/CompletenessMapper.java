package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CompletenessMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String city = fields[1].trim();
        if (city.isEmpty()) return;

        // Check these specific indexes
        int[] auditCols = {7, 18, 14, 16, 10, 27, 31};
        String[] names = {"temp","humidity","pressure","precip","wind","CO","PM25"};

        int total = auditCols.length;
        int missing = 0;

        for (int idx : auditCols) {
            if (idx >= fields.length || fields[idx].trim().isEmpty()
                    || fields[idx].trim().equals("null")) {
                missing++;
            }
        }

        // Extra validation: try parsing temperature
        try {
            Double.parseDouble(fields[7].trim());
        } catch (Exception e) {
            missing++;
            total++;
        }

        double completeness = ((double)(total - missing) / total) * 100.0;
        context.write(new Text(city), new Text("COMPLETENESS:" + String.format("%.2f", completeness)));
    }
}
