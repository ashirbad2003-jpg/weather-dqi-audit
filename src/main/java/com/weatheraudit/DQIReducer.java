package com.weatheraudit;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DQIReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double compSum = 0, consSum = 0, outlSum = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length < 3) continue;
            try {
                compSum += Double.parseDouble(parts[0]);
                consSum += Double.parseDouble(parts[1]);
                outlSum += Double.parseDouble(parts[2]);
                count++;
            } catch (Exception e) { continue; }
        }

        if (count == 0) return;

        double completeness = compSum / count;
        double consistency  = consSum / count;
        double outlier      = outlSum / count;
        double dqi = (0.40 * completeness) + (0.35 * consistency) + (0.25 * outlier);

        String tag;
        if (dqi >= 98) tag = "TRUSTED";
        else if (dqi >= 92) tag = "MODERATE";
        else tag = "POOR";

        String result = String.format(
            "completeness=%.2f consistency=%.2f outlier=%.2f DQI=%.2f tag=%s",
            completeness, consistency, outlier, dqi, tag);
        context.write(key, new Text(result));
    }
}
