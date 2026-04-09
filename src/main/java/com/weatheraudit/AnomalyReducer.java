package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class AnomalyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<double[]> records = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length < 5) continue;
            try {
                double[] row = new double[5];
                for (int i = 0; i < 5; i++) {
                    row[i] = Double.parseDouble(parts[i]);
                }
                records.add(row);
            } catch (Exception e) { continue; }
        }

        if (records.isEmpty()) return;

        int n = records.size();
        double[] sum = new double[5];
        for (double[] r : records) {
            for (int i = 0; i < 5; i++) sum[i] += r[i];
        }
        double[] mean = new double[5];
        for (int i = 0; i < 5; i++) mean[i] = sum[i] / n;

        double[] variance = new double[5];
        for (double[] r : records) {
            for (int i = 0; i < 5; i++) {
                variance[i] += Math.pow(r[i] - mean[i], 2);
            }
        }
        double[] stddev = new double[5];
        for (int i = 0; i < 5; i++) {
            stddev[i] = Math.sqrt(variance[i] / n);
        }

        // Count anomalies using Z-score > 3
        int anomalyCount = 0;
        for (double[] r : records) {
            for (int i = 0; i < 5; i++) {
                if (stddev[i] > 0) {
                    double z = Math.abs((r[i] - mean[i]) / stddev[i]);
                    if (z > 3) { anomalyCount++; break; }
                }
            }
        }

        double anomalyRate = ((double) anomalyCount / n) * 100.0;

        String status;
        if (anomalyRate > 10) status = "HIGH_ANOMALY";
        else if (anomalyRate > 3) status = "MEDIUM_ANOMALY";
        else status = "NORMAL";

        String result = String.format(
            "records=%d anomalies=%d rate=%.2f%% status=%s " +
            "avg_temp=%.2f avg_humidity=%.2f avg_precip=%.2f avg_pm25=%.2f avg_wind=%.2f",
            n, anomalyCount, anomalyRate, status,
            mean[0], mean[1], mean[2], mean[3], mean[4]
        );

        context.write(key, new Text(result));
    }
}
