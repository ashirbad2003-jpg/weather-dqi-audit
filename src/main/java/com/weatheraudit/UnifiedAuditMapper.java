package com.weatheraudit;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class UnifiedAuditMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("country") || line.isEmpty()) return;

        String[] fields = line.split(",", -1);
        if (fields.length < 35) return;

        String city = fields[1].trim();
        if (city.isEmpty()) return;

        // === COMPLETENESS ===
        int[] auditCols = {7, 18, 14, 16, 10, 27, 31};
        int total = auditCols.length;
        int missing = 0;
        for (int idx : auditCols) {
            if (idx >= fields.length || fields[idx].trim().isEmpty()
                    || fields[idx].trim().equals("null")) {
                missing++;
            }
        }
        try {
            Double.parseDouble(fields[7].trim());
        } catch (Exception e) { missing++; total++; }
        double completeness = ((double)(total - missing) / total) * 100.0;

        // === CONSISTENCY ===
        int violations = 0;
        int checks = 5;
        try {
            double temp = Double.parseDouble(fields[7].trim());
            if (temp < -30 || temp > 45) violations++;
        } catch (Exception e) { violations++; }
        try {
            double humidity = Double.parseDouble(fields[18].trim());
            if (humidity < 0 || humidity > 100) violations++;
        } catch (Exception e) { violations++; }
        try {
            double pressure = Double.parseDouble(fields[14].trim());
            if (pressure < 950 || pressure > 1050) violations++;
        } catch (Exception e) { violations++; }
        try {
            double precip = Double.parseDouble(fields[16].trim());
            if (precip < 0) violations++;
        } catch (Exception e) { violations++; }
        try {
            double wind = Double.parseDouble(fields[10].trim());
            if (wind < 0) violations++;
        } catch (Exception e) { violations++; }
        double consistency = ((double)(checks - violations) / checks) * 100.0;

        // === OUTLIER ===
        int outliers = 0;
        int ochecks = 4;
        try {
            double pm25 = Double.parseDouble(fields[31].trim());
            if (pm25 > 35) outliers++;
        } catch (Exception e) { outliers++; }
        try {
            double co = Double.parseDouble(fields[27].trim());
            if (co > 400) outliers++;
        } catch (Exception e) { outliers++; }
        try {
            double temp = Double.parseDouble(fields[7].trim());
            if (temp > 40 || temp < -60) outliers++;
        } catch (Exception e) { outliers++; }
        try {
            double precip = Double.parseDouble(fields[16].trim());
            if (precip > 100) outliers++;
        } catch (Exception e) { outliers++; }
        double outlier = ((double)(ochecks - outliers) / ochecks) * 100.0;

        // Emit all 3 in one value
        String combined = String.format("%.2f:%.2f:%.2f", completeness, consistency, outlier);
        context.write(new Text(city), new Text(combined));
    }
}
