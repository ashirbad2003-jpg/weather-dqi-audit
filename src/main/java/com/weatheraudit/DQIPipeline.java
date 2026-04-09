package com.weatheraudit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DQIPipeline {

    public static void main(String[] args) throws Exception {

        String input       = "/input/weather_audit/big_weather.csv";
        String outputDQI   = "/output/weather_audit_dqi";
        String outputAnom  = "/output/weather_audit_anomaly";
        String outputCountry = "/output/weather_audit_country";

        // ── Job 1: DQI Audit ──────────────────────────────────────
        System.out.println("\n>>> JOB 1: DQI Audit Starting...");
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job1 DQI Audit");
        job1.setJarByClass(DQIPipeline.class);
        job1.setMapperClass(UnifiedAuditMapper.class);
        job1.setReducerClass(DQIReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(outputDQI));
        if (!job1.waitForCompletion(true)) System.exit(1);
        System.out.println(">>> JOB 1: DONE ✅");

        // ── Job 2: Anomaly Detection ──────────────────────────────
        System.out.println("\n>>> JOB 2: Anomaly Detection Starting...");
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job2 Anomaly Detection");
        job2.setJarByClass(DQIPipeline.class);
        job2.setMapperClass(AnomalyMapper.class);
        job2.setReducerClass(AnomalyReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(input));
        FileOutputFormat.setOutputPath(job2, new Path(outputAnom));
        if (!job2.waitForCompletion(true)) System.exit(1);
        System.out.println(">>> JOB 2: DONE ✅");

        // ── Job 3: Country Aggregation ────────────────────────────
        System.out.println("\n>>> JOB 3: Country Aggregation Starting...");
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job3 Country Aggregation");
        job3.setJarByClass(DQIPipeline.class);
        job3.setMapperClass(CountryMapper.class);
        job3.setReducerClass(CountryReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(input));
        FileOutputFormat.setOutputPath(job3, new Path(outputCountry));
        if (!job3.waitForCompletion(true)) System.exit(1);
        System.out.println(">>> JOB 3: DONE ✅");

        System.out.println("\n🎉 ALL 3 JOBS COMPLETED SUCCESSFULLY!");
    }
}
