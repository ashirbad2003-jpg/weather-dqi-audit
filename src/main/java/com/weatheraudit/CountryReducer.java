package com.weatheraudit;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class CountryReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double tempSum = 0, humSum = 0, precipSum = 0, pm25Sum = 0;
        int count = 0;
        Set<String> cities = new HashSet<>();

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length < 5) continue;
            try {
                cities.add(parts[0]);
                tempSum   += Double.parseDouble(parts[1]);
                humSum    += Double.parseDouble(parts[2]);
                precipSum += Double.parseDouble(parts[3]);
                pm25Sum   += Double.parseDouble(parts[4]);
                count++;
            } catch (Exception e) { continue; }
        }

        if (count == 0) return;

        double avgTemp   = tempSum   / count;
        double avgHum    = humSum    / count;
        double avgPrecip = precipSum / count;
        double avgPm25   = pm25Sum   / count;

        // Country level DQI approximation
        double tempScore   = (avgTemp   >= -30 && avgTemp   <= 45)  ? 100 : 50;
        double humScore    = (avgHum    >= 0   && avgHum    <= 100) ? 100 : 50;
        double pm25Score   = avgPm25 <= 35 ? 100 : avgPm25 <= 75 ? 70 : 40;
        double countryDQI  = (0.35 * tempScore) + (0.35 * humScore) + (0.30 * pm25Score);

        String risk;
        if (countryDQI >= 98) risk = "TRUSTED";
        else if (countryDQI >= 92) risk = "MODERATE";
        else risk = "POOR";

        String result = String.format(
            "cities=%d records=%d avg_temp=%.2f avg_humidity=%.2f " +
            "avg_precip=%.2f avg_pm25=%.2f country_dqi=%.2f tag=%s",
            cities.size(), count, avgTemp, avgHum,
            avgPrecip, avgPm25, countryDQI, risk
        );

        context.write(key, new Text(result));
    }
}
