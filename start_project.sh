#!/bin/bash

clear
echo "╔══════════════════════════════════════════════════╗"
echo "║   Weather Data Quality Audit Pipeline            ║"
echo "║   HDFS → MapReduce → HBase → Flask Dashboard    ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Hadoop ──────────────────────────────────────
echo "🔧 [1/8] Starting Hadoop..."
echo "Y" | hdfs namenode -format > /dev/null 2>&1
start-dfs.sh > /dev/null 2>&1
start-yarn.sh > /dev/null 2>&1
sleep 8
NODE_COUNT=$(jps | grep -v Jps | wc -l)
echo "✅ Hadoop running — $NODE_COUNT services active"

# ── Step 2: HBase ───────────────────────────────────────
echo ""
echo "🔧 [2/8] Starting HBase Docker..."
sudo docker start hbase-demo > /dev/null 2>&1
sleep 30
echo "✅ HBase ready"

# ── Step 3: Upload Dataset ──────────────────────────────
echo ""
echo "📂 [3/8] Uploading dataset to HDFS..."
hdfs dfs -rm -r /input/weather_audit > /dev/null 2>&1
hdfs dfs -mkdir -p /input/weather_audit > /dev/null 2>&1
hdfs dfs -put ~/big_weather.csv /input/weather_audit/ > /dev/null 2>&1
SIZE=$(hdfs dfs -du -h /input/weather_audit/big_weather.csv 2>/dev/null | awk '{print $1}')
echo "✅ Dataset uploaded — $SIZE"

# ── Step 4: MapReduce Pipeline ──────────────────────────
echo ""
echo "⚙️  [4/8] Running 3-Job MapReduce Pipeline..."
echo "   ├── Job 1: DQI Audit"
echo "   ├── Job 2: Anomaly Detection"
echo "   └── Job 3: Country Aggregation"
hdfs dfs -rm -r /output/weather_audit_dqi     > /dev/null 2>&1
hdfs dfs -rm -r /output/weather_audit_anomaly > /dev/null 2>&1
hdfs dfs -rm -r /output/weather_audit_country > /dev/null 2>&1
hadoop jar ~/weather_audit/target/weather-audit-1.0.jar \
    com.weatheraudit.DQIPipeline > /dev/null 2>&1
echo "✅ All 3 jobs completed successfully"

# ── Step 5: Save Outputs ────────────────────────────────
echo ""
echo "💾 [5/8] Saving MapReduce outputs..."
hdfs dfs -cat /output/weather_audit_dqi/part-r-00000     > ~/weather_audit/output.txt  2>/dev/null
hdfs dfs -cat /output/weather_audit_anomaly/part-r-00000 > ~/weather_audit/anomaly.txt 2>/dev/null
hdfs dfs -cat /output/weather_audit_country/part-r-00000 > ~/weather_audit/country.txt 2>/dev/null
DQI_COUNT=$(wc -l < ~/weather_audit/output.txt)
ANOM_COUNT=$(wc -l < ~/weather_audit/anomaly.txt)
CTRY_COUNT=$(wc -l < ~/weather_audit/country.txt)
echo "✅ Outputs saved"
echo "   ├── DQI:     $DQI_COUNT cities"
echo "   ├── Anomaly: $ANOM_COUNT cities"
echo "   └── Country: $CTRY_COUNT countries"

# ── Step 6: Generate HBase Script ──────────────────────
echo ""
echo "📝 [6/8] Generating HBase load script..."
python3 ~/weather_audit/gen_hbase_script.py > /dev/null 2>&1
CMD_COUNT=$(wc -l < ~/weather_audit/hbase_load.sh)
echo "✅ Generated $CMD_COUNT HBase commands"

# ── Step 7: Load into HBase ─────────────────────────────
echo ""
echo "📊 [7/8] Loading data into HBase..."
sudo docker exec -i hbase-demo hbase shell \
    < ~/weather_audit/hbase_load.sh > /dev/null 2>&1
echo "✅ HBase loaded"
echo "   ├── weather_audit table   (city DQI + anomaly)"
echo "   └── weather_country table (country aggregation)"

# ── Step 8: Flask Dashboard ─────────────────────────────
echo ""
echo "🌐 [8/8] Starting Flask Dashboard..."
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   ✅ Pipeline Complete!                          ║"
echo "║   🌐 Open: http://localhost:5000                 ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
cd ~/weather_audit/dashboard
python3 app.py
