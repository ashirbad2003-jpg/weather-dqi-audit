# Parse DQI output
with open('/home/ashirbad2003/weather_audit/output.txt', 'r') as f:
    lines = f.readlines()

commands = []
count = 0

for line in lines:
    line = line.strip()
    if not line: continue
    parts = line.split('\t')
    if len(parts) < 2: continue
    city = parts[0].strip().replace("'", "")
    metrics = parts[1].strip()
    data = {}
    for item in metrics.split():
        if '=' in item:
            k, v = item.split('=', 1)
            data[k] = v
    if not data: continue
    commands.append(f"put 'weather_audit', '{city}', 'dqi:completeness', '{data.get('completeness','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'dqi:consistency', '{data.get('consistency','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'dqi:outlier', '{data.get('outlier','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'dqi:score', '{data.get('DQI','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'dqi:tag', '{data.get('tag','')}'")
    count += 1

# Parse Anomaly output
with open('/home/ashirbad2003/weather_audit/anomaly.txt', 'r') as f:
    lines = f.readlines()

for line in lines:
    line = line.strip()
    if not line: continue
    parts = line.split('\t')
    if len(parts) < 2: continue
    city = parts[0].strip().replace("'", "")
    metrics = parts[1].strip()
    data = {}
    for item in metrics.split():
        if '=' in item:
            k, v = item.split('=', 1)
            data[k] = v
    if not data: continue
    commands.append(f"put 'weather_audit', '{city}', 'anomaly:status', '{data.get('status','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'anomaly:rate', '{data.get('rate','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'anomaly:records', '{data.get('records','')}'")
    commands.append(f"put 'weather_audit', '{city}', 'anomaly:anomalies', '{data.get('anomalies','')}'")

# Parse Country output
with open('/home/ashirbad2003/weather_audit/country.txt', 'r') as f:
    lines = f.readlines()

for line in lines:
    line = line.strip()
    if not line: continue
    parts = line.split('\t')
    if len(parts) < 2: continue
    country = parts[0].strip().replace("'", "")
    metrics = parts[1].strip()
    data = {}
    for item in metrics.split():
        if '=' in item:
            k, v = item.split('=', 1)
            data[k] = v
    if not data: continue
    commands.append(f"put 'weather_country', '{country}', 'stats:avg_temp', '{data.get('avg_temp','')}'")
    commands.append(f"put 'weather_country', '{country}', 'stats:avg_pm25', '{data.get('avg_pm25','')}'")
    commands.append(f"put 'weather_country', '{country}', 'stats:country_dqi', '{data.get('country_dqi','')}'")
    commands.append(f"put 'weather_country', '{country}', 'stats:tag', '{data.get('tag','')}'")
    commands.append(f"put 'weather_country', '{country}', 'stats:cities', '{data.get('cities','')}'")

commands.append('exit')

with open('/home/ashirbad2003/weather_audit/hbase_load.sh', 'w') as f:
    f.write('\n'.join(commands))

print(f"Generated {len(commands)-1} HBase commands")
print(f"  - DQI: {count} cities")
