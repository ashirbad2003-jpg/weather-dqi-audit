import subprocess

def load_to_hbase():
    with open('/home/ashirbad2003/weather_audit/output.txt', 'r') as f:
        lines = f.readlines()

    print(f"Loading {len(lines)} cities into HBase...")
    success = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 2:
            continue

        city = parts[0].strip()
        metrics = parts[1].strip()

        # Parse metrics
        completeness = consistency = outlier = dqi = tag = None
        for item in metrics.split():
            if item.startswith('completeness='):
                completeness = item.split('=')[1]
            elif item.startswith('consistency='):
                consistency = item.split('=')[1]
            elif item.startswith('outlier='):
                outlier = item.split('=')[1]
            elif item.startswith('DQI='):
                dqi = item.split('=')[1]
            elif item.startswith('tag='):
                tag = item.split('=')[1]

        if not all([completeness, consistency, outlier, dqi, tag]):
            continue

        # Escape city name for HBase shell
        row_key = city.replace("'", "\\'")

        cmd = f"""echo "put 'weather_audit', '{row_key}', 'dqi:completeness', '{completeness}'" | sudo docker exec -i hbase-demo hbase shell 2>/dev/null"""
        subprocess.run(cmd, shell=True)

        cmd = f"""echo "put 'weather_audit', '{row_key}', 'dqi:consistency', '{consistency}'" | sudo docker exec -i hbase-demo hbase shell 2>/dev/null"""
        subprocess.run(cmd, shell=True)

        cmd = f"""echo "put 'weather_audit', '{row_key}', 'dqi:outlier', '{outlier}'" | sudo docker exec -i hbase-demo hbase shell 2>/dev/null"""
        subprocess.run(cmd, shell=True)

        cmd = f"""echo "put 'weather_audit', '{row_key}', 'dqi:score', '{dqi}'" | sudo docker exec -i hbase-demo hbase shell 2>/dev/null"""
        subprocess.run(cmd, shell=True)

        cmd = f"""echo "put 'weather_audit', '{row_key}', 'dqi:tag', '{tag}'" | sudo docker exec -i hbase-demo hbase shell 2>/dev/null"""
        subprocess.run(cmd, shell=True)

        success += 1
        print(f"  Loaded: {city}")

    print(f"\nDone! {success}/{len(lines)} cities loaded.")

if __name__ == '__main__':
    load_to_hbase()
