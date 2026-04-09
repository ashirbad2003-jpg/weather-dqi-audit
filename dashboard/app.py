from flask import Flask, render_template, jsonify

app = Flask(__name__)

def parse_dqi():
    cities = {}
    with open('/home/ashirbad2003/weather_audit/output.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split('\t')
            if len(parts) < 2: continue
            city = parts[0].strip()
            metrics = parts[1].strip()
            data = {}
            for item in metrics.split():
                if '=' in item:
                    k, v = item.split('=', 1)
                    data[k] = v
            cities[city] = data
    return cities

def parse_anomaly():
    cities = {}
    with open('/home/ashirbad2003/weather_audit/anomaly.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split('\t')
            if len(parts) < 2: continue
            city = parts[0].strip()
            metrics = parts[1].strip()
            data = {}
            for item in metrics.split():
                if '=' in item:
                    k, v = item.split('=', 1)
                    data[k] = v
            cities[city] = data
    return cities

def parse_country():
    countries = {}
    with open('/home/ashirbad2003/weather_audit/country.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split('\t')
            if len(parts) < 2: continue
            country = parts[0].strip()
            metrics = parts[1].strip()
            data = {}
            for item in metrics.split():
                if '=' in item:
                    k, v = item.split('=', 1)
                    data[k] = v
            countries[country] = data
    return countries

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    cities = parse_dqi()
    tags = {'TRUSTED': 0, 'MODERATE': 0, 'POOR': 0}
    scores = []
    labels = []
    for city, data in cities.items():
        tag = data.get('tag', 'POOR')
        if tag in tags: tags[tag] += 1
        try:
            scores.append(float(data.get('DQI', 0)))
            labels.append(city)
        except: pass
    return jsonify({'tags': tags, 'scores': scores, 'labels': labels})

@app.route('/api/data')
def api_data():
    return jsonify(parse_dqi())

@app.route('/api/anomaly')
def api_anomaly():
    data = parse_anomaly()
    statuses = {'NORMAL': 0, 'MEDIUM_ANOMALY': 0, 'HIGH_ANOMALY': 0}
    labels = []
    rates = []
    top_anomaly = []
    for city, d in data.items():
        status = d.get('status', 'NORMAL')
        if status in statuses: statuses[status] += 1
        try:
            rate = float(d.get('rate', '0%').replace('%', ''))
            labels.append(city)
            rates.append(rate)
            if rate > 3:
                top_anomaly.append({'city': city, 'rate': rate, 'status': status})
        except: pass
    top_anomaly = sorted(top_anomaly, key=lambda x: x['rate'], reverse=True)[:20]
    return jsonify({
        'statuses': statuses,
        'labels': labels,
        'rates': rates,
        'top_anomaly': top_anomaly
    })

@app.route('/api/country')
def api_country():
    data = parse_country()
    labels = []
    temps = []
    pm25s = []
    dqis = []
    tags = {'TRUSTED': 0, 'MODERATE': 0, 'POOR': 0}
    for country, d in data.items():
        labels.append(country)
        try: temps.append(float(d.get('avg_temp', 0)))
        except: temps.append(0)
        try: pm25s.append(float(d.get('avg_pm25', 0)))
        except: pm25s.append(0)
        try: dqis.append(float(d.get('country_dqi', 0)))
        except: dqis.append(0)
        tag = d.get('tag', 'POOR')
        if tag in tags: tags[tag] += 1
    return jsonify({
        'labels': labels,
        'temps': temps,
        'pm25s': pm25s,
        'dqis': dqis,
        'tags': tags
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
