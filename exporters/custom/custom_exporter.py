from prometheus_client import start_http_server, Gauge, Counter
import time, requests, random

# --- Метрики ---
temp_c = Gauge('weather_temperature_c', 'Temperature, C', ['city'])
windspeed = Gauge('weather_wind_speed', 'Wind speed, m/s', ['city'])
humidity = Gauge('weather_humidity_percent', 'Humidity, %', ['city'])
api_ok = Counter('weather_api_ok_total', 'Successful API polls', ['city'])
api_err = Counter('weather_api_err_total', 'Failed API polls', ['city'])
latency_ms = Gauge('weather_api_latency_ms', 'API latency in ms', ['city'])
rand01 = Gauge('exporter_random_0_1', 'Random 0..1', ['city'])
last_scrape_ts = Gauge('exporter_last_scrape_ts', 'Last scrape Unix time', ['city'])

# --- Города и координаты ---
CITIES = {
    "Almaty": (43.238949, 76.889709),
    "Astana": (51.169392, 71.449074),
    "Shymkent": (42.3417, 69.5901),
}

def poll_weather():
    for city, (lat, lon) in CITIES.items():
        t0 = time.time()
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
            j = requests.get(url, timeout=5).json()
            cur = j.get('current_weather', {})
            temp_c.labels(city=city).set(cur.get('temperature', 0))
            windspeed.labels(city=city).set(cur.get('windspeed', 0))
            humidity.labels(city=city).set(random.randint(35, 75))  # псевдо
            api_ok.labels(city=city).inc()
        except Exception:
            api_err.labels(city=city).inc()
        latency_ms.labels(city=city).set((time.time() - t0) * 1000)
        rand01.labels(city=city).set(random.random())
        last_scrape_ts.labels(city=city).set(time.time())

def main():
    start_http_server(8000)
    print("✅ Custom exporter running on :8000/metrics")
    while True:
        poll_weather()
        time.sleep(20)

if __name__ == "__main__":
    main()
