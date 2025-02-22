import requests
import time
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter


def request_weather_data(s_city):
    ''' 
    Takes a city name string, calls a weather API and returns a 
    dictionary with the weather metrics
    '''
    s_api_url = f"https://wttr.in/{s_city}?format=j1"
    o_response = requests.get(s_api_url)
    return o_response 

def extract_weather_data_from_json(d_metric): 
    ''' 
    Takes dictionary with weather metrics, maps and returns the
    relevant values to a dictionary for "current condition" metrics
    '''
    d_weather_metrics = { 
        "i_temperature_feels_like_c": d_metric["current_condition"][0]["FeelsLikeC"],
        "i_cloud_cover": d_metric["current_condition"][0]["cloudcover"],
        "i_humidity": d_metric["current_condition"][0]["humidity"],
        "i_precipitation_mm": d_metric["current_condition"][0]["precipMM"],
        "i_pressure_pa": d_metric["current_condition"][0]["pressure"],
        "i_temperature_c": d_metric["current_condition"][0]["temp_C"],
        "i_uv_index": d_metric["current_condition"][0]["uvIndex"],
        "i_visibility": d_metric["current_condition"][0]["visibility"],
        "i_winddirection_degree": d_metric["current_condition"][0]["winddirDegree"],
        "i_windspeed_kmph": d_metric["current_condition"][0]["windspeedKmph"],
        "s_date_time_stamp": d_metric["current_condition"][0]["localObsDateTime"],
    }   
    return d_weather_metrics

# create HELP and TYPE annotations for prometheus metrics
o_temperature_feels_like_c = Gauge('temperature_feels_like_c', 'Returns the felt temperature in Celsius')
o_cloud_cover = Gauge('cloud_cover', 'Returns the cloud coverage indicated by a number, 0 is clear sky')
o_humidity = Gauge('atmospheric_humidity', 'Returns mesured air humidity')
o_precipitation_mm = Gauge('precipitation_mm', 'Returns precipitation in mm')
o_pressure_pa = Gauge('pressure_pa', 'Returns pressure mesured in pa')
o_temperature_c = Gauge('temperature_c', 'Returns temperature in C')
o_uv_index = Gauge('uv_index', 'Returns mesured UV index')
o_visibility = Gauge('atmospheric_visibility', 'Returns atmospheric visibility')
o_winddirection_degree = Gauge('winddirection_degree', 'Returns mesured wind direction in degree')
o_windspeed_kmph = Gauge('windspeed_kmph', 'Returns windspeed in kmph')

if __name__ == '__main__':
    # start http server for prometheus on port 
    start_http_server(9200)

    while True:
        # date time stamp for notifications 
        s_date_time_stamp_check_raw = datetime.now()
        s_date_time_stamp_check = s_date_time_stamp_check_raw.replace(microsecond=0)
        try:
            # extract metrics
            o_full_weather_metrics = request_weather_data('<s_your_city>')
            # get and parse data
            d_full_weather_metrics = o_full_weather_metrics.json()
            d_metrics = extract_weather_data_from_json(d_full_weather_metrics)
            # print date-time-stamp from when the mesurement has been taken 
            s_date_time_stamp = d_metrics["s_date_time_stamp"]
            print(f"[{s_date_time_stamp_check}] - Last mesurement has been taken at: {s_date_time_stamp}")
            with open("log", "a") as file:
                file.write(f"\n[{s_date_time_stamp_check}] - Last mesurement has been taken at: {s_date_time_stamp}")

            # populate metric keys with weather data
            o_temperature_feels_like_c.set(d_metrics["i_temperature_feels_like_c"])
            o_cloud_cover.set(d_metrics["i_cloud_cover"])
            o_humidity.set(d_metrics["i_humidity"])
            o_precipitation_mm.set(d_metrics["i_precipitation_mm"])
            o_pressure_pa.set(d_metrics["i_pressure_pa"])
            o_temperature_c.set(d_metrics["i_temperature_c"])
            o_uv_index.set(d_metrics["i_uv_index"])
            o_visibility.set(d_metrics["i_visibility"])
            o_winddirection_degree.set(d_metrics["i_winddirection_degree"])
            o_windspeed_kmph.set(d_metrics["i_windspeed_kmph"])

            # sleep for 1h 
            i_sleep_time = 1 * 60 * 60  # 1*60*60 == 1h
            time.sleep(i_sleep_time)
        except:
            print(f"[{s_date_time_stamp_check}] - Request failed")
            with open("log", "a") as file:
                file.write(f"\n[{s_date_time_stamp_check}] - Request failed")
            # sleep for 10 minute and try again 
            time.sleep(10 * 60)

