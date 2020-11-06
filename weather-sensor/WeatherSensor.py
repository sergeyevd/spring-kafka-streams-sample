import time
from multiprocessing import Process, Queue

import random
import requests
import datetime
import sys

hostname = sys.argv[1]
device_id = sys.argv[2]
has_real_sensor = sys.argv[3] or False
min_hum = float(sys.argv[4])
max_hum = float(sys.argv[5])

def getMeasurements():
  if has_real_sensor == 'True':
    return getDHT22Measurements()
  else:
    return { 'temp': 27.456, 'hum': random.uniform(min_hum, max_hum) }

def getDHT22Measurements():
  import Adafruit_DHT as dht
  #Set DATA pin
  DHT = 4
  print("Getting measurement from the sensor...")
  h,t = dht.read_retry(dht.DHT22, DHT)
  return { 'temp': t, 'hum': h }


def collectMeasurements(measurements):
  while (1):
    timestamp = datetime.datetime.utcnow().strftime(
      "%Y-%m-%dT%H:%M:%S") + "+00:00"
    collected_measurements = getMeasurements()
    measurement = {
      "deviceId": device_id,
      "temperatureCelsius": collected_measurements['temp'],
      "humidity": collected_measurements['hum'],
      "timestamp": timestamp
    }
    measurements.put(measurement)
    print(f"Collected measurement: [{measurement}]")
    time.sleep(0.7)





def sendMeasurement(measurement):
  try:
    requests.post(url=f'http://{hostname}/api/v1/weather', json=measurement)
    print(f"Sent a measurement: [{measurement}]")
  except:
    print("Got an error while sending a measurement, retrying...")
    time.sleep(0.5)
    sendMeasurement(measurement)


if __name__ == '__main__':
  measurements = Queue()
  Process(target=collectMeasurements, args=([measurements])).start()
  while (1):
    sendMeasurement(measurements.get(block=True))
