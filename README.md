# Weather processor application

This is a sample application used in the "Using Kafka Streams with Spring Framework" speech.

It collects weather data from sensors via REST API, sends the data to Kafka and finally send alerts to the standart output of threshold values are reached.

The whole solution consists of 3 parts:

1) WeatherSensor.py - sample sensor application that can generate mock data or collect real measurements from DHT22 sensor connected to RaspberryPi
2) "REST API" application - simple Spring Boot microservice that exposes REST API endpoint for collecting weather data sent by WeatherSensor.py, it then forwards the data to a Kafka topic.
3) "Streams" application - Spring Boot application that uses Spring Cloud Stream Kafka Streams to listen for weather event and applies threshold processing (simply logs alerts to standard output).