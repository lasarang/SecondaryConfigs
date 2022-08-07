from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.auth import HTTPBasicAuth
from time import sleep, time, time_ns
import pandas as pd
import requests
import certifi
import serial
import random
import json

tiempo=1

# InfluxDB config
#token = 'dTEdjIbGNNTUB7oErdw1QR9S3klwMauHwTJkbfdFKwDx2btao9xWPb4A4AuK-Igwe9crWdWyDQCA07xWyCBGbQ=='
token = 'bJdAFfHIRJOehHexyebAYtZ8Q2ED8dibdco_DiMnDIXHC8L8GHIwUp6FAXI-LekxTTQobg_1zz2VLasfqsg2XA=='
url_influxDB = 'https://basetsdb-cidis.ngrok.io/'
org = "ESPOL"
bucket = "Tester"
client = InfluxDBClient(url=url_influxDB, token=token, ssl_ca_cert=certifi.where())
write_api = client.write_api(write_options=SYNCHRONOUS)

#Django server config
url_server = 'http://server-cidis.ngrok.io/'

#GET RPI info 
id = 'TEST-0000001'
response =requests.get(url_server + 'info/sensor/raspberry/' + id,\
	auth = HTTPBasicAuth('CIDIS-ESPOL', 'c1d1sESPOL2021'))

datos = json.loads(response.content.decode())
print("GET RPI sensor: ", datos)

cultivo = datos['cultivo']
finca = datos['finca']
user = datos['user']
id_cultivo = datos['id_cultivo']

#GET Cultivo/Umbral info 
response =requests.get(url_server + 'info/crop/crop/'+ str(id_cultivo),\
	auth = HTTPBasicAuth('CIDIS-ESPOL', 'c1d1sESPOL2021'))

datos = json.loads(response.content.decode())
print("GET Cultivo info: ", datos)

min_temperatura = datos["minimo_temperatura"]
max_temperatura = datos["maximo_temperatura"]

min_humedad = datos["minimo_humedad"]
max_humedad = datos["maximo_humedad"]

min_precipitacion = datos["minimo_precipitacion"]
max_precipitacion = datos["maximo_precipitacion"]

min_radiacion = datos["minimo_radiacion"]
max_radiacion = datos["maximo_radiacion"]

#Setting Coordenadas
coordenadas = [
	[-2.059503, -79.903884],
	[-2.059637, -79.903812],
	[-2.059538, -79.904002],
	[-2.059643, -79.903936],
	[-2.059589, -79.903920]
]

#Arduino config
'''
aduino = serial.Serial('/dev/ttyUSB0',9600)
arduino.flushInput()
'''

#Reading data.csv file
df = pd.read_csv("data.csv",sep=';')
#print(df)

while True:
	try:
		for d in range(len(df)):
			print("Activo, leyendo linea #"+str((d+1))+"...")
					
			temperatura = df["temperatura"][d]
			humedad = df["humedad"][d]
			numero = df["id sensor"][d]
			
			name = "Nodo" + str(numero)
			latitud = coordenadas[numero][0]
			longitud = coordenadas[numero][1]

			precipitacion = (random.random() * 1023)
			radiacion = random.random() * 65000

			timestamp = time_ns()

			print("\ntemperatura: {}".format(temperatura))
			print("humedad: {}".format(humedad))
			print("precipitacion: {}".format(precipitacion))
			print("radiacion: {}\n".format(radiacion))

			point_temperatura = Point("temperatura")\
				.tag("planta",cultivo)\
				.tag("finca",finca)\
				.tag("id_sensor", name)\
				.tag("usuario", user)\
				.field("valor",temperatura)\
				.field("minimo",min_temperatura)\
				.field("maximo",max_temperatura)\
				.field("latitud",latitud)\
				.field("longitud",longitud)\
				.time(timestamp, WritePrecision.NS)

			point_humedad = Point("humedad")\
				.tag("planta",cultivo)\
				.tag("finca",finca)\
				.tag("id_sensor", name)\
				.tag("usuario", user)\
				.field("valor",humedad)\
				.field("minimo",min_humedad)\
				.field("maximo",max_humedad)\
				.field("latitud",latitud)\
				.field("longitud",longitud)\
				.time(timestamp, WritePrecision.NS)

			point_precipitacion = Point("precipitacion")\
				.tag("planta",cultivo)\
				.tag("finca",finca)\
				.tag("id_sensor", name)\
				.tag("usuario", user)\
				.field("valor",precipitacion)\
				.field("minimo",min_precipitacion)\
				.field("maximo",max_precipitacion)\
				.field("latitud",latitud)\
				.field("longitud",longitud)\
				.time(timestamp, WritePrecision.NS)

			point_radiacion = Point("radiacion")\
				.tag("planta",cultivo)\
				.tag("finca",finca)\
				.tag("id_sensor", name)\
				.tag("usuario", user)\
				.field("valor", radiacion)\
				.field("minimo",min_radiacion)\
				.field("maximo",max_radiacion)\
				.field("latitud",latitud)\
				.field("longitud",longitud)\
				.time(timestamp, WritePrecision.NS)

			write_api.write(bucket, org, point_temperatura)
			write_api.write(bucket, org, point_humedad)
			write_api.write(bucket, org, point_precipitacion)
			write_api.write(bucket, org, point_radiacion)

	except KeyboardInterrupt:
		print('Received order to stop')
		break
    