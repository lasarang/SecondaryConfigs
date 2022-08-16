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
coordenadas = [
	[-2.059503, -79.903884],
	[-2.059637, -79.903812],
	[-2.059538, -79.904002],
	[-2.059643, -79.903936],
	[-2.059589, -79.903920]
]

#InfluxDB config
influxDB = {
	'TOKEN' : 'bJdAFfHIRJOehHexyebAYtZ8Q2ED8dibdco_DiMnDIXHC8L8GHIwUp6FAXI-LekxTTQobg_1zz2VLasfqsg2XA==',
	'URL' :'https://basetsdb-cidis.ngrok.io/',
	'ORG' : 'ESPOL',
	'BUCKET' : 'Tester',
}

client = InfluxDBClient(
				url = influxDB['URL'], \
				token = influxDB['TOKEN'], \
				ssl_ca_cert = certifi.where()\
		)

write_api = client.write_api(write_options = SYNCHRONOUS)


#Django server config
url_server = 'https://server-cidis.ngrok.io/'


# Functions    
def get_finca(id_pi: str):
	response =requests.get(
		'{}info/sensor/raspberry_umbrales/{}'.format(url_server, id_pi),\
		auth = HTTPBasicAuth('CIDIS-ESPOL', 'c1d1sESPOL2021')
	)

	datos = json.loads(response.content.decode())
	print("GET Finca: ", datos)

	return {
			'finca': datos['finca'], 
			'cultivo': datos['cultivo'],
			'user' : datos['user'], 
			'min_temperatura' : datos["minimo_temperatura"],
			'max_temperatura' : datos["maximo_temperatura"],

			'min_humedad' : datos["minimo_humedad"],
			'max_humedad' : datos["maximo_humedad"],

			'min_precipitacion' : datos["minimo_precipitacion"],
			'max_precipitacion' : datos["maximo_precipitacion"],

			'min_radiacion' : datos["minimo_radiacion"],
			'max_radiacion' : datos["maximo_radiacion"]
	}

def read_data_line(df, d): 
	numero = df["id sensor"][d]
	timestamp = time_ns()
	return {
		'name': "Nodo" + str(numero),
		'temperatura' : df["temperatura"][d],
		'humedad' : df["humedad"][d],
		'precipitacion': (random.random() * 1023),
		'radiacion' : random.random() * 65000,
		'latitud' : coordenadas[numero][0],
		'longitud' : coordenadas[numero][1],
		'time': timestamp
	}

def build_point(medida: str, line:dict, finca_data:dict): 
	
	return Point(medida)\
			.tag("planta",finca_data['cultivo'])\
			.tag("finca",finca_data['finca'],)\
			.tag("id_sensor", line['name'])\
			.tag("usuario", finca_data['user'])\
			.field("valor",line[medida])\
			.field("minimo",finca_data['min_{}'.format(medida)])\
			.field("maximo",finca_data['max_{}'.format(medida)])\
			.field("latitud", line['latitud'])\
			.field("longitud", line['longitud'])\
			.time(line['time'], WritePrecision.NS)

def main():
	id_pi:str = 'TEST-0000001'
	finca_data:dict = get_finca(id_pi)

	#aduino = serial.Serial('/dev/ttyUSB0',9600)
	#arduino.flushInput()
	df = pd.read_csv("data.csv",sep=';')

	while True:
		try:
			for d in range(len(df)):
				print("\nActivo, leyendo linea #"+str((d+1))+"...")
				
				line = read_data_line(df, d)

				print("\ntemperatura: {}".format(line['temperatura']))
				print("humedad: {}".format(line['humedad']))
				print("precipitacion: {}".format(line['precipitacion']))
				print("radiacion: {}\n".format(line['radiacion']))

				
				point_temperatura = build_point('temperatura', line, finca_data)
				point_humedad = build_point('humedad', line, finca_data)
				point_precipitacion = build_point('precipitacion', line, finca_data)
				point_radiacion = build_point('radiacion', line, finca_data)

				write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_temperatura)
				write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_humedad)
				write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_precipitacion)
				write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_radiacion)
				
				sleep(tiempo)
				
		except KeyboardInterrupt:
			print('Received order to stop')
			break

main()