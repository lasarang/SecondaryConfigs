from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.auth import HTTPBasicAuth
from time import sleep, time, time_ns
import time
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
	#'URL' :'https://basetsdb-cidis.ngrok.io/',
	'URL' :'http://200.10.147.120:80/',
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
#url_server = 'https://server-cidis.ngrok.io/'
url_server = 'http://localhost:4000/'


# Functions    
def get_id_finca(id_pi: str):
	response =requests.get(
		'{}info/sensor/raspberry_umbrales/{}'.format(url_server, id_pi),\
		auth = HTTPBasicAuth('CIDIS-ESPOL', 'c1d1sESPOL2021')
	)

	datos = json.loads(response.content.decode())
	#print("GET Finca: {}/n".format(datos))

	return datos['id_finca']
	
#Lista de diccionarios con los usuarios 
def get_users_finca(id_finca: str):
	response = requests.get(
		'{}info/crop/user_farms_new?finca={}'.format(url_server, id_finca),\
		auth = HTTPBasicAuth('CIDIS-ESPOL', 'c1d1sESPOL2021')
	)
	datos = json.loads(response.content.decode())

	user_dicts = []

	for data in datos:
		user_dicts.append(
			{
				'finca': data['finca']['nombre'], 
				'cultivo': data['cultivo'],
				'user' : data['user']['user_tag'], 
				'min_temperatura' : data["minimo_temperatura"],
				'max_temperatura' : data["maximo_temperatura"],

				'min_humedad' : data["minimo_humedad"],
				'max_humedad' : data["maximo_humedad"],

				'min_precipitacion' : data["minimo_precipitacion"],
				'max_precipitacion' : data["maximo_precipitacion"],

				'min_radiacion' : data["minimo_radiacion"],
				'max_radiacion' : data["maximo_radiacion"]
			}
		)
	return user_dicts

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

def build_point(medida: str, line:dict, user_data:dict): 
	
	return Point(medida)\
			.tag("planta",user_data['cultivo'])\
			.tag("finca",user_data['finca'])\
			.tag("id_sensor", line['name'])\
			.tag("usuario", user_data['user'])\
			.field("valor",line[medida])\
			.field("minimo",user_data['min_{}'.format(medida)])\
			.field("maximo",user_data['max_{}'.format(medida)])\
			.field("latitud", line['latitud'])\
			.field("longitud", line['longitud'])\
			.time(line['time'], WritePrecision.NS)

def main():
	id_pi:str = 'TEST-0000001'
	id_finca:str = get_id_finca(id_pi)
	users_dicts = get_users_finca(id_finca)

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
				
				for user in users_dicts:
					point_temperatura = build_point('temperatura', line, user)
					point_humedad = build_point('humedad', line, user)
					point_precipitacion = build_point('precipitacion', line, user)
					point_radiacion = build_point('radiacion', line, user)

					write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_temperatura)
					write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_humedad)
					write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_precipitacion)
					write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_radiacion)
					sleep(tiempo)
					return
						
		except KeyboardInterrupt:
			print('Received order to stop')
			break

main()