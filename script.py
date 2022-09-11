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

def get_indicador(precipitacion:int):
	if precipitacion >= 0 and precipitacion < 300:
		return "Está lloviendo con alta intensidad"
	elif precipitacion >= 300 and precipitacion < 600:
		return  "Está lloviendo con intensidad moderada"
	elif precipitacion >= 600 and precipitacion < 800:
		return "Está lloviendo con baja intensidad"
	elif precipitacion >= 800 and precipitacion < 900:
		return "Se encuentra brisando"
	else:
		return "No hay lluvia"

def read_data_line(line:str): 
	data = line.split('-')
	i = random.randint(0,4)
	timestamp = time_ns()
	precipitacion = int(data[4])
	return {
		'name': "Nodo" + data[0],
		'temperatura' : float(data[1]),
		'humedad' : float(data[2]),
		'precipitacion': precipitacion,
		'indicador': get_indicador(precipitacion),
		'radiacion' : float(data[3]),
		'latitud' : coordenadas[i][0],
		'longitud' : coordenadas[i][1],
		'time': timestamp
	}

def build_point(medida: str, data:dict, user_data:dict): 

	point = Point(medida)\
			.tag('planta',user_data['cultivo'])\
			.tag('finca',user_data['finca'],)\
			.tag('id_sensor', data['name'])\
			.tag('usuario', user_data['user'])\
			.field('valor', data[medida])\
			.field('minimo',user_data['min_{}'.format(medida)])\
			.field('maximo',user_data['max_{}'.format(medida)])\
			.field('latitud', data['latitud'])\
			.field('longitud', data['longitud'])\
			.time(data['time'], WritePrecision.NS)

	if(medida == 'precipitacion'):
		return point.field('indicador', data['indicador'])

	return point
	
	
def main():
	id_pi:str = 'TEST-0000001'
	id_finca:str = get_id_finca(id_pi)
	users_dicts = get_users_finca(id_finca)
	print('{}/n'.format(users_dicts))

	arduino = serial.Serial('/dev/ttyUSB0',9600)
	arduino.flushInput()

	while True:
		try:
			datos = arduino.readline()
			linea = datos.decode('latin-1').strip()
			lineas = linea.split('\n')

			for line in lineas:

				if (len(line) != 0) and (len(line) > 2):
					print(line)
					data = read_data_line(line)

					print("\ntemperatura: {}".format(data['temperatura']))
					print("humedad: {}".format(data['humedad']))
					print("precipitacion: {}".format(data['precipitacion']))
					print("radiacion: {}\n".format(data['radiacion']))

					for user in users_dicts:
						point_temperatura = build_point('temperatura', data, user)
						point_humedad = build_point('humedad', data, user)
						point_precipitacion = build_point('precipitacion', data, user)
						point_radiacion = build_point('radiacion', data, user)

						write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_temperatura)
						write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_humedad)
						write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_precipitacion)
						write_api.write(influxDB['BUCKET'], influxDB['ORG'], point_radiacion)
						sleep(tiempo)
				
		except KeyboardInterrupt:
			print('Received order to stop')
			break

main()