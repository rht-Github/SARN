from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
from msrest.authentication import CognitiveServicesCredentials
import time
from dotenv import load_dotenv
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import shutil
import pathlib
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from PIL import Image

##variables global
SORTEO_ID="1393"
SALVAR="N"
FOLDER_PATH="C:\\Selftraining\\PycharmProjects\\pythonProject1\\venv\\workDir\\Azure\\OCR\\imagenes\\automatico\\"
DIRECTORIO_ORIGEN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images\\automatico")
DIRECTORIO_DESTINO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images\\automatico_procesado")
LIST_ID = 1

def main():
	try:
		jugadas_main=[]
		imagenes_main=[]
		# Get Configuration Settings
		load_dotenv()
		endpoint = 'https://ocrdemorht.cognitiveservices.azure.com/'
		key = '28ecffd3ecc84e2e95a68b62473c9635'
		# Authenticate Computer Vision client
		computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(key))

		# Extract test
		with os.scandir(DIRECTORIO_ORIGEN) as ficheros:
			for fichero in ficheros:
				print(fichero.name)
				jugadas_main = get_text(DIRECTORIO_ORIGEN+"\\"+fichero.name, computervision_client)
				#convesion de string a int
				procesa_jugadas(jugadas_main, LIST_ID)
				#shutil.move(DIRECTORIO_ORIGEN+"\\"+fichero.name, DIRECTORIO_DESTINO+"\\"+fichero.name)
	except Exception as ex:
		print(ex)

#leer mas imagenes contenidas en el directorio origen
def leer_directorio_origen():
	print(DIRECTORIO_ORIGEN)
	contenido = os.listdir(DIRECTORIO_ORIGEN)

	imagenes=[]
	for fichero in contenido:
		if os.path.isfile(os.path.join(DIRECTORIO_ORIGEN, fichero)) and fichero.endswith('.png'):
			imagenes.append(fichero)
	#print(imagenes)
	return imagenes


	#		read_image_path = os.path.join(images_folder, "test1.jpeg")
	#		get_text(read_image_path, computervision_client)
	print('\n')
	#		read_image_path = os.path.join(images_folder, "notes2.jpg")
	#		get_text(read_image_path, computervision_client)
	print('\n')
#	read_image_path = os.path.join(images_folder, "1355_21_40.png")


def get_text(image_file, computervision_client):
	jugadas = []
	# Open local image file
	with open(image_file, "rb") as image:
		# Call the API
		read_response = computervision_client.read_in_stream(image, raw=True)

	# Get the operation location (URL with an ID at the end)
	read_operation_location = read_response.headers["Operation-Location"]
	# Grab the ID from the URL
	operation_id = read_operation_location.split("/")[-1]

	# Retrieve the results
	while True:
		read_result = computervision_client.get_read_result(operation_id)
		if read_result.status.lower() not in ['notstarted', 'running']:
			break
		time.sleep(1)

	# Get the detected text
	#if read_result.status == OperationStatusCodes.succeeded:
	for page in read_result.analyze_result.read_results:
		for line in page.lines:
			# Print line
			#print(line.text + " " + SORTEO_ID )
			jugadas.append (line.text + " " + SORTEO_ID)
	return jugadas


def procesa_jugadas(jugadas, list_id):
	#lista_numerica=[]
	id ="0"
	#print(len(jugadas))
	print("----------")
	for i in range(len(jugadas)):
		try:
			#print(jugadas[i]," len", len(jugadas[i]))
			if len(jugadas[i]) == 11:
				id=jugadas[i][1:6]
			else:
				b1 = jugadas[i][0:2]
				b2 = jugadas[i][3:5]
				b3 = jugadas[i][6:8]
				b4 = jugadas[i][9:11]
				b5 = jugadas[i][12:14]
				b6 = jugadas[i][15:17]
				sorteo_id = jugadas[i][18:22]
				#print(int(id),int(b1),int(b2),int(b3),int(b4),int(b5),int(b6),int(sorteo_id))
				#crear_insert_stmts(int(id),int(b1),int(b2),int(b3),int(b4),int(b5),int(b6),int(sorteo_id)
				#				 , archivo_salida)
				#ejecutar procedimiento de base de datos para insertar las jugadas
				ejecutar_procedimiento(id, b1, b2, b3, b4, b5, b6, list_id)
		except ValueError as err:
			print("Could not convert data to an integer.",err)
		except Exception as err:
			print(f"Unexpected {err=}, {type(err)=}")
			raise


def crear_insert_stmts(id, b1, b2, b3, b4, b5 ,b6, sorteo_id, archivo_salida):
	insert_base = "INSERT INTO OLAP_SYS.GL_INTERFACE(GL_ID,COMB1,COMB2,COMB3,COMB4,COMB5,COMB6,SORTEO_ID) VALUES("
	insert_stmt = insert_base + str(id) +","+ str(b1)+","+ str(b2)+","+ str(b3)+","+ str(b4)+","+ str(b5)+","+ str(b6)+","+ str(sorteo_id)+");\n"
	print (insert_stmt)
	archivo_salida.write(insert_stmt)

def ejecutar_procedimiento(id, ia1, ia2, ia3, ia4, ia5, ia6, list_id):

	try:
		# conectando a la base de datos
		conn = cx_Oracle.connect('olap_sys/Ingenier1a@//localhost:1521/lcl')
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			print(id, ia1, ia2, ia3, ia4, ia5, ia6, list_id)

			cursor = conn.cursor()

			# Llamar al procedimiento almacenado con dos parámetros de tipo cadena
			cursor.callproc('W_GL_AUTOMATICAS_PKG.INS_GL_AUTOMATICAS_HANDLER', ["mrtr", id, ia1, ia2, ia3, ia4, ia5, ia6, list_id])

		except Exception as err:
			print('Exception raised while executing the procedure', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()


if __name__ == "__main__":
	main()