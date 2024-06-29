import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, HistGradientBoostingRegressor, HistGradientBoostingClassifier
from sklearn.metrics import mean_squared_error
from datetime import datetime, timedelta
from xgboost import XGBRegressor, XGBClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn.naive_bayes import GaussianNB
from statsmodels.tsa.arima.model import ARIMA
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor
import warnings

# Suprimir todas las advertencias (no recomendado a menos que sepas lo que estás haciendo)
warnings.filterwarnings("ignore")

#valores constantes
HISTORICO = 830
RANDOM_STATE = 42
TEST_SIZE = 0.2
ARREGLO_CONFIG = [[1,"Frecuencia","SELECT GAMBLING_DATE FECHA, CU1 P1, CU2 P2, CU3 P3, CU4 P4, CU5 P5, CU6 P6, GAMBLING_ID ID",1]
				 ,[2,"Ley_del_Tercio","SELECT GAMBLING_DATE FECHA, CLT1 P1, CLT2 P2, CLT3 P3, CLT4 P4, CLT5 P5, CLT6 P6, GAMBLING_ID ID",1]
				 ,[3,"Numerica","SELECT GAMBLING_DATE FECHA, COMB1 P1, COMB2 P2, COMB3 P3, COMB4 P4, COMB5 P5, COMB6 P6, GAMBLING_ID ID",1]
				 ,[4,"Primo_Impar_Par","SELECT FECHA, P1, P2, P3, P4, P5, P6, ID",2]
                 ,[5,"Preferente","SELECT FECHA, P1, P2, P3, P4, P5, P6, ID",3]
				  ]

#valores que se setean par a cada sorteo
GUARDA_PREDICCION=True

def recuperar_id_base ():
	try:
		# conectando a la base de datos
		conn = cx_Oracle.connect('olap_sys/Ingenier1a@//localhost:1521/lcl')
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			#construyendo el query
			query_stmt = "WITH MAX_ID_TBL AS ("
			query_stmt = query_stmt + " SELECT MAX(DRAWING_ID) MAX_ID"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " ), ULTIMA_JUGADA_CNT_TBL AS ("
			query_stmt = query_stmt + " SELECT DRAWING_ID"
			query_stmt = query_stmt + " , COUNT(1) JCNT"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE DRAWING_ID = (SELECT MAX_ID FROM MAX_ID_TBL)"
			query_stmt = query_stmt + " GROUP BY DRAWING_ID"
			query_stmt = query_stmt + " ), ULTIMA_JUGADA_MATCH_CNT_TBL AS ("
			query_stmt = query_stmt + " SELECT DRAWING_ID"
			query_stmt = query_stmt + " , COUNT(1) JCNT"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE DRAWING_ID = (SELECT MAX_ID FROM MAX_ID_TBL)"
			query_stmt = query_stmt + " AND WINNER_FLAG IS NULL"
			query_stmt = query_stmt + " GROUP BY DRAWING_ID"
			query_stmt = query_stmt + " ) SELECT CASE WHEN JCNT = (SELECT JCNT FROM ULTIMA_JUGADA_MATCH_CNT_TBL) THEN DRAWING_ID+1 ELSE DRAWING_ID END DRAWING_ID"
			query_stmt = query_stmt + " FROM ULTIMA_JUGADA_CNT_TBL"

			cursor = conn.cursor()
			cursor.execute(query_stmt)

			id_base = cursor.fetchone()

			# Retornar el valor obtenido
			if id_base:
				return id_base[0]  # Devolver el primer valor de la tupla
			else:
				return 0  # Retornar zero si no se encuentra ningún resultado
		except Exception as err:
			print('Exception raised while executing the query', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()

def crear_dataframe (select_stmt, historico, id_base, query_num):
	try:
		# conectando a la base de datos
		conn = cx_Oracle.connect('olap_sys/Ingenier1a@//localhost:1521/lcl')
		# conectando a la base de datos
		cursor = conn.cursor()
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			#formando el query
			if query_num == 1:
				query_stmt = select_stmt
				query_stmt = query_stmt + " FROM OLAP_SYS.MR_RESULTADOS_NUM_V"
				query_stmt = query_stmt + " WHERE GAMBLING_ID > (SELECT MAX(GAMBLING_ID) FROM OLAP_SYS.SL_GAMBLINGS) - " + str(historico)
				query_stmt = query_stmt + " AND GAMBLING_ID < " + str(id_base)
				query_stmt = query_stmt + " ORDER BY GAMBLING_ID"
			elif query_num == 2:
				query_stmt = select_stmt
				query_stmt = query_stmt + " FROM OLAP_SYS.RES_PRIMO_IMPAR_PAR_NUM_V "
				query_stmt = query_stmt + " ORDER BY ID"
			elif query_num == 3:
				query_stmt = "WITH RESULTADOS_TBL AS ( "
				query_stmt = query_stmt + " SELECT GAMBLING_DATE FECHA "
				query_stmt = query_stmt + " , DECODE(PXC1,NULL,0,1) PXC1, DECODE(PXC2,NULL,0,1) PXC2, DECODE(PXC3,NULL,0,1) PXC3 "
				query_stmt = query_stmt + " , DECODE(PXC4,NULL,0,1) PXC4, DECODE(PXC5,NULL,0,1) PXC5, DECODE(PXC6,NULL,0,1) PXC6 "
				query_stmt = query_stmt + " , DECODE(PRE1,NULL,0,2) PRE1, DECODE(PRE2,NULL,0,2) PRE2, DECODE(PRE3,NULL,0,2) PRE3 "
				query_stmt = query_stmt + " , DECODE(PRE4,NULL,0,2) PRE4, DECODE(PRE5,NULL,0,2) PRE5, DECODE(PRE6,NULL,0,2) PRE6 "
				query_stmt = query_stmt + " , GAMBLING_ID ID "
				query_stmt = query_stmt + " FROM OLAP_SYS.PM_MR_RESULTADOS_V2 "
				query_stmt = query_stmt + " WHERE GAMBLING_ID > 897) "
				query_stmt = query_stmt + " , OUTPUT_TBL AS (SELECT FECHA "
				query_stmt = query_stmt + " , CASE WHEN PXC1 = 0 AND PRE1 = 0 THEN 0 ELSE CASE WHEN PXC1 = 0 AND PRE1 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC1 > 0 AND PRE1 = 0 THEN 4 ELSE CASE WHEN PXC1 > 0 AND PRE1 > 0 THEN 5 ELSE 9 END END END END P1 "
				query_stmt = query_stmt + " , CASE WHEN PXC2 = 0 AND PRE2 = 0 THEN 0 ELSE CASE WHEN PXC2 = 0 AND PRE2 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC2 > 0 AND PRE2 = 0 THEN 4 ELSE CASE WHEN PXC2 > 0 AND PRE2 > 0 THEN 5 ELSE 9 END END END END P2 "
				query_stmt = query_stmt + " , CASE WHEN PXC3 = 0 AND PRE3 = 0 THEN 0 ELSE CASE WHEN PXC3 = 0 AND PRE3 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC3 > 0 AND PRE3 = 0 THEN 4 ELSE CASE WHEN PXC3 > 0 AND PRE3 > 0 THEN 5 ELSE 9 END END END END P3 "
				query_stmt = query_stmt + " , CASE WHEN PXC4 = 0 AND PRE4 = 0 THEN 0 ELSE CASE WHEN PXC4 = 0 AND PRE4 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC4 > 0 AND PRE4 = 0 THEN 4 ELSE CASE WHEN PXC4 > 0 AND PRE4 > 0 THEN 5 ELSE 9 END END END END P4 "
				query_stmt = query_stmt + " , CASE WHEN PXC5 = 0 AND PRE5 = 0 THEN 0 ELSE CASE WHEN PXC5 = 0 AND PRE5 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC5 > 0 AND PRE5 = 0 THEN 4 ELSE CASE WHEN PXC5 > 0 AND PRE5 > 0 THEN 5 ELSE 9 END END END END P5 "
				query_stmt = query_stmt + " , CASE WHEN PXC6 = 0 AND PRE6 = 0 THEN 0 ELSE CASE WHEN PXC6 = 0 AND PRE6 > 0 THEN 3 "
				query_stmt = query_stmt + " ELSE CASE WHEN PXC6 > 0 AND PRE6 = 0 THEN 4 ELSE CASE WHEN PXC6 > 0 AND PRE6 > 0 THEN 5 ELSE 9 END END END END P6 "
				query_stmt = query_stmt + " , ID "
				query_stmt = query_stmt + " FROM RESULTADOS_TBL) "
				query_stmt = query_stmt + select_stmt
				query_stmt = query_stmt + " FROM OUTPUT_TBL "
				query_stmt = query_stmt + " ORDER BY ID"

			#print(query_stmt)

			cursor.execute(query_stmt)
			# Convirtiendo el resultset en un DataFrame de Pandas
			columns = ['fecha', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'id']  # Nombres de columnas
			df = pd.DataFrame(cursor.fetchall(), columns=columns)
			#print(df.head(10))
			return df
		except Exception as err:
			print('Exception raised while executing the query', err)
	finally:
		# Cerrar el cursor
		cursor.close()

def crear_prediccion_RandomForestRegressor( posicion:int, fecha_sorteo, df, arreglo_entrada):
	#print("posicion: ", posicion)
	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva característica numérica para representar la fecha (número de días desde la primera fecha)
	df['DIAS'] = (df['fecha'] - df['fecha'].min()).dt.days
	#print("paso1")

	# Dividir los datos en conjunto de entrenamiento y prueba
	#X_train, X_test, y_train, y_test = train_test_split(df[['DIAS']], df[[posicion_str]], test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(df[['DIAS']], df[[posicion_str]], test_size=TEST_SIZE,
														random_state=RANDOM_STATE)

	# Entrenar un modelo de regresión (Random Forest en este caso)
	#model = RandomForestRegressor(n_estimators=200, max_depth= 10, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	model = RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=5, min_samples_leaf=2, max_features='sqrt', bootstrap=True, random_state=RANDOM_STATE)
	model.fit(X_train, y_train)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada
	if posicion == 1:
		arreglo_salida[0] = round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1] = round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2] = round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3] = round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4] = round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5] = round(prediccion[0])
	return arreglo_salida


def crear_prediccion_RandomForestClassifier( posicion:int, fecha_sorteo, df, arreglo_entrada):
	#print("posicion: ", posicion)
	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva característica numérica para representar la fecha (número de días desde la primera fecha)
	df['DIAS'] = (df['fecha'] - df['fecha'].min()).dt.days
	#print("paso1")

	# Dividir los datos en conjunto de entrenamiento y prueba
	#X_train, X_test, y_train, y_test = train_test_split(df[['DIAS']], df[[posicion_str]], test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(df[['DIAS']], df[[posicion_str]], test_size=TEST_SIZE,
														random_state=RANDOM_STATE)

	# Entrenar un modelo de regresión (Random Forest en este caso)
	#model = RandomForestClassifier(n_estimators=200, max_depth= 10, random_state=42)
	#esta modificacion es en base al articulo customer churn prediction
	model = RandomForestClassifier(n_estimators=100, max_depth=10, min_samples_split=5, min_samples_leaf=2, max_features='sqrt', bootstrap=True, random_state=RANDOM_STATE)
	model.fit(X_train, y_train)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada
	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_XGBRegressor( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos en conjunto de entrenamiento y prueba
	#X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
	#													test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
														test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Inicializar y entrenar el modelo con hiperparámetros optimizados
	#model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=RANDOM_STATE)
	model.fit(X_train, y_train)

	# Realizar la predicción para el siguiente elemento
	#siguiente_elemento = df.iloc[-1][['DiasDesdeInicio']].values.reshape(1, -1)
	#prediccion = model.predict(siguiente_elemento)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_HistGradientBoostingRegressor( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos en conjunto de entrenamiento y prueba
	#X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
	#													test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
														test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Inicializar y entrenar el modelo con hiperparámetros optimizados
	#model = HistGradientBoostingRegressor(max_iter=100, learning_rate=0.1, max_depth=5, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	model = HistGradientBoostingRegressor(max_iter=100, learning_rate=0.1, max_depth=5, random_state=RANDOM_STATE)
	model.fit(X_train, y_train)

	# Realizar la predicción para el siguiente elemento
	#siguiente_elemento = df.iloc[-1][['DiasDesdeInicio']].values.reshape(1, -1)
	#prediccion = model.predict(siguiente_elemento)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_HistGradientBoostingClassifier( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos en conjunto de entrenamiento y prueba
	#X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
	#													test_size=0.2, random_state=42)
	X_train, X_test, y_train, y_test = train_test_split(df[['DiasDesdeInicio']], df[posicion_str],
														test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Inicializar y entrenar el modelo
	#model = HistGradientBoostingClassifier(max_iter=100, learning_rate=0.1, max_depth=5, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	model = HistGradientBoostingClassifier(max_iter=100, learning_rate=0.1, max_depth=5, random_state=RANDOM_STATE)
	model.fit(X_train, y_train)

	# Realizar la predicción para el siguiente elemento
	#siguiente_elemento = df.iloc[-1][['DiasDesdeInicio']].values.reshape(1, -1)
	#prediccion = model.predict(siguiente_elemento)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_MLPClassifier( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos de entrenamiento y prueba
	#X_train = df[['DiasDesdeInicio']][:len(df) - 1]
	#y_train = df[posicion_str][:-1]
	X = df[['DiasDesdeInicio', 'id']]
	y = df[posicion_str]

	# Normalizar las características
	scaler = StandardScaler()
	X_scaled = scaler.fit_transform(X)

	# Dividir los datos en conjuntos de entrenamiento y prueba
	X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Inicializar y entrenar el modelo de red neuronal
	#model = MLPClassifier(hidden_layer_sizes=(100,), max_iter=100)
	model = MLPClassifier(hidden_layer_sizes=(15,), activation='relu', solver='adam', alpha=0.001, max_iter=100)
	model.fit(X_train, y_train)

	# Realizar la predicción para el siguiente elemento
	#siguiente_elemento = df.iloc[-1][['DiasDesdeInicio']].values.reshape(1, -1)
	#prediccion = model.predict(siguiente_elemento)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	prediccion = model.predict([[dias_desde_inicio]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_KNeighborsClassifier( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos de entrenamiento y prueba
	X = df[['DiasDesdeInicio']].values
	y = df[posicion_str].values

	#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Normalizar los datos
	scaler = StandardScaler()
	X_train_scaled = scaler.fit_transform(X_train)
	X_test_scaled = scaler.transform(X_test)

	# Definir los hiperparámetros a ajustar
	param_grid = {
		'n_neighbors': [3, 5, 7, 9],
		'weights': ['uniform', 'distance'],
		'metric': ['euclidean', 'manhattan']
	}

	# Crear el modelo k-NN
	knn = KNeighborsClassifier()

	# Realizar la búsqueda de hiperparámetros
	grid_search = GridSearchCV(knn, param_grid, cv=5, scoring='accuracy')
	grid_search.fit(X_train_scaled, y_train)

	# Obtener el mejor modelo
	best_knn = grid_search.best_estimator_

	# Hacer predicciones
	y_pred = best_knn.predict(X_test_scaled)
	#print("Predicción para la fecha 20-2-2024:", best_knn.predict(scaler.transform([[113]]))[0])

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(best_knn.predict(scaler.transform([[113]]))[0])
	if posicion == 2:
		arreglo_salida[1]=round(best_knn.predict(scaler.transform([[113]]))[0])
	if posicion == 3:
		arreglo_salida[2]=round(best_knn.predict(scaler.transform([[113]]))[0])
	if posicion == 4:
		arreglo_salida[3]=round(best_knn.predict(scaler.transform([[113]]))[0])
	if posicion == 5:
		arreglo_salida[4]=round(best_knn.predict(scaler.transform([[113]]))[0])
	if posicion == 6:
		arreglo_salida[5]=round(best_knn.predict(scaler.transform([[113]]))[0])
	return arreglo_salida


def crear_prediccion_GaussianNB( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos de entrenamiento y prueba
	X = df[['DiasDesdeInicio']].values
	y = df[posicion_str].values
	#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
	# esta modificacion es en base al articulo customer churn prediction
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Normalizar los datos
	scaler = StandardScaler()
	X_train_scaled = scaler.fit_transform(X_train)
	X_test_scaled = scaler.transform(X_test)

	# Definir los hiperparámetros a ajustar
	param_grid = {
		'var_smoothing': [1e-9, 1e-8, 1e-7, 1e-6, 1e-5]
	}

	# Crear el modelo Naive Bayes
	nb = GaussianNB()

	# Realizar la búsqueda de hiperparámetros
	grid_search = GridSearchCV(nb, param_grid, cv=5, scoring='accuracy')
	grid_search.fit(X_train_scaled, y_train)

	# Obtener el mejor modelo
	best_nb = grid_search.best_estimator_

	# Hacer predicciones
	y_pred = best_nb.predict(X_test_scaled)
	#print("Predicción para la fecha 20-2-2024:", best_knn.predict(scaler.transform([[113]]))[0])

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(best_nb.predict(scaler.transform([[113]]))[0])
	if posicion == 2:
		arreglo_salida[1]=round(best_nb.predict(scaler.transform([[113]]))[0])
	if posicion == 3:
		arreglo_salida[2]=round(best_nb.predict(scaler.transform([[113]]))[0])
	if posicion == 4:
		arreglo_salida[3]=round(best_nb.predict(scaler.transform([[113]]))[0])
	if posicion == 5:
		arreglo_salida[4]=round(best_nb.predict(scaler.transform([[113]]))[0])
	if posicion == 6:
		arreglo_salida[5]=round(best_nb.predict(scaler.transform([[113]]))[0])
	return arreglo_salida


def crear_prediccion_Arima( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Entrenar un modelo ARIMA
	model = ARIMA(df[posicion_str], order=(5, 1, 0))
	model_fit = model.fit()

    # Prediccion de siguinete valor
	prediccion = round(model_fit.forecast(steps=1).values[0])

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=prediccion
	if posicion == 2:
		arreglo_salida[1]=prediccion
	if posicion == 3:
		arreglo_salida[2]=prediccion
	if posicion == 4:
		arreglo_salida[3]=prediccion
	if posicion == 5:
		arreglo_salida[4]=prediccion
	if posicion == 6:
		arreglo_salida[5]=prediccion
	return arreglo_salida



def crear_prediccion_SVR( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos en conjunto de entrenamiento y prueba
	X = df[['DiasDesdeInicio', 'id']]
	y = df[posicion_str]

	# Normalizar las características
	scaler = StandardScaler()
	X_scaled = scaler.fit_transform(X)

	# Dividir los datos en conjuntos de entrenamiento y prueba
	X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Entrenar el modelo SVM
	svm_model = SVC(kernel='rbf')
	svm_model.fit(X_train, y_train)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	sorteo_siguiente = df['id'].max()+1
	X_objetivo = scaler.transform([[dias_desde_inicio, sorteo_siguiente]])
	prediccion = svm_model.predict(X_objetivo)

	# Calcular el error cuadrático medio
	mse = mean_squared_error(y_test, prediccion)
	print("Error cuadrático medio:", mse)

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_DecisionTreeRegressor( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Crear una nueva columna de tiempo para entrenar el modelo
	df['DiasDesdeInicio'] = (df['fecha'] - df['fecha'].min()).dt.days

	# Separar los datos en conjunto de entrenamiento y prueba
	X = df[['DiasDesdeInicio', 'id']]
	y = df[posicion_str]

	# Normalizar las características
	scaler = StandardScaler()
	X_scaled = scaler.fit_transform(X)

	# Dividir los datos en conjuntos de entrenamiento y prueba
	X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

	# Entrenar el modelo SVM
	dt_regressor = DecisionTreeRegressor(random_state=RANDOM_STATE)
	dt_regressor.fit(X_train, y_train)

	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	dias_desde_inicio = (fecha_prediccion - df['fecha'].min()).days
	sorteo_siguiente = df['id'].max()+1
	X_objetivo = scaler.transform([[dias_desde_inicio, sorteo_siguiente]])
	prediccion = dt_regressor.predict(X_objetivo)

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


def crear_prediccion_LinearRegression( posicion:int, fecha_sorteo, df, arreglo_entrada):

	# extraemos la info de cada columna
	posicion_str = 'p' + str(posicion)
	fecha_arr = df['fecha']
	pn_arr = df[posicion_str]

	# Imprimir el arreglo
	#print(f"fECHA: {fecha_arr}")
	#print(f"p1: {pn_arr}")

	# Convertir la columna 'FECHA' a formato de fecha
	df['fecha'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y')

	# Ordenar el DataFrame por fecha
	df = df.sort_values(by='fecha')

	# Dividir la fecha en año, mes y día
	df['year'] = df['fecha'].dt.year
	df['monnth'] = df['fecha'].dt.month
	df['day'] = df['fecha'].dt.day

	# Dividir los datos en características (X) y la variable objetivo (y)
	X = df[['year', 'monnth', 'day', 'id']]
	y = df[posicion_str]

	# Crear y entrenar el modelo de regresión lineal
	model = LinearRegression()
	model.fit(X, y)

	# Predecir el valor de P1 para la fecha 7-5-2024
	fecha_prediccion = datetime.strptime('7-5-2024', '%d-%m-%Y')
	year_prediccion = fecha_prediccion.year
	month_prediccion = fecha_prediccion.month
	day_prediccion = fecha_prediccion.day
	id_prediccion = df['id'].max()+1
	prediccion = model.predict([[year_prediccion, month_prediccion, day_prediccion, id_prediccion]])


	# Predecir el siguiente valor en base a la fecha del proximo sorteo
	fecha_prediccion = datetime.strptime(fecha_sorteo, '%d-%m-%Y')
	year_prediccion = fecha_prediccion.year
	month_prediccion = fecha_prediccion.month
	day_prediccion = fecha_prediccion.day
	id_prediccion = df['id'].max()+1
	prediccion = model.predict([[year_prediccion, month_prediccion, day_prediccion, id_prediccion]])

	#print(f'Predicción para el {fecha_sorteo} y posicion {posicion_str}: {round(prediccion[0])}')

	arreglo_salida = arreglo_entrada

	if posicion == 1:
		arreglo_salida[0]=round(prediccion[0])
	if posicion == 2:
		arreglo_salida[1]=round(prediccion[0])
	if posicion == 3:
		arreglo_salida[2]=round(prediccion[0])
	if posicion == 4:
		arreglo_salida[3]=round(prediccion[0])
	if posicion == 5:
		arreglo_salida[4]=round(prediccion[0])
	if posicion == 6:
		arreglo_salida[5]=round(prediccion[0])
	return arreglo_salida


#formatea la salida de la prediccion de 1,2 o 3 a R,G y B respectivamente
def formatear_prediccion(arreglo_entrada):
	arreglo_salida = arreglo_entrada
	#[if elemento == 1: return "R" elif elemento == 2: return "G" elif elemento == 3: return "B" else: return "#" for elemento in arreglo_entrada]
	for elemento in range(0,6):
		if arreglo_entrada[elemento] == 1:
			arreglo_salida[elemento] = "R"
		elif arreglo_entrada[elemento] == 2:
			arreglo_salida[elemento] = "G"
		elif arreglo_entrada[elemento] == 3:
			arreglo_salida[elemento] = "B"
		else: arreglo_salida[elemento] = "#"
	return arreglo_salida

#ejecutar procedimiento de base de datos para guardar las predicciones
def ejecutar_procedimiento(nombre, muestra, fecha, fecha_inicio, fecha_fin, tipo
						, prediccion1, prediccion2, prediccion3, prediccion4, prediccion5, prediccion6, id_base):

	try:
		# conectando a la base de datos
		conn = cx_Oracle.connect('olap_sys/Ingenier1a@//localhost:1521/lcl')
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			cursor = conn.cursor()

			# Llamar al procedimiento almacenado con dos parámetros de tipo cadena
			cursor.callproc('W_GL_AUTOMATICAS_PKG.PREDICCIONES_HANDLER', [nombre, muestra, fecha, fecha_inicio, fecha_fin, tipo
							, prediccion1, prediccion2, prediccion3, prediccion4, prediccion5, prediccion6, id_base])
		except Exception as err:
			print('Exception raised while executing the procedure', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()


#formate la fecha del sorteo como sigue DD-MM-YYYY
def formatear_fecha_sorteo (fecha_sorteo) -> str:
	# Convertir a objeto datetime si es necesario
	if isinstance(fecha_sorteo, str):
		fecha_sorteo = datetime.strptime(fecha_sorteo, '%Y-%m-%d %H:%M:%S')

	# Cambiar el formato de la fecha
	fecha_formateada = fecha_sorteo.strftime('%d-%m-%Y')

	# Convertir la fecha formateada a string
	fecha_formateada_string = str(fecha_formateada)
	return fecha_formateada_string


#se crea el dataframe recuperando info de la base de datos
#ejecuta el algoritmo de prediccion y si la bandera esta encendida llama un procedimiento de base de datos para
#guardar las predicciones
def ejecutar_tarea (select_stmt, mensaje, historico, id_base, guarda_prediccion, nombre_algoritmo, imprime_fecha_sorteo, query_num):
	prediccion_gl = [-1, -2, -3, -4, -5, -6]
	dataframe = crear_dataframe(select_stmt, historico, id_base, query_num)

	#Recuperar la fecha del penultimo sorteo
	fecha_objeto = datetime.strptime(dataframe.loc[dataframe.shape[0]-2,'fecha'], '%d-%m-%Y')

	#Calcular la fecha del proximo sorteo
	nueva_fecha_sumada = fecha_objeto + timedelta(days=7)

	#obtener la fecha del sorteo formateada
	fecha_sorteo = formatear_fecha_sorteo(nueva_fecha_sumada)
	#se imprime la fecha del sorteo solo para la 1er ejecucion del loop
	if imprime_fecha_sorteo:
		print("Fecha Sorteo:", fecha_sorteo)

	# este loop se ejecuta del 1 al 6 para cada posicion de la jugada
	for posicion in range(1,7):
		if nombre_algoritmo == "RandomForestRegressor":
			crear_prediccion_RandomForestRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "RandomForestClassifier":
			crear_prediccion_RandomForestClassifier(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "XGBRegressor":
			crear_prediccion_XGBRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "HistGradientBoostingRegressor":
			crear_prediccion_HistGradientBoostingRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "HistGradientBoostingClassifier":
			crear_prediccion_HistGradientBoostingRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "MLPClassifier":
			crear_prediccion_MLPClassifier(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "KNeighborsClassifier":
			crear_prediccion_KNeighborsClassifier(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "GaussianNB":
			crear_prediccion_GaussianNB(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "Arima":
			crear_prediccion_GaussianNB(posicion, fecha_sorteo, dataframe, prediccion_gl)
		#elif nombre_algoritmo == "SVR":
		#	crear_prediccion_SVR(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "DecisionTreeRegressor":
			crear_prediccion_DecisionTreeRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)
		elif nombre_algoritmo == "LinearRegression":
			crear_prediccion_DecisionTreeRegressor(posicion, fecha_sorteo, dataframe, prediccion_gl)

	print(mensaje)
	print("Fecha Minima: ", dataframe['fecha'][0])
	print("Fecha Maxima: ",dataframe['fecha'][dataframe['fecha'].count()-1])

	if mensaje not in ("Ley_del_Tercio","Frecuencia"):
		print(prediccion_gl)

	if mensaje in ("Ley_del_Tercio","Frecuencia"):
		prediccion_gl = formatear_prediccion(prediccion_gl)
		print(prediccion_gl)

	if guarda_prediccion:
		ejecutar_procedimiento(nombre_algoritmo
							, historico
							, fecha_sorteo
							, dataframe['fecha'][0]
							, dataframe['fecha'][dataframe['fecha'].count()-1]
							, mensaje
							, prediccion_gl[0]
							, prediccion_gl[1]
							, prediccion_gl[2]
							, prediccion_gl[3]
							, prediccion_gl[4]
							, prediccion_gl[5]
							, id_base)




def main():
	id_base = recuperar_id_base()
	loop_index = 1

	if id_base > 0:
		#imprimir los valores seteados para la ejecucion
		print(f"random_state: {RANDOM_STATE}")
		print(f"test_size: {TEST_SIZE}")
		print(f"Tamaño de la muestra {HISTORICO} sorteos")
		print(f"Sorteo Id: {id_base}")
		print("-------------------------------------")

		nombre_algoritmo="RandomForestRegressor"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="RandomForestClassifier"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="XGBRegressor"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="HistGradientBoostingRegressor"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="HistGradientBoostingClassifier"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="KNeighborsClassifier"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="GaussianNB"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="Arima"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="DecisionTreeRegressor"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1

		print("-------------------------------------")
		nombre_algoritmo="LinearRegression"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1
		"""
		print("-------------------------------------")
		nombre_algoritmo="SVR"
		print(nombre_algoritmo)
		#ejecucion de los 3 escenarios
		loop_index = 1
		imprime_fecha_sorteo = True
		for contenido in ARREGLO_CONFIG:
			if loop_index > 1: imprime_fecha_sorteo = False
			ejecutar_tarea(contenido[2], contenido[1], HISTORICO, id_base, GUARDA_PREDICCION, nombre_algoritmo, imprime_fecha_sorteo, contenido[3])
			loop_index += 1
		"""


		#Restaurar la configuración original después de ejecutar tu código si es necesario
		#Este codigo se ejecuta en la funcion main_pos1_pos6
		#warnings.resetwarnings()
	else:
		print("El ultimo sorteo ya tiene actualizados los resultados.")




if __name__ == "__main__":
	main()
