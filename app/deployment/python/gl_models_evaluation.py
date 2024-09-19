import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import warnings

#valores contantes
DB_USER = 'olap_sys'
DB_PWD = 'Ingenier1a'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'lcl'
GUARDA_PREDICCION=False

# Suprimir todas las advertencias (no recomendado a menos que sepas lo que estás haciendo)
warnings.filterwarnings("ignore")

#recupera el maximo ID del sorteo que se va a jugar
def qry_id_base ():
	try:
		# formando el string de conexion
		str_conn = DB_USER + "/" + DB_PWD + "@//" +DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
		#print(str_conn)
		# conectando a la base de datos
		conn = cx_Oracle.connect(str_conn)
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
			print(f"id_base: {id_base}")
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


#crear el dataframe con la info del historico de los sorteos
def create_gl_dataframe (sorteo_id:int):
	try:
		#print(f"create_gl_dataframe {sorteo_id}")
		#formando el string de conexion
		str_conn = DB_USER + "/" + DB_PWD + "@//" +DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
		# conectando a la base de datos
		conn = cx_Oracle.connect(str_conn)
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			#construyendo el query
			query_stmt = "WITH GIGA_TBL AS ("
			query_stmt = query_stmt + " SELECT DRAWING_ID ID"
			query_stmt = query_stmt + " , B_TYPE"
			query_stmt = query_stmt + " , DIGIT"
			query_stmt = query_stmt + " , COLOR_LEY_TERCIO LT"
			query_stmt = query_stmt + " , COLOR_UBICACION FR"
			query_stmt = query_stmt + " , NVL(CICLO_APARICION,3) CA"
			query_stmt = query_stmt + " , NVL(PRONOS_CICLO,0) PXC"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 1 AND INPAR_NUMBER_FLAG IN (0,1) THEN 1 ELSE 0 END PRIMO"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 1 THEN 1 ELSE 0 END IMPAR"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 0 THEN 1 ELSE 0 END PAR"
			query_stmt = query_stmt + " , CASE WHEN CHNG_POSICION IS NULL THEN 0 ELSE 1 END CHNG"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE 1=1"
			query_stmt = query_stmt + " AND WINNER_FLAG IS NOT NULL"
			#query_stmt = query_stmt + " AND DRAWING_ID < " + str(sorteo_id)
			query_stmt = query_stmt + " AND DRAWING_ID BETWEEN 654 AND " + str(sorteo_id)
			#query_stmt = query_stmt + " AND B_TYPE = " + "'" + b_type + "'"
			query_stmt = query_stmt + " ORDER BY DRAWING_ID, B_TYPE)"
			query_stmt = query_stmt + " SELECT *"
			query_stmt = query_stmt + " FROM GIGA_TBL"

			cursor = conn.cursor()
			cursor.execute(query_stmt)
			# Convirtiendo el resultset en un DataFrame de Pandas
			columns = ['ID', 'B_TYPE', 'DIGIT', 'LT', 'FR', 'CA', 'PXC', 'PRIMO','IMPAR','PAR','CHNG']  # Nombres de columnas
			df = pd.DataFrame(cursor.fetchall(), columns=columns)
			#print(df.head(10))
			return df
		except Exception as err:
			print('Exception raised while executing the query', err)
	finally:
		# Cerrar la conexion
		conn.close()


#prediccion Logistic Regression
def prediccion_lt(df, label, sorteo_id, posicion, arreglo_entrada, nombre_algoritmo, df_log):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Dividir los datos en conjuntos de entrenamiento y prueba
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		if nombre_algoritmo == "lt_log_reg":
			# Inicializar y entrenar Logistic Regression
			log_reg = LogisticRegression(max_iter=300, random_state=42)
			log_reg.fit(X_train, y_train)

			# Realizar predicciones en el conjunto de prueba
			log_reg_pred = log_reg.predict(X_test)

			# Calcular la precisión de cada modelo
			log_reg_accuracy = accuracy_score(y_test, log_reg_pred)

			#print("Precisión de Logistic Regression:", log_reg_accuracy)
			#print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = log_reg.predict(nuevo_registro)
		elif nombre_algoritmo == "lt_rf":
			# Inicializar y entrenar Random Forest Classifier
			rf_clf = RandomForestClassifier(random_state=42)
			rf_clf.fit(X_train, y_train)

			# Realizar predicciones en el conjunto de prueba
			rf_clf_pred = rf_clf.predict(X_test)

			# Calcular la precisión de cada modelo
			rf_clf_accuracy = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"{siguiente_sorteo}, {valor_prediccion}, {nombre_algoritmo}, ")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		if nombre_algoritmo == "lt_log_reg":
			if posicion == 1:
				df_update_row(df_log, sorteo_id, 'B1', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))
			if posicion == 2:
				df_update_row(df_log, sorteo_id, 'B2', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))
			if posicion == 3:
				df_update_row(df_log, sorteo_id, 'B3', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))
			if posicion == 4:
				df_update_row(df_log, sorteo_id, 'B4', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))
			if posicion == 5:
				df_update_row(df_log, sorteo_id, 'B5', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))
			if posicion == 6:
				df_update_row(df_log, sorteo_id, 'B6', 'LOG_REG_VALOR_PRED', round(valor_prediccion[0]))

		return df_log;






		"""
		arreglo_salida = arreglo_entrada
		if posicion == 1:
			arreglo_salida[0] = round(valor_prediccion[0])
		if posicion == 2:
			arreglo_salida[1] = round(valor_prediccion[0])
		if posicion == 3:
			arreglo_salida[2] = round(valor_prediccion[0])
		if posicion == 4:
			arreglo_salida[3] = round(valor_prediccion[0])
		if posicion == 5:
			arreglo_salida[4] = round(valor_prediccion[0])
		if posicion == 6:
			arreglo_salida[5] = round(valor_prediccion[0])
		return arreglo_salida
		"""
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


# Chequear y visualizar NaNs en el DataFrame
def check_nans(df) -> int:
	# Mostrar todas las filas que contienen al menos un valor NaN
	nans = df[df.isnull().any(axis=1)]
	#print("Filas con NaN:")
	#print(nans)

	# Mostrar un resumen de la cantidad de valores NaN por columna
	nan_count = df.isnull().sum()
	#print("Cantidad de NaN por columna:")
	#print(nan_count)

	# Verificar si hay algún valor NaN en el dataframe
	if nan_count.any():
		return 1
	else:
		return 0


# Función para agregar o actualizar una fila en el DataFrame
def df_update_row(df_log, ID, B_TYPE, column, value):
	# Buscar la combinación de ID y B_TYPE en el DataFrame
	condition = (df_log['ID'] == ID) & (df_log['B_TYPE'] == B_TYPE)
	# Buscar la combinación de ID, B_TYPE y LT en el DataFrame
	condition_lt = (df_log['ID'] == ID) & (df_log['B_TYPE'] == B_TYPE) & (df_log['LT'] == value)

	# Si la combinación de ID y B_TYPE existe, actualizar la columna correspondiente
	if not df_log[condition].empty:
		df_log.loc[condition, column] = value
		if not df_log[condition_lt].empty:
			df_log.loc[condition_lt, 'LOG_REG_MATCH'] = 1
		else:
			df_log.loc[condition_lt, 'LOG_REG_MATCH'] = 0
		imprimir_todas_las_columnas(df_log, ID, B_TYPE, column, value)
		#print(f"df_update_row: id: {ID}, b_type: {B_TYPE}, lt: {valor_lt}, column: {column}, value: {value}")

	return df_log


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones(df, nombre_algoritmo:str, label:str, id_base:int, df_log):

	prediccion_gl = [-1, -2, -3, -4, -5, -6]

	for b_type_id in range(1,7):
		#formacion dinamica del valor de la columna b_type
		b_type = "B" + str(b_type_id)

		#filtrado de la info del dataset en base a valor dinamico de b_type
		df_b_type = df[df["B_TYPE"]==b_type]
		#print(f"{b_type}.counr: {df_b_type.count()}")

		# prediccion relacionadas al ley del tercio
		if label == "LT":
			prediccion_lt(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo, df_log)

			#ya que estan completas las predicciones de la jugada se imprimen
			#if b_type_id == 6:
			#	print(f"{id_base}, {prediccion_gl}, {nombre_algoritmo}")

	if GUARDA_PREDICCION:
		print("ejecutar_procedimiento")

	return df_log;


def imprimir_todas_las_columnas(df, ID, B_TYPE, column, value):
    # Iterar sobre cada fila y su índice en el DataFrame
	for index, row in df.iterrows():
		if row['ID'] == ID and row['B_TYPE'] == B_TYPE:
			print(f"Fila {index}:")
			for col in df.columns:
				print(f"  {col}: {row[col]}")
			print("-" * 40)  # Imprime una línea divisoria para mejor visualización entre filas


#proceso que ejecutar las tareas que manden llamar las funciones que realizaran las predicciones
def procesa_tarea():
	#recupera el maximo ID del sorteo que se va a jugar
	#id_base = qry_id_base()
	#id_base = 901

	match_cnt = 0
	for id_base in range(765,1450):
		#formar el dataframe con la info del historico
		df= create_gl_dataframe(id_base)

		#print(df.head(10))

		#copia del dataframe
		df_log = df.copy()

		#agregando nuevas columnas al dataframe log
		df_log['LOG_REG_VALOR_PRED'] = 0
		df_log['LOG_REG_PRECISION'] = 0
		df_log['LOG_REG_MATCH'] = 0

		# Chequear y visualizar NaNs en el DataFrame
		nan_count = check_nans(df)

		#si no hay valores nulos se procede a realizar la prediccion
		if nan_count == 0:
			#definimos el label que se va a predecir
			label = "LT"

			#print("-------------------------------------")
			nombre_algoritmo = "lt_log_reg"
			procesa_predicciones(df, nombre_algoritmo, label, id_base, df_log)

			#print("-------------------------------------")
			#nombre_algoritmo = "lt_rf"
			#procesa_predicciones(df, nombre_algoritmo, label, id_base)
		else:
			print("Hay valores NaN en el dataset")
			raise

		for index, row in df_log.iterrows():
			# Verifica si el ID actual es igual a id_base
			if row['ID'] == id_base:
				if row['LOG_REG_MATCH'] == 1:
					match_cnt += 1
				print(f"match_cnt: {match_cnt}, {row['ID']}, {id_base}")



#funcion principal
def main():
	# proceso encargado de ejecutar todos los modelos de prediccion
	procesa_tarea()


if __name__ == "__main__":
	main()


"""	
#entrenamiento de modelos
def entrena_modelos(df):
	try:
		# print(f"entrena_modelos {label}, {features}")
		print(df.count())
		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns]  # Features
		y = df['LT']  # Label

		# Dividir los datos en conjuntos de entrenamiento y prueba
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Inicializar y entrenar Logistic Regression
		log_reg = LogisticRegression(max_iter=300, random_state=42)
		log_reg.fit(X_train, y_train)

		# Inicializar y entrenar Random Forest Classifier
		rf_clf = RandomForestClassifier(random_state=42)
		rf_clf.fit(X_train, y_train)

		# Realizar predicciones en el conjunto de prueba
		log_reg_pred = log_reg.predict(X_test)
		rf_clf_pred = rf_clf.predict(X_test)

		# Calcular la precisión de cada modelo
		log_reg_accuracy = accuracy_score(y_test, log_reg_pred)
		rf_clf_accuracy = accuracy_score(y_test, rf_clf_pred)

		#print("Precisión de Logistic Regression:", log_reg_accuracy)
		#print("Precisión de Random Forest:", rf_clf_accuracy)

		# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
		nuevo_registro = X.iloc[-1].values.reshape(1, -1)

		# Realizar predicciones con los modelos entrenados
		prediccion_log_reg = log_reg.predict(nuevo_registro)
		prediccion_rf = rf_clf.predict(nuevo_registro)

		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones
		siguiente_sorteo = df['ID'].max()+1
		print(f"siguiente_sorteo: {siguiente_sorteo}, {prediccion_log_reg}, {prediccion_rf}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise
"""