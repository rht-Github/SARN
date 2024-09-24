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
			query_stmt = " SELECT MAX(DRAWING_ID) MAX_ID"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE WINNER_FLAG IS NULL"

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
			query_stmt = query_stmt + " , CASE WHEN PRONOS_CICLO IS NOT NULL THEN 1 ELSE 0 END PXC"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 1 AND INPAR_NUMBER_FLAG IN (0,1) THEN 1 ELSE 0 END PRIMO"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 1 THEN 1 ELSE 0 END IMPAR"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 0 THEN 1 ELSE 0 END PAR"
			query_stmt = query_stmt + " , CASE WHEN CHNG_POSICION IS NULL THEN 0 ELSE 1 END CHNG"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE 1=1"
			query_stmt = query_stmt + " AND WINNER_FLAG IS NOT NULL"
			#query_stmt = query_stmt + " AND DRAWING_ID < " + str(sorteo_id)
			#query_stmt = query_stmt + " AND DRAWING_ID BETWEEN 654 AND " + str(sorteo_id)
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


#prediccion ley del tercio
def prediccion_lt(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion frecuencia
def prediccion_fr(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'IMPAR']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros primos
def prediccion_primo(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros impares
def prediccion_impar(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise

#prediccion de numeros pares
def prediccion_par(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise

#prediccion de numeros con cambio
def prediccion_change(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros
def prediccion_digit(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG']
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
			precision_score = accuracy_score(y_test, log_reg_pred)

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
			precision_score = accuracy_score(y_test, rf_clf_pred)

			# print("Precisión de Logistic Regression:", log_reg_accuracy)
			# print("Precisión de Random Forest:", rf_clf_accuracy)

			# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)

			# Realizar predicciones con los modelos entrenados
			valor_prediccion = rf_clf.predict(nuevo_registro)


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
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
	print("Filas con NaN:")
	print(nans)

	# Mostrar un resumen de la cantidad de valores NaN por columna
	nan_count = df.isnull().sum()
	#print("Cantidad de NaN por columna:")
	#print(nan_count)

	# Verificar si hay algún valor NaN en el dataframe
	if nan_count.any():
		return 1
	else:
		return 0


#ejecutar procedimiento de base de datos para guardar la info de las predicciones
def ejecutar_procedimiento(prediccion_info):

	try:
		# conectando a la base de datos
		str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
		conn = cx_Oracle.connect(str_conn)
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			cursor = conn.cursor()

			# Llamar al procedimiento almacenado con dos parámetros de tipo cadena
			cursor.callproc('W_GL_AUTOMATICAS_PKG.PREDICCIONES_ALL_HANDLER', [prediccion_info["nombre_algoritmo"],
																			  prediccion_info["prediccion_sorteo"],
																			  prediccion_info["prediccion_tipo"],
																			  prediccion_info["siguiente_sorteo_1"],
																			  prediccion_info["prediccion_1"],
																			  prediccion_info["precision_1"],
																			  prediccion_info["siguiente_sorteo_2"],
																			  prediccion_info["prediccion_2"],
																			  prediccion_info["precision_2"],
																			  prediccion_info["siguiente_sorteo_3"],
																			  prediccion_info["prediccion_3"],
																			  prediccion_info["precision_3"],
																			  prediccion_info["siguiente_sorteo_4"],
																			  prediccion_info["prediccion_4"],
																			  prediccion_info["precision_4"],
																			  prediccion_info["siguiente_sorteo_5"],
																			  prediccion_info["prediccion_5"],
																			  prediccion_info["precision_5"],
																			  prediccion_info["siguiente_sorteo_6"],
																			  prediccion_info["prediccion_6"],
																			  prediccion_info["precision_6"]])
		except Exception as err:
			print('Exception raised while executing the procedure', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones(df, nombre_algoritmo:str, label:str, id_base:int):

	# arreglo para almacenar el valor de las predicciones por cada b_type
	prediccion_gl = {
		"nombre_algoritmo": None,
		"prediccion_tipo": None,
		"prediccion_sorteo": 0,
		"siguiente_sorteo_1": 0,
		"prediccion_1": 0,
		"precision_1": 0.0,
		"siguiente_sorteo_2": 0,
		"prediccion_2": 0,
		"precision_2": 0.0,
		"siguiente_sorteo_3": 0,
		"prediccion_3": 0,
		"precision_3": 0.0,
		"siguiente_sorteo_4": 0,
		"prediccion_4": 0,
		"precision_4": 0.0,
		"siguiente_sorteo_5": 0,
		"prediccion_5": 0,
		"precision_5": 0.0,
		"siguiente_sorteo_6": 0,
		"prediccion_6": 0,
		"precision_6": 0.0
	}

	for b_type_id in range(1,7):
		#formacion dinamica del valor de la columna b_type
		b_type = "B" + str(b_type_id)

		#filtrado de la info del dataset en base a valor dinamico de b_type
		df_b_type = df[df["B_TYPE"]==b_type]
		#print(f"{b_type}.counr: {df_b_type.count()}")

		# prediccion relacionadas al ley del tercio
		if label == "LT":
			prediccion_lt(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "FR":
			prediccion_fr(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'PRIMO':
			prediccion_primo(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'IMPAR':
			prediccion_impar(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'PAR':
			prediccion_par(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'CHNG':
			prediccion_change(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'DIGIT':
			prediccion_digit(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		ejecutar_procedimiento(prediccion_gl)



def procesa_tarea():
	#recupera el maximo ID del sorteo que se va a jugar
	id_base = qry_id_base()
	#id_base = 1000

	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		#ley del tercio
		label = "LT"

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#ley del tercio
		label = "FR"

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#numeros primos
		label = 'PRIMO'

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#numeros impares
		label = 'IMPAR'

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#numeros pares
		label = 'PAR'

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#numeros con cambio
		label = 'CHNG'

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		#digits
		label = 'DIGIT'

		print("-------------------------------------")
		nombre_algoritmo = "lt_log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "lt_rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)
	else:
		print("Hay valores NaN en el dataset")
		raise


#funcion principal
def main():
	# proceso encargado de ejecutar todos los modelos de prediccion
	procesa_tarea()


if __name__ == "__main__":
	main()