import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.utils.class_weight import compute_class_weight
import tensorflow as tf
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import precision_score
import gl_dataset_handler as dh
import gl_utils_handler as uh
import warnings

GUARDA_PREDICCION=True

# Suprimir todas las advertencias (no recomendado a menos que sepas lo que estás haciendo)
warnings.filterwarnings("ignore")


#funcion generica que ejecuta el modelo rf y log_reg
def prediccion(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=5):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns =  ['J_CNT', 'J_CNT_FLAG', 'R_CNT', 'R_CNT_FLAG', 'SORTEO_ID', 'DIF', 'DIF_FLAG', 'DIGIT_TYPE', 'COLOR_FR', 'COLOR_LT', 'CHNG']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        n_samples = X.shape[0]
        if n_samples < n_splits:
            print(f"Reduciendo n_splits a {n_samples} debido a un número insuficiente de muestras.")
            n_splits = n_samples

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []
        models = []

        for train_index, val_index in kf.split(X):
            #X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            #y_train, y_val = y.iloc[train_index], y.iloc[val_index]
            X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

            if nombre_algoritmo == "log_reg":
                model = LogisticRegression(max_iter=300, random_state=42)
            elif nombre_algoritmo == "rf":
                model = RandomForestClassifier(n_estimators=100, random_state=42)
            else:
                raise ValueError("Algoritmo no soportado")

            model.fit(X_train, y_train)
            y_pred = model.predict(X_val)
            accuracies.append(accuracy_score(y_val, y_pred))
            models.append(model)

        # Calcular promedio de precisión
        average_accuracy = np.mean(accuracies)

        # Predecir siguiente experimento usando el último modelo
        next_experiment_data = [df[feature_columns].mean().values]  # Corregido: feature_columns
        valor_prediccion = models[-1].predict(next_experiment_data)

        # Actualizar diccionario de predicciones
        prediccion_dic.update({
            f'prediccion_{posicion}': str(valor_prediccion[0]),
            f'precision_{posicion}': float(round(average_accuracy, 3)),
            f'siguiente_sorteo_{posicion}': int(siguiente_sorteo),
            'nombre_algoritmo': nombre_algoritmo,
			'prediccion_sorteo': int(sorteo_id),
            'prediccion_tipo': f"13.{label}"
        })

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# funcion generica que ejecuta el modelo rf y log_reg
def prediccion_bayes(df, label, sorteo_id, prediccion_dic, nombre_algoritmo, n_splits=5):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Selección de características y etiqueta
		features = ['J_CNT', 'J_CNT_FLAG', 'R_CNT', 'R_CNT_FLAG', 'SORTEO_ID', 'DIF', 'DIF_FLAG', 'DIGIT_TYPE',
					'COLOR_FR', 'COLOR_LT', 'CHNG']
		label = 'POS'

		# Separar en conjuntos de entrenamiento y prueba
		X = df[features]
		y = df[label]
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Entrenamiento del modelo Naive Bayes
		nb_model = GaussianNB()
		nb_model.fit(X_train, y_train)

		# Predicción y cálculo de precisión
		y_pred = nb_model.predict(X_test)
		precision = precision_score(y_test, y_pred, average='weighted')

		# Agrupar por B_TYPE para realizar predicciones separadas
		#predictions = []

		for b_type, group in df.groupby('B_TYPE'):
			# Predecir probabilidades para el grupo actual
			proba = nb_model.predict_proba(group[features])
			top_indices = np.argsort(proba.max(axis=1))[-3:][::-1]  # Seleccionar las 3 más altas

			for idx in top_indices:
				pos = group.iloc[idx]['POS']
				max_proba = proba[idx].max()

				prediccion_dic.append([
					'bayes',  # PREDICCION_NOMBRE
					sorteo_id, # PREDICCION_SORTEO
					f'14.{b_type}', #PREDICCION_TIPO
					pos,  # PRED1
					siguiente_sorteo,  # ID del siguiente experimento
					round(max_proba, 2),  # PROBABILITY
				])

		for row in prediccion_dic:
			print(row)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise



#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones(df, nombre_algoritmo:str, label:str, id_base:int):

	# arreglo para almacenar el valor de las predicciones por cada b_type
	prediccion_gl = {
		"nombre_algoritmo": None,
		"prediccion_tipo": None,
		"prediccion_sorteo": 0,
		"siguiente_sorteo_1": 0,
		"prediccion_1": "0",
		"precision_1": 0.0,
		"siguiente_sorteo_2": 0,
		"prediccion_2": "0",
		"precision_2": 0.0,
		"siguiente_sorteo_3": 0,
		"prediccion_3": "0",
		"precision_3": 0.0,
		"siguiente_sorteo_4": 0,
		"prediccion_4": "0",
		"precision_4": 0.0,
		"siguiente_sorteo_5": 0,
		"prediccion_5": "0",
		"precision_5": 0.0,
		"siguiente_sorteo_6": 0,
		"prediccion_6": "0",
		"precision_6": 0.0
	}

	label == "POS"

	for b_type_id in range(1,7):
		#formacion dinamica del valor de la columna b_type
		b_type = "B" + str(b_type_id)

		#filtrado de la info del dataset en base a valor dinamico de b_type
		df_b_type = df[df["B_TYPE"]==b_type]
		#print(f"{b_type}.counr: {df_b_type.count()}")

		# prediccion relacionadas al ley del tercio
		prediccion(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)


		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones_bayes(df, nombre_algoritmo:str, label:str, id_base:int):

	# arreglo para almacenar el valor de las predicciones por cada b_type
	prediccion_gl = []

	label == "POS"

	# prediccion relacionadas al ley del tercio
	prediccion_bayes(df, label, id_base, prediccion_gl, nombre_algoritmo)


	#ya que estan completas las predicciones de la jugada se imprimen


	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento_array(prediccion_gl)



def procesa_subtarea(id_base, df):
	#ley del tercio
	label = "POS"
	print("-------------------------------------")
	nombre_algoritmo = "bayes"
	procesa_predicciones_bayes(df, nombre_algoritmo, label, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
def procesa_tarea(id_base):
	#formar el dataframe con la info del histiroc
	df= dh.create_dataframe(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = uh.check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		# proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
		procesa_subtarea(id_base, df)
	else:
		print("Hay valores NaN en el dataset")
		raise


#funcion principal
def main():
	#recupera el maximo ID del sorteo que se va a jugar
	id_base = dh.qry_id_base()

	# proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	procesa_tarea(id_base)


if __name__ == "__main__":
	main()