import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
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
from sklearn.model_selection import GridSearchCV
import gl_dataset_handler as dh
import warnings

#valores contantes
DB_USER = 'olap_sys'
DB_PWD = 'Ingenier1a'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'lcl'
SORTEO_BASE = 896
EPOCHS = 13
N_SPLITS = 5

GUARDA_PREDICCION=True

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
def create_gl_dataframe (sorteo_id:int, sorteo_base:int=0):
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
			query_stmt = query_stmt + " , PRIME_NUMBER_FLAG PRIMO"
			query_stmt = query_stmt + " , INPAR_NUMBER_FLAG IMPAR"
			query_stmt = query_stmt + " , PAR_NUMBER_FLAG PAR"
			query_stmt = query_stmt + " , CASE WHEN CHNG_POSICION IS NULL THEN 0 ELSE 1 END CHNG"
			query_stmt = query_stmt + " , OLAP_SYS.W_COMMON_PKG.GET_DIGITO_TO_NUMERO(DIGIT) DECENA"
			if sorteo_base > 0:
				query_stmt = query_stmt + " , CASE WHEN PREFERENCIA_FLAG IS NULL THEN 0 ELSE 1 END PREF"
			query_stmt = query_stmt + " FROM OLAP_SYS.S_CALCULO_STATS"
			query_stmt = query_stmt + " WHERE 1=1"
			query_stmt = query_stmt + " AND WINNER_FLAG IS NOT NULL"
			#query_stmt = query_stmt + " AND DRAWING_ID < " + str(sorteo_id)
			#query_stmt = query_stmt + " AND DRAWING_ID BETWEEN 654 AND " + str(sorteo_id)
			#query_stmt = query_stmt + " AND B_TYPE = " + "'" + b_type + "'"
			if sorteo_base > 0:
				query_stmt = query_stmt + " AND DRAWING_ID >= " + str(sorteo_base)
			query_stmt = query_stmt + " ORDER BY DRAWING_ID, B_TYPE)"
			if sorteo_base > 0:
				query_stmt = query_stmt + " SELECT ID, B_TYPE, DIGIT, LT, FR, CA, PXC, PRIMO, IMPAR, PAR, CHNG, DECENA"
				query_stmt = query_stmt + " , CASE WHEN PXC = 0 AND PREF = 0 THEN 0"
				query_stmt = query_stmt + " WHEN PXC = 0 AND PREF = 1 THEN 1"
				query_stmt = query_stmt + " WHEN PXC = 1 AND PREF = 0 THEN 2"
				query_stmt = query_stmt + " WHEN PXC = 1 AND PREF = 1 THEN 3 END PXC_PREF"
			else:
				query_stmt = query_stmt + " SELECT ID, B_TYPE, DIGIT, LT, FR, CA, PXC, PRIMO, IMPAR, PAR, CHNG, DECENA"
			query_stmt = query_stmt + " FROM GIGA_TBL"

			cursor = conn.cursor()
			cursor.execute(query_stmt)
			# Convirtiendo el resultset en un DataFrame de Pandas
			if sorteo_base > 0:
				columns = ['ID', 'B_TYPE', 'DIGIT', 'LT', 'FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG','DECENA','PXC_PREF']  # Nombres de columnas
			else:
				columns = ['ID', 'B_TYPE', 'DIGIT', 'LT', 'FR', 'CA', 'PXC', 'PRIMO','IMPAR','PAR','CHNG','DECENA']  # Nombres de columnas
			df = pd.DataFrame(cursor.fetchall(), columns=columns)
			#print(df.head(10))
			return df
		except Exception as err:
			print('Exception raised while executing the query', err)
	finally:
		# Cerrar la conexion
		conn.close()


# Crear el dataframe con la info del histórico de los sorteos basado en primos, impares y pares
def create_gl_dataframe_pip(sorteo_id: int = 0):
    try:
        # Formando el string de conexión
        str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
        # Conectando a la base de datos
        conn = cx_Oracle.connect(str_conn)
    except Exception as err:
        print('Exception while creating a Oracle connection', err)
        return None
    else:
        try:
            # Construyendo el query
            query_stmt = """
            SELECT GAMBLING_ID ID, 
                   PN_CNT, 
                   NONE_CNT, 
                   PAR_CNT 
            FROM OLAP_SYS.PM_MR_RESULTADOS_V2
            WHERE 1=1
            ORDER BY ID
            """
            cursor = conn.cursor()
            cursor.execute(query_stmt)
            # Convirtiendo el resultset en un DataFrame de Pandas
            columns = ['ID', 'PN_CNT', 'NONE_CNT', 'PAR_CNT']  # Nombres de columnas
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            return df
        except Exception as err:
            print('Exception raised while executing the query', err)
            return None
        finally:
            cursor.close()  # Asegurarse de cerrar el cursor
    finally:
        conn.close()  # Cerrar la conexión


# Crear el dataframe con la info del histórico de los sorteos basado el conteo de terminaciones
def create_gl_dataframe_terminaciones(sorteo_id: int = 0):
    try:
        # Formando el string de conexión
        str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
        # Conectando a la base de datos
        conn = cx_Oracle.connect(str_conn)
    except Exception as err:
        print('Exception while creating a Oracle connection', err)
        return None
    else:
        try:
            # Construyendo el query
            query_stmt = """
            SELECT GAMBLING_ID ID
                 , T1
				 , T2
				 , T3
				 , T4
				 , T5
				 , T6
				 , T7
				 , T8
				 , T9
				 , T0
            FROM OLAP_SYS.PM_MR_RESULTADOS_V2
            WHERE 1=1
            ORDER BY ID
            """
            cursor = conn.cursor()
            cursor.execute(query_stmt)
            # Convirtiendo el resultset en un DataFrame de Pandas
            columns = ['ID', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0']  # Nombres de columnas
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            return df
        except Exception as err:
            print('Exception raised while executing the query', err)
            return None
        finally:
            cursor.close()  # Asegurarse de cerrar el cursor
    finally:
        conn.close()  # Cerrar la conexión

#Crear el dataframe con la info del histórico de los sorteos basado en una vista que solo muestra numeros
def create_gl_dataframe_generic(qry_type: int = 1):
    try:
        # Formando el string de conexión
        str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
        # Conectando a la base de datos
        conn = cx_Oracle.connect(str_conn)
    except Exception as err:
        print('Exception while creating a Oracle connection', err)
        return None
    else:
        try:
            # frecuencia
            if qry_type == 1:
                query_stmt = "SELECT GAMBLING_ID ID, CU1, CU2, CU3, CU4, CU5, CU6"

            # ley del tercio
            elif qry_type == 2:
                query_stmt = "SELECT GAMBLING_ID ID, CLT1, CLT2, CLT3, CLT4, CLT5, CLT6"

            query_stmt += " FROM OLAP_SYS.MR_RESULTADOS_NUM_V"
            query_stmt += " ORDER BY GAMBLING_ID"

            cursor = conn.cursor()
            cursor.execute(query_stmt)
            # Convirtiendo el resultset en un DataFrame de Pandas
            columns = ['ID', 'POS1', 'POS2', 'POS3', 'POS4', 'POS5', 'POS6']  # Nombres de columnas
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            return df
        except Exception as err:
            print('Exception raised while executing the query', err)
            return None
        finally:
            cursor.close()  # Asegurarse de cerrar el cursor
            conn.close()  # Cerrar la conexión



#prediccion ley del tercio
def prediccion_lt(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "2." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion ley del tercio
def prediccion_lt_2(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

		# Separate features and label
		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Preprocessing for scaling only 'FR' and 'CA'
		numeric_features = ['FR', 'CA', 'DIGIT']
		remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		categorical_features = ['B_TYPE']

		# Create preprocessing pipeline for scaling FR and CA, and encoding B_TYPE
		preprocessor = ColumnTransformer(
			transformers=[
				('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
				('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
				('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
			])

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		if nombre_algoritmo == "log_reg":
			# Compute class weights to handle imbalance
			class_weights = dict(zip(range(1, 4), compute_class_weight('balanced', classes=[1, 2, 3], y=y_train)))

			# Define Logistic Regression pipeline
			model = Pipeline(steps=[
				('preprocessor', preprocessor),
				('classifier', LogisticRegression(max_iter=300, random_state=42, class_weight=class_weights))
			])

			# Fit the model
			model.fit(X_train, y_train)

			# Predictions and accuracy
			y_pred = model.predict(X_test)
			precision_score = accuracy_score(y_test, y_pred)

			# New record prediction
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)
			valor_prediccion = model.predict(nuevo_registro)

		elif nombre_algoritmo == "rf":
			# Random Forest Classifier pipeline
			model = Pipeline(steps=[
				('preprocessor', preprocessor),
				('classifier', RandomForestClassifier(random_state=42))
			])

			# Fit the model
			model.fit(X_train, y_train)

			# Predictions and accuracy
			y_pred = model.predict(X_test)
			precision_score = accuracy_score(y_test, y_pred)

			# New record prediction
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)
			valor_prediccion = model.predict(nuevo_registro)

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "2." + label+"_2"
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

#prediccion ley del tercio en base a tensorflow
def prediccion_lt_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

		# Separate features and label
		X = df[feature_columns]  # Features
		y = df[label]  # Label (Assumed to have values 1, 2, 3)

		# Preprocessing for scaling only 'FR' and 'CA'
		numeric_features = ['FR', 'CA', 'DIGIT']
		remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		categorical_features = ['B_TYPE']

		# Create preprocessing pipeline for scaling numeric features and one-hot encoding categorical features
		preprocessor = ColumnTransformer(
			transformers=[
				('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
				('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
				('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
			])

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Convert labels to range starting from 0 (TensorFlow expects classes starting from 0)
		y_train -= 1
		y_test -= 1

		# Build a neural network model with TensorFlow/Keras for multi-class classification
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(3, activation='softmax')  # Softmax for multi-class classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Predict on test set
		y_pred_prob = model.predict(X_test)
		y_pred = y_pred_prob.argmax(axis=1)  # Get class with highest probability

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the current instance (e.g., latest record)
		X_latest = X_test[-1:]  # Assuming you want the prediction for the last test instance
		valor_prediccion_prob = model.predict(X_latest)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[
								0] + 1  # Add 1 to match original class labels (1, 2, 3)

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "2." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_lt_tf: {e}")
	return None


#prediccion ley del tercio en base a torch
def prediccion_lt_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # Define feature columns
        feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

        # Separate features and label
        X = df[feature_columns]  # Features
        y = df[label]  # Label (Assumed to have values 1, 2, 3)

        # Preprocessing for scaling only 'FR' and 'CA' and one-hot encoding 'B_TYPE'
        numeric_features = ['FR', 'CA', 'DIGIT']
        remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
        categorical_features = ['B_TYPE']

        # Create preprocessing pipeline for scaling numeric features and one-hot encoding categorical features
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
                ('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
                ('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
            ])

        # Split data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Apply preprocessing
        X_train = preprocessor.fit_transform(X_train)
        X_test = preprocessor.transform(X_test)

        # Convert labels to range starting from 0 (PyTorch expects classes starting from 0)
        y_train = y_train - 1
        y_test = y_test - 1

        # Convert data to PyTorch tensors
        X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
        y_train_tensor = torch.tensor(y_train.values, dtype=torch.long)
        X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
        y_test_tensor = torch.tensor(y_test.values, dtype=torch.long)

        # Create DataLoader for training data
        train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
        train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

        # Define the neural network model
        class NeuralNet(nn.Module):
            def __init__(self, input_dim, output_dim):
                super(NeuralNet, self).__init__()
                self.fc1 = nn.Linear(input_dim, 64)
                self.fc2 = nn.Linear(64, 32)
                self.fc3 = nn.Linear(32, output_dim)

            def forward(self, x):
                x = torch.relu(self.fc1(x))
                x = torch.relu(self.fc2(x))
                x = torch.softmax(self.fc3(x), dim=1)
                return x

        # Instantiate the model, loss function, and optimizer
        model = NeuralNet(X_train.shape[1], 3)  # 3 classes for output (1, 2, 3)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)

        # Training loop
        model.train()
        for epoch in range(epochs):
            for batch_X, batch_y in train_loader:
                # Forward pass
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)

                # Backward pass and optimization
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            #print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

        # Evaluate the model on test data
        model.eval()
        with torch.no_grad():
            y_pred_prob = model(X_test_tensor)
            y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

        # Calculate accuracy
        precision_score = accuracy_score(y_test, y_pred)
        #print(f"Model Accuracy: {precision_score * 100:.2f}%")

        # Single prediction for the latest record in the dataset
        X_latest = X_test_tensor[-1].unsqueeze(0)  # Assuming you want the prediction for the last test instance
        with torch.no_grad():
            valor_prediccion_prob = model(X_latest)
            valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1  # Add 1 to match original labels (1, 2, 3)

        #print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

        # Update prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "2." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion)
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion)
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion)
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt_torch: {e}")
        return None


#prediccion frecuencia
def prediccion_fr(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Validación cruzada
		kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
		accuracies = []

		for train_index, val_index in kf.split(X):
			X_train, X_val = X.iloc[train_index], X.iloc[val_index]
			y_train, y_val = y.iloc[train_index], y.iloc[val_index]

			if nombre_algoritmo == "log_reg":
				# Inicializar y entrenar Logistic Regression
				log_reg = LogisticRegression(max_iter=300, random_state=42)
				log_reg.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				log_reg_pred = log_reg.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, log_reg_pred)
				accuracies.append(precision_score)

				# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
				nuevo_registro = X.iloc[-1].values.reshape(1, -1)
				valor_prediccion = log_reg.predict(nuevo_registro)

			elif nombre_algoritmo == "rf":
				# Inicializar y entrenar Random Forest Classifier
				rf_clf = RandomForestClassifier(random_state=42)
				rf_clf.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				rf_clf_pred = rf_clf.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, rf_clf_pred)
				accuracies.append(precision_score)

				# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
				nuevo_registro = X.iloc[-1].values.reshape(1, -1)
				valor_prediccion = rf_clf.predict(nuevo_registro)

		# Promedio de precisión entre pliegues
		average_accuracy = np.mean(accuracies)

		# Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "1." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion frecuencia
def prediccion_fr_2(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

		# Separate features and label
		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Preprocessing for scaling only 'LT' and 'CA'
		numeric_features = ['LT', 'CA', 'DIGIT']
		remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		categorical_features = ['B_TYPE']

		# Create preprocessing pipeline for scaling FR and CA, and encoding B_TYPE
		preprocessor = ColumnTransformer(
			transformers=[
				('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
				('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
				('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
			])

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		if nombre_algoritmo == "log_reg":
			# Compute class weights to handle imbalance
			class_weights = dict(zip(range(1, 4), compute_class_weight('balanced', classes=[1, 2, 3], y=y_train)))

			# Define Logistic Regression pipeline
			model = Pipeline(steps=[
				('preprocessor', preprocessor),
				('classifier', LogisticRegression(max_iter=300, random_state=42, class_weight=class_weights))
			])

			# Fit the model
			model.fit(X_train, y_train)

			# Predictions and accuracy
			y_pred = model.predict(X_test)
			precision_score = accuracy_score(y_test, y_pred)

			# New record prediction
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)
			valor_prediccion = model.predict(nuevo_registro)

		elif nombre_algoritmo == "rf":
			# Random Forest Classifier pipeline
			model = Pipeline(steps=[
				('preprocessor', preprocessor),
				('classifier', RandomForestClassifier(random_state=42))
			])

			# Fit the model
			model.fit(X_train, y_train)

			# Predictions and accuracy
			y_pred = model.predict(X_test)
			precision_score = accuracy_score(y_test, y_pred)

			# New record prediction
			nuevo_registro = X.iloc[-1].values.reshape(1, -1)
			valor_prediccion = model.predict(nuevo_registro)

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "1." + label + "_2"
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
def prediccion_fr_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

		# Separate features and label
		X = df[feature_columns]  # Features
		y = df[label]  # Label (Assumed to have values 1, 2, 3)

		# Preprocessing for scaling only 'LT' and 'CA'
		numeric_features = ['LT', 'CA', 'DIGIT']
		remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		categorical_features = ['B_TYPE']

		# Create preprocessing pipeline for scaling numeric features and one-hot encoding categorical features
		preprocessor = ColumnTransformer(
			transformers=[
				('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
				('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
				('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
			])

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Convert labels to range starting from 0 (TensorFlow expects classes starting from 0)
		y_train -= 1
		y_test -= 1

		# Build a neural network model with TensorFlow/Keras for multi-class classification
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(3, activation='softmax')  # Softmax for multi-class classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Predict on test set
		y_pred_prob = model.predict(X_test)
		y_pred = y_pred_prob.argmax(axis=1)  # Get class with highest probability

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the current instance (e.g., latest record)
		X_latest = X_test[-1:]  # Assuming you want the prediction for the last test instance
		valor_prediccion_prob = model.predict(X_latest)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[
								0] + 1  # Add 1 to match original class labels (1, 2, 3)

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "1." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_fr_tf: {e}")
	return None


#prediccion de frecuencia en base a torch
def prediccion_fr_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT','DECENA']

		# Separate features and label
		X = df[feature_columns]  # Features
		y = df[label]  # Label (Assumed to have values 1, 2, 3)

		# Preprocessing for scaling only 'FR' and 'CA' and one-hot encoding 'B_TYPE'
		numeric_features = ['LT', 'CA', 'DIGIT']
		remaining_features = ['PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		categorical_features = ['B_TYPE']

		# Create preprocessing pipeline for scaling numeric features and one-hot encoding categorical features
		preprocessor = ColumnTransformer(
			transformers=[
				('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features]),
				('remain', 'passthrough', [feature_columns.index(f) for f in remaining_features]),
				('cat', OneHotEncoder(), [feature_columns.index(f) for f in categorical_features])
			])

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Convert labels to range starting from 0 (PyTorch expects classes starting from 0)
		y_train = y_train - 1
		y_test = y_test - 1

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train.values, dtype=torch.long)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test.values, dtype=torch.long)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model
		class NeuralNet(nn.Module):
			def __init__(self, input_dim, output_dim):
				super(NeuralNet, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, output_dim)

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				x = torch.softmax(self.fc3(x), dim=1)
				return x

		# Instantiate the model, loss function, and optimizer
		model = NeuralNet(X_train.shape[1], 3)  # 3 classes for output (1, 2, 3)
		criterion = nn.CrossEntropyLoss()
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

				# Backward pass and optimization
				optimizer.zero_grad()
				loss.backward()
				optimizer.step()

			#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on test data
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		X_latest = X_test_tensor[-1].unsqueeze(0)  # Assuming you want the prediction for the last test instance
		with torch.no_grad():
			valor_prediccion_prob = model(X_latest)
			valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1  # Add 1 to match original labels (1, 2, 3)

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "1." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_fr_torch: {e}")
		return None


#prediccion de numeros primos
def prediccion_primo(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns =['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "3." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de numeros primos en base a tensorflow
def prediccion_primo_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0 or 1)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Build a binary classification neural network using TensorFlow/Keras
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(1, activation='sigmoid')  # Sigmoid for binary classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Evaluate on the test set
		test_loss, precision_score = model.evaluate(X_test, y_test, verbose=0)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset (similar to original logic)
		nuevo_registro = X_test[-1:]  # Last row for prediction
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = (valor_prediccion_prob > 0.5).astype(int)[0][0]  # Convert to binary 0 or 1


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "3." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise

#prediccion de numeros primos en base a torch
def prediccion_primo_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1,1)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32).view(-1,1)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model for binary classification
		class BinaryClassificationModel(nn.Module):
			def __init__(self, input_dim):
				super(BinaryClassificationModel, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, 1)  # Single output neuron for binary classification

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return torch.sigmoid(self.fc3(x))  # Sigmoid activation for binary classification

		model = BinaryClassificationModel(X_train.shape[1])

		# Loss and optimizer
		criterion = nn.BCELoss()  # Binary Cross Entropy Loss
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				batch_y = batch_y.view(-1,1)  # Ensure 1D tensor
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy().squeeze())
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion_prob = model(nuevo_registro).item()  # Get the scalar value
			valor_prediccion = int(valor_prediccion_prob > 0.5)  # Convert to binary 0 or 1

		# Update the prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "3." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros impares
def prediccion_impar(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns =['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "5." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de numeros impares en base a tensorflow
def prediccion_impar_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0 or 1)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Build a binary classification neural network using TensorFlow/Keras
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(1, activation='sigmoid')  # Sigmoid for binary classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Evaluate on the test set
		test_loss, precision_score = model.evaluate(X_test, y_test, verbose=0)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset (similar to original logic)
		nuevo_registro = X_test[-1:]  # Last row for prediction
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = (valor_prediccion_prob > 0.5).astype(int)[0][0]  # Convert to binary 0 or 1


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "5." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros impares en base a torch
def prediccion_impar_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32).view(-1, 1)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model for binary classification
		class BinaryClassificationModel(nn.Module):
			def __init__(self, input_dim):
				super(BinaryClassificationModel, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, 1)  # Single output neuron for binary classification

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return torch.sigmoid(self.fc3(x))  # Sigmoid activation for binary classification

		model = BinaryClassificationModel(X_train.shape[1])

		# Loss and optimizer
		criterion = nn.BCELoss()  # Binary Cross Entropy Loss
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				batch_y = batch_y.view(-1, 1)
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy().squeeze())
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion_prob = model(nuevo_registro).item()  # Get the scalar value
			valor_prediccion = int(valor_prediccion_prob > 0.5)  # Convert to binary 0 or 1

		# Update the prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "5." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros pares
def prediccion_par(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "4." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de numeros pares en base a tensorflow
def prediccion_par_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0 or 1)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Build a binary classification neural network using TensorFlow/Keras
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(1, activation='sigmoid')  # Sigmoid for binary classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Evaluate on the test set
		test_loss, precision_score = model.evaluate(X_test, y_test, verbose=0)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset (similar to original logic)
		nuevo_registro = X_test[-1:]  # Last row for prediction
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = (valor_prediccion_prob > 0.5).astype(int)[0][0]  # Convert to binary 0 or 1


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "4." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros pares en base a torch
def prediccion_par_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG','DECENA']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32).view(-1, 1)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model for binary classification
		class BinaryClassificationModel(nn.Module):
			def __init__(self, input_dim):
				super(BinaryClassificationModel, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, 1)  # Single output neuron for binary classification

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return torch.sigmoid(self.fc3(x))  # Sigmoid activation for binary classification

		model = BinaryClassificationModel(X_train.shape[1])

		# Loss and optimizer
		criterion = nn.BCELoss()  # Binary Cross Entropy Loss
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				batch_y = batch_y.view(-1, 1)
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy().squeeze())
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion_prob = model(nuevo_registro).item()  # Get the scalar value
			valor_prediccion = int(valor_prediccion_prob > 0.5)  # Convert to binary 0 or 1

		# Update the prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "4." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros con cambio
def prediccion_change(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "6." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de numeros con cambio en base a tensorflow
def prediccion_change_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0 or 1)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Build a binary classification neural network using TensorFlow/Keras
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(1, activation='sigmoid')  # Sigmoid for binary classification
		])

		# Compile the model
		model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Evaluate on the test set
		test_loss, precision_score = model.evaluate(X_test, y_test, verbose=0)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset (similar to original logic)
		nuevo_registro = X_test[-1:]  # Last row for prediction
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = (valor_prediccion_prob > 0.5).astype(int)[0][0]  # Convert to binary 0 or 1


		# Ajustar las etiquetas de vuelta al rango original y mostrar las predicciones

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "6." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic
	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros con cambio en base a torch
def prediccion_change_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','DECENA']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32).view(-1, 1)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model for binary classification
		class BinaryClassificationModel(nn.Module):
			def __init__(self, input_dim):
				super(BinaryClassificationModel, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, 1)  # Single output neuron for binary classification

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return torch.sigmoid(self.fc3(x))  # Sigmoid activation for binary classification

		model = BinaryClassificationModel(X_train.shape[1])

		# Loss and optimizer
		criterion = nn.BCELoss()  # Binary Cross Entropy Loss
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				batch_y = batch_y.view(-1, 1)
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy().squeeze())
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Predict for the latest record in the dataset
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion_prob = model(nuevo_registro).item()  # Get the scalar value
			valor_prediccion = int(valor_prediccion_prob > 0.5)  # Convert to binary 0 or 1

		# Update the prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "6." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de digitos
def prediccion_digit(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "7." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de digitos en base a tensorflow
def prediccion_digit_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values range from 1 to 39)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
		transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Subtract 1 from the target labels so they start at 0 (as required by TensorFlow)
		y_train -= 1
		y_test -= 1

		# Build a neural network model using TensorFlow/Keras for multi-class classification
		model = tf.keras.Sequential([
		tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
		tf.keras.layers.Dense(128, activation='relu'),
		tf.keras.layers.Dense(64, activation='relu'),
		tf.keras.layers.Dense(39, activation='softmax')  # Softmax for 39 classes (1 to 39)
		])

		# Compile the model
		model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Predict on test set to evaluate the model
		y_pred_prob = model.predict(X_test)
		y_pred = y_pred_prob.argmax(axis=1)  # Get class with highest probability

		# Evaluate accuracy on test data
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		nuevo_registro = X_test[-1:]  # Last row for prediction
		single_prediction_prob = model.predict(nuevo_registro)
		valor_prediccion = single_prediction_prob.argmax(axis=1)[0] + 1  # Add 1 to match original range (1 to 39)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, valor_prediccion: {valor_prediccion}")

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "7." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
		prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de digitos en base a torch
def prediccion_digit_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values range from 1 to 39)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Subtract 1 from the target labels so they start at 0 (as required by PyTorch)
		y_train = y_train - 1
		y_test = y_test - 1

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train.values, dtype=torch.long).squeeze()
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test.values, dtype=torch.long).squeeze()

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model
		class NeuralNet(nn.Module):
			def __init__(self, input_dim, output_dim):
				super(NeuralNet, self).__init__()
				self.fc1 = nn.Linear(input_dim, 128)
				self.fc2 = nn.Linear(128, 64)
				self.fc3 = nn.Linear(64, output_dim)

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				x = torch.softmax(self.fc3(x), dim=1)
				return x

		# Instantiate the model, loss function, and optimizer
		model = NeuralNet(X_train.shape[1], 39)  # 39 classes for output
		criterion = nn.CrossEntropyLoss()
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		epochs = EPOCHS
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

				# Backward pass and optimization
				optimizer.zero_grad()
				loss.backward()
				optimizer.step()

			#print(f"Epoch [{epoch + 1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on test data
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

		# Compute accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		nuevo_registro = X_test_tensor[-1].unsqueeze(0)  # Last row for prediction
		with torch.no_grad():
			single_prediction_prob = model(nuevo_registro)
			valor_prediccion = torch.argmax(single_prediction_prob,
											dim=1).item() + 1  # Add 1 to match original range (1 to 39)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, valor_prediccion: {valor_prediccion}")

		# Update prediction dictionary as in original code
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "7." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
		prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)

		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)

		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)

		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)

		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de numeros impares
def prediccion_pxc(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (LT)
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG', 'IMPAR','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Dividir los datos en conjuntos de entrenamiento y prueba
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		if nombre_algoritmo == "log_reg":
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
		elif nombre_algoritmo == "rf":
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

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "8." + label
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


#prediccion de combinacion de pronostico por ciclo junto con numeros favorables
def prediccion_pxc_pref(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG','DECENA']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "9." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de combinacion de pronostico por ciclo junto con numeros favorables en base a tensorflow
def prediccion_pxc_pref_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG','DECENA']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0, 1, 2, or 3)

		# Preprocessing pipeline: scale numeric features
		numeric_features = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG','DECENA']
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Build a neural network model using TensorFlow/Keras for multi-class classification
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(4, activation='softmax')
			# Softmax for multi-class classification (4 classes: 0,1,2,3)
		])

		# Compile the model
		model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Predict on test set to evaluate the model
		y_pred_prob = model.predict(X_test)
		y_pred = y_pred_prob.argmax(axis=1)  # Get class with highest probability for each prediction

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset (similar to what was done in the original function)
		nuevo_registro = X_test[-1:]  # Assuming you want to predict for the last instance in the test set
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[0]  # Get the predicted class (0, 1, 2, or 3)

		# Print or return the single prediction
		print(f"Predicción para el registro más reciente: {valor_prediccion}")

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "9." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_lt: {e}")
	return None


#prediccion de combinacion de pronostico por ciclo junto con numeros favorables en base a torch
def prediccion_pxc_pref_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # Separate features and label
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG','DECENA']
        X = df[feature_columns].values  # Features
        y = df[label].values  # Label (expected values 0, 1, 2, or 3)

        # Preprocessing: scale numeric features
        scaler = StandardScaler()
        X = scaler.fit_transform(X)

        # Split data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Convert data to PyTorch tensors
        X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
        y_train_tensor = torch.tensor(y_train, dtype=torch.long)
        X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
        y_test_tensor = torch.tensor(y_test, dtype=torch.long)

        # Create DataLoader for training data
        train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
        train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

        # Define the neural network model
        class NeuralNet(nn.Module):
            def __init__(self, input_dim, output_dim):
                super(NeuralNet, self).__init__()
                self.fc1 = nn.Linear(input_dim, 64)
                self.fc2 = nn.Linear(64, 32)
                self.fc3 = nn.Linear(32, output_dim)

            def forward(self, x):
                x = torch.relu(self.fc1(x))
                x = torch.relu(self.fc2(x))
                return torch.softmax(self.fc3(x), dim=1)

        model = NeuralNet(X_train.shape[1], 4)  # 4 output classes (0, 1, 2, 3)

        # Loss and optimizer
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)

        # Training loop
        model.train()
        for epoch in range(epochs):
            for batch_X, batch_y in train_loader:
                # Forward pass
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)

                # Backward pass and optimization
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            #print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

        # Evaluate the model on the test set
        model.eval()
        with torch.no_grad():
            y_pred_prob = model(X_test_tensor)
            y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

        # Calculate accuracy
        precision_score = accuracy_score(y_test, y_pred)
        #print(f"Model Accuracy: {precision_score * 100:.2f}%")

        # Single prediction for the latest record in the dataset
        nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
        with torch.no_grad():
            valor_prediccion_prob = model(nuevo_registro)
            valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item()

        # Update prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "9." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion)
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion)
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion)
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        # Print or return the single prediction
        #print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt: {e}")
        return None


#prediccion de decenas
def prediccion_decena(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separar los features y el label (LT)
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Validación cruzada
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X.iloc[train_index], X.iloc[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            if nombre_algoritmo == "log_reg":
                # Inicializar y entrenar Logistic Regression
                log_reg = LogisticRegression(max_iter=300, random_state=42)
                log_reg.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                log_reg_pred = log_reg.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, log_reg_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = log_reg.predict(nuevo_registro)

            elif nombre_algoritmo == "rf":
                # Inicializar y entrenar Random Forest Classifier
                rf_clf = RandomForestClassifier(random_state=42)
                rf_clf.fit(X_train, y_train)

                # Realizar predicciones en el conjunto de validación
                rf_clf_pred = rf_clf.predict(X_val)

                # Calcular la precisión
                precision_score = accuracy_score(y_val, rf_clf_pred)
                accuracies.append(precision_score)

                # Suponer que se tiene un nuevo registro similar al último del conjunto de datos
                nuevo_registro = X.iloc[-1].values.reshape(1, -1)
                valor_prediccion = rf_clf.predict(nuevo_registro)

        # Promedio de precisión entre pliegues
        average_accuracy = np.mean(accuracies)

        # Actualizar las etiquetas de vuelta al rango original y mostrar las predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "10." + label
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#prediccion de decenas en base a tensorflow
def prediccion_decena_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0, 1, 2, or 3)

		# Preprocessing pipeline: scale numeric features
		numeric_features = feature_columns
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), numeric_features)]
		)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Apply preprocessing
		X_train = preprocessor.fit_transform(X_train)
		X_test = preprocessor.transform(X_test)

		# Subtract 1 from target labels so they start at 0 (for TensorFlow compatibility)
		y_train -= 1
		y_test -= 1

		# Build a neural network model using TensorFlow/Keras for multi-class classification
		model = tf.keras.Sequential([
			tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(64, activation='relu'),
			tf.keras.layers.Dense(32, activation='relu'),
			tf.keras.layers.Dense(4, activation='softmax')
			# Softmax for multi-class classification (4 classes: 0,1,2,3)
		])

		# Compile the model
		model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

		# Train the model
		model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_split=0.1, verbose=1)

		# Predict on test set to evaluate the model
		y_pred_prob = model.predict(X_test)
		y_pred = y_pred_prob.argmax(axis=1)  # Get class with highest probability for each prediction

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset (similar to what was done in the original function)
		nuevo_registro = X_test[-1:]  # Assuming you want to predict for the last instance in the test set
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[0] + 1 # Add 1 to match original class range (1 to 4)

		# Print or return the single prediction
		print(f"Predicción para el registro más reciente: {valor_prediccion}")

		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "10." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score,3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_lt: {e}")
	return None


#prediccion de decenas en base a torch
def prediccion_decena_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 1, 2, 3 or 4)

		# Adjust labels to range [0, 3] for CrossEntropyLoss compatibility
		y = y - 1

		# Preprocessing: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.long)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.long)

		# Create DataLoader for training data
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Define the neural network model
		class NeuralNet(nn.Module):
			def __init__(self, input_dim, output_dim):
				super(NeuralNet, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, output_dim)

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return torch.softmax(self.fc3(x), dim=1)

		model = NeuralNet(X_train.shape[1], 4)  # 4 output classes (1, 2, 3, 4)

		# Loss and optimizer
		criterion = nn.CrossEntropyLoss()
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Training loop
		model.train()
		for epoch in range(epochs):
			for batch_X, batch_y in train_loader:
				# Forward pass
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

			#print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred)
		#print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion_prob = model(nuevo_registro)
			valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1 # Adjust back to original range [1, 4]

		# Update prediction dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "10." + label
		prediccion_dic["prediccion_sorteo"] = sorteo_id

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)

		# Print or return the single prediction
		#print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except Exception as e:
		print(f"Error in prediccion_lt: {e}")
		return None


# Predicción de contador de números primos
def prediccion_primo_pip(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
    try:
        # Calcular el siguiente sorteo
        siguiente_sorteo = df['ID'].max() + 1

        # Definir las columnas de features y label
        feature_columns = ['NONE_CNT', 'PAR_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Dividir los datos en conjuntos de entrenamiento y prueba
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Seleccionar el algoritmo especificado
        if nombre_algoritmo == "log_reg":
            # Inicializar y entrenar Logistic Regression
            log_reg = LogisticRegression(max_iter=300, random_state=42)
            log_reg.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            log_reg_pred = log_reg.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, log_reg_pred)
            #print("Precisión de Logistic Regression:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = log_reg.predict(nuevo_registro)[0]

        elif nombre_algoritmo == "rf":
            # Inicializar y entrenar Random Forest Classifier
            rf_clf = RandomForestClassifier(random_state=42)
            rf_clf.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            rf_clf_pred = rf_clf.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, rf_clf_pred)
            #print("Precisión de Random Forest:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = rf_clf.predict(nuevo_registro)[0]

        # Asegurar que la predicción esté dentro del rango 0 a 6
        valor_prediccion = max(0, min(6, valor_prediccion))
        #print(f"Predicción ({nombre_algoritmo}):", valor_prediccion)

        # Actualizar el diccionario de predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt: {e}")
        return None


# Predicción de contador de números impares
def prediccion_impar_pip(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
    try:
        # Calcular el siguiente sorteo
        siguiente_sorteo = df['ID'].max() + 1

        # Definir las columnas de features y label
        feature_columns = ['PN_CNT', 'PAR_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Dividir los datos en conjuntos de entrenamiento y prueba
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Seleccionar el algoritmo especificado
        if nombre_algoritmo == "log_reg":
            # Inicializar y entrenar Logistic Regression
            log_reg = LogisticRegression(max_iter=300, random_state=42)
            log_reg.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            log_reg_pred = log_reg.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, log_reg_pred)
            #print("Precisión de Logistic Regression:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = log_reg.predict(nuevo_registro)[0]

        elif nombre_algoritmo == "rf":
            # Inicializar y entrenar Random Forest Classifier
            rf_clf = RandomForestClassifier(random_state=42)
            rf_clf.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            rf_clf_pred = rf_clf.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, rf_clf_pred)
            #print("Precisión de Random Forest:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = rf_clf.predict(nuevo_registro)[0]

        # Asegurar que la predicción esté dentro del rango 0 a 6
        valor_prediccion = max(0, min(6, valor_prediccion))
        #print(f"Predicción ({nombre_algoritmo}):", valor_prediccion)

        # Actualizar el diccionario de predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt: {e}")
        return None


# Predicción de contador de números pares
def prediccion_par_pip(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
    try:
        # Calcular el siguiente sorteo
        siguiente_sorteo = df['ID'].max() + 1

        # Definir las columnas de features y label
        feature_columns = ['PN_CNT', 'NONE_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Dividir los datos en conjuntos de entrenamiento y prueba
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Seleccionar el algoritmo especificado
        if nombre_algoritmo == "log_reg":
            # Inicializar y entrenar Logistic Regression
            log_reg = LogisticRegression(max_iter=300, random_state=42)
            log_reg.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            log_reg_pred = log_reg.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, log_reg_pred)
            #print("Precisión de Logistic Regression:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = log_reg.predict(nuevo_registro)[0]

        elif nombre_algoritmo == "rf":
            # Inicializar y entrenar Random Forest Classifier
            rf_clf = RandomForestClassifier(random_state=42)
            rf_clf.fit(X_train, y_train)

            # Realizar predicciones en el conjunto de prueba
            rf_clf_pred = rf_clf.predict(X_test)

            # Calcular la precisión del modelo
            precision_score = accuracy_score(y_test, rf_clf_pred)
            #print("Precisión de Random Forest:", precision_score)

            # Generar predicción para un nuevo registro (último registro en el dataset)
            nuevo_registro = X.iloc[-1].values.reshape(1, -1)
            valor_prediccion = rf_clf.predict(nuevo_registro)[0]

        # Asegurar que la predicción esté dentro del rango 0 a 6
        valor_prediccion = max(0, min(6, valor_prediccion))
        #print(f"Predicción ({nombre_algoritmo}):", valor_prediccion)

        # Actualizar el diccionario de predicciones
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt: {e}")
        return None


# Predicción de contador de números primos basados en tensorflow
def prediccion_primo_pip_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separate features and label
        feature_columns = ['NONE_CNT', 'PAR_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label (expected values range from 0 to 6)

        # Preprocessing pipeline: scale numeric features
        preprocessor = ColumnTransformer(
            transformers=[('num', StandardScaler(), feature_columns)]
        )
        X = preprocessor.fit_transform(X)

        # Initialize KFold
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)

        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            # Build a neural network model
            model = tf.keras.Sequential([
                tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
                tf.keras.layers.Dense(128, activation='relu'),
                tf.keras.layers.Dense(64, activation='relu'),
                tf.keras.layers.Dense(7, activation='softmax')  # Softmax for 7 classes (0 to 6)
            ])

            # Compile the model
            model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

            # Train the model
            model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_data=(X_val, y_val), verbose=1)

            # Evaluate accuracy on validation data
            y_val_pred_prob = model.predict(X_val)
            y_val_pred = y_val_pred_prob.argmax(axis=1)
            precision_score = accuracy_score(y_val, y_val_pred)
            accuracies.append(precision_score)

        # Average accuracy across folds
        average_accuracy = np.mean(accuracies)

        # Single prediction for the latest record in the dataset
        nuevo_registro = X[-1:]  # Last row for prediction
        single_prediction_prob = model.predict(nuevo_registro)
        valor_prediccion = single_prediction_prob.argmax(axis=1)[0]  # No need to add 1, as range is already 0 to 6

        # Print or return the single prediction
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# Predicción de contador de números impares basados en tensorflow
def prediccion_impar_pip_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separate features and label
        feature_columns = ['PN_CNT', 'PAR_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label (expected values range from 0 to 6)

        # Preprocessing pipeline: scale numeric features
        preprocessor = ColumnTransformer(
            transformers=[('num', StandardScaler(), feature_columns)]
        )
        X = preprocessor.fit_transform(X)

        # Initialize KFold
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)

        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            # Build a neural network model
            model = tf.keras.Sequential([
                tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
                tf.keras.layers.Dense(128, activation='relu'),
                tf.keras.layers.Dense(64, activation='relu'),
                tf.keras.layers.Dense(7, activation='softmax')  # Softmax for 7 classes (0 to 6)
            ])

            # Compile the model
            model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

            # Train the model
            model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_data=(X_val, y_val), verbose=1)

            # Evaluate accuracy on validation data
            y_val_pred_prob = model.predict(X_val)
            y_val_pred = y_val_pred_prob.argmax(axis=1)
            precision_score = accuracy_score(y_val, y_val_pred)
            accuracies.append(precision_score)

        # Average accuracy across folds
        average_accuracy = np.mean(accuracies)

        # Single prediction for the latest record in the dataset
        nuevo_registro = X[-1:]  # Last row for prediction
        single_prediction_prob = model.predict(nuevo_registro)
        valor_prediccion = single_prediction_prob.argmax(axis=1)[0]  # No need to add 1, as range is already 0 to 6

        # Print or return the single prediction
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# Predicción de contador de números pares basados en tensorflow
def prediccion_par_pip_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # 1. Separate features and label
        feature_columns = ['PN_CNT', 'NONE_CNT']
        X = df[feature_columns]  # Features
        y = df[label]  # Label (expected values range from 0 to 6)

        # Preprocessing pipeline: scale numeric features
        preprocessor = ColumnTransformer(
            transformers=[('num', StandardScaler(), feature_columns)]
        )
        X = preprocessor.fit_transform(X)

        # Initialize KFold
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)

        accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y.iloc[train_index], y.iloc[val_index]

            # Build a neural network model
            model = tf.keras.Sequential([
                tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
                tf.keras.layers.Dense(128, activation='relu'),
                tf.keras.layers.Dense(64, activation='relu'),
                tf.keras.layers.Dense(7, activation='softmax')  # Softmax for 7 classes (0 to 6)
            ])

            # Compile the model
            model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

            # Train the model
            model.fit(X_train, y_train, epochs=EPOCHS, batch_size=32, validation_data=(X_val, y_val), verbose=1)

            # Evaluate accuracy on validation data
            y_val_pred_prob = model.predict(X_val)
            y_val_pred = y_val_pred_prob.argmax(axis=1)
            precision_score = accuracy_score(y_val, y_val_pred)
            accuracies.append(precision_score)

        # Average accuracy across folds
        average_accuracy = np.mean(accuracies)

        # Single prediction for the latest record in the dataset
        nuevo_registro = X[-1:]  # Last row for prediction
        single_prediction_prob = model.predict(nuevo_registro)
        valor_prediccion = single_prediction_prob.argmax(axis=1)[0]  # No need to add 1, as range is already 0 to 6

        # Print or return the single prediction
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# Predicción de contador de números primos basados en torch
def prediccion_primo_pip_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # Separate features and label
        feature_columns = ['NONE_CNT', 'PAR_CNT']
        X = df[feature_columns].values  # Features
        y = df[label].values  # Label (expected values are integers from 0 to 6)

        # Preprocess data: scale numeric features
        scaler = StandardScaler()
        X = scaler.fit_transform(X)

        # KFold cross-validation
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        fold_accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y[train_index], y[val_index]

            # Convert data to PyTorch tensors
            X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
            y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
            X_val_tensor = torch.tensor(X_val, dtype=torch.float32)
            y_val_tensor = torch.tensor(y_val, dtype=torch.float32)

            # Create DataLoader for training data
            train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
            train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

            # Define the neural network model for regression
            class RegressionModel(nn.Module):
                def __init__(self, input_dim):
                    super(RegressionModel, self).__init__()
                    self.fc1 = nn.Linear(input_dim, 64)
                    self.fc2 = nn.Linear(64, 32)
                    self.fc3 = nn.Linear(32, 1)  # Single output neuron for regression

                def forward(self, x):
                    x = torch.relu(self.fc1(x))
                    x = torch.relu(self.fc2(x))
                    return self.fc3(x)  # Linear activation for regression

            model = RegressionModel(X_train.shape[1])

            # Loss and optimizer
            criterion = nn.MSELoss()  # Mean Squared Error Loss
            optimizer = optim.Adam(model.parameters(), lr=0.001)

            # Training loop
            model.train()
            for epoch in range(epochs):
                for batch_X, batch_y in train_loader:
                    # Forward pass
                    outputs = model(batch_X).squeeze()
                    loss = criterion(outputs, batch_y)

                    # Backward pass and optimization
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

            # Validate the model on the validation set
            model.eval()
            with torch.no_grad():
                y_val_pred = model(X_val_tensor).squeeze()
                mse = mean_squared_error(y_val, y_val_pred.numpy())
                accuracy = 1 - (mse / 6)  # Simplified accuracy metric based on range
                fold_accuracies.append(accuracy)

        # Average accuracy across folds
        average_accuracy = np.mean(fold_accuracies)

        # Predict for the latest record in the dataset
        nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
        with torch.no_grad():
            valor_prediccion = model(nuevo_registro).item()
            valor_prediccion = round(max(0, min(6, valor_prediccion)))  # Clip to range 0-6

        # Update the prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)

        if posicion == 1:
            prediccion_dic['prediccion_1'] = valor_prediccion
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = siguiente_sorteo
        if posicion == 2:
            prediccion_dic['prediccion_2'] = valor_prediccion
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = siguiente_sorteo
        if posicion == 3:
            prediccion_dic['prediccion_3'] = valor_prediccion
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = siguiente_sorteo

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# Predicción de contador de números impares basados en torch
def prediccion_impar_pip_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # Separate features and label
        feature_columns = ['PN_CNT', 'PAR_CNT']
        X = df[feature_columns].values  # Features
        y = df[label].values  # Label (expected values are integers from 0 to 6)

        # Preprocess data: scale numeric features
        scaler = StandardScaler()
        X = scaler.fit_transform(X)

        # KFold cross-validation
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        fold_accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y[train_index], y[val_index]

            # Convert data to PyTorch tensors
            X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
            y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
            X_val_tensor = torch.tensor(X_val, dtype=torch.float32)
            y_val_tensor = torch.tensor(y_val, dtype=torch.float32)

            # Create DataLoader for training data
            train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
            train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

            # Define the neural network model for regression
            class RegressionModel(nn.Module):
                def __init__(self, input_dim):
                    super(RegressionModel, self).__init__()
                    self.fc1 = nn.Linear(input_dim, 64)
                    self.fc2 = nn.Linear(64, 32)
                    self.fc3 = nn.Linear(32, 1)  # Single output neuron for regression

                def forward(self, x):
                    x = torch.relu(self.fc1(x))
                    x = torch.relu(self.fc2(x))
                    return self.fc3(x)  # Linear activation for regression

            model = RegressionModel(X_train.shape[1])

            # Loss and optimizer
            criterion = nn.MSELoss()  # Mean Squared Error Loss
            optimizer = optim.Adam(model.parameters(), lr=0.001)

            # Training loop
            model.train()
            for epoch in range(epochs):
                for batch_X, batch_y in train_loader:
                    # Forward pass
                    outputs = model(batch_X).squeeze()
                    loss = criterion(outputs, batch_y)

                    # Backward pass and optimization
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

            # Validate the model on the validation set
            model.eval()
            with torch.no_grad():
                y_val_pred = model(X_val_tensor).squeeze()
                mse = mean_squared_error(y_val, y_val_pred.numpy())
                accuracy = 1 - (mse / 6)  # Simplified accuracy metric based on range
                fold_accuracies.append(accuracy)

        # Average accuracy across folds
        average_accuracy = np.mean(fold_accuracies)

        # Predict for the latest record in the dataset
        nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
        with torch.no_grad():
            valor_prediccion = model(nuevo_registro).item()
            valor_prediccion = round(max(0, min(6, valor_prediccion)))  # Clip to range 0-6

        # Update the prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)

        if posicion == 1:
            prediccion_dic['prediccion_1'] = valor_prediccion
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = siguiente_sorteo
        if posicion == 2:
            prediccion_dic['prediccion_2'] = valor_prediccion
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = siguiente_sorteo
        if posicion == 3:
            prediccion_dic['prediccion_3'] = valor_prediccion
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = siguiente_sorteo

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


# Predicción de contador de números pares basados en torch
def prediccion_par_pip_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS, n_splits=N_SPLITS):
    try:
        # Determine the next draw number
        siguiente_sorteo = df['ID'].max() + 1

        # Separate features and label
        feature_columns = ['PN_CNT', 'NONE_CNT']
        X = df[feature_columns].values.astype(np.float32)  # Features (ensure compatibility with PyTorch)
        y = df[label].values.astype(np.float32)  # Label (ensure compatibility with PyTorch)

        # Preprocess data: scale numeric features
        scaler = StandardScaler()
        X = scaler.fit_transform(X)

        # KFold cross-validation
        kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        fold_accuracies = []

        for train_index, val_index in kf.split(X):
            X_train, X_val = X[train_index], X[val_index]
            y_train, y_val = y[train_index], y[val_index]

            # Convert data to PyTorch tensors
            X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
            y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
            X_val_tensor = torch.tensor(X_val, dtype=torch.float32)
            y_val_tensor = torch.tensor(y_val, dtype=torch.float32)

            # Create DataLoader for training data
            train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
            train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

            # Define the neural network model for regression
            class RegressionModel(nn.Module):
                def __init__(self, input_dim):
                    super(RegressionModel, self).__init__()
                    self.fc1 = nn.Linear(input_dim, 64)
                    self.fc2 = nn.Linear(64, 32)
                    self.fc3 = nn.Linear(32, 1)  # Single output neuron for regression

                def forward(self, x):
                    x = torch.relu(self.fc1(x))
                    x = torch.relu(self.fc2(x))
                    return self.fc3(x)  # Linear activation for regression

            model = RegressionModel(X_train.shape[1])

            # Loss and optimizer
            criterion = nn.MSELoss()  # Mean Squared Error Loss
            optimizer = optim.Adam(model.parameters(), lr=0.001)

            # Training loop
            model.train()
            for epoch in range(epochs):
                for batch_X, batch_y in train_loader:
                    # Forward pass
                    outputs = model(batch_X).squeeze()
                    loss = criterion(outputs, batch_y)

                    # Backward pass and optimization
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

            # Validate the model on the validation set
            model.eval()
            with torch.no_grad():
                y_val_pred = model(X_val_tensor).squeeze()
                mse = mean_squared_error(y_val, y_val_pred.numpy())
                accuracy = 1 - (mse / 6)  # Simplified accuracy metric based on range
                fold_accuracies.append(accuracy)

        # Average accuracy across folds
        average_accuracy = np.mean(fold_accuracies)

        # Predict for the latest record in the dataset
        nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)).astype(np.float32), dtype=torch.float32)
        with torch.no_grad():
            valor_prediccion = model(nuevo_registro).item()
            valor_prediccion = round(max(0, min(6, valor_prediccion)))  # Clip to range 0-6

        # Update the prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = "11.PIP"
        prediccion_dic["prediccion_sorteo"] = sorteo_id
        prediccion_dic["precision_promedio"] = round(average_accuracy, 3)

        if posicion == 1:
            prediccion_dic['prediccion_1'] = valor_prediccion
            prediccion_dic['precision_1'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_1"] = siguiente_sorteo
        if posicion == 2:
            prediccion_dic['prediccion_2'] = valor_prediccion
            prediccion_dic['precision_2'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_2"] = siguiente_sorteo
        if posicion == 3:
            prediccion_dic['prediccion_3'] = valor_prediccion
            prediccion_dic['precision_3'] = round(average_accuracy, 3)
            prediccion_dic["siguiente_sorteo_3"] = siguiente_sorteo

        return prediccion_dic

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante el procesamiento: {e}")
        raise


#calculo de predicciones basadas en termnaciones
def prediccion_terminaciones(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS):
	try:
		# Determinar el siguiente sorteo basado en ID
		siguiente_sorteo = df['ID'].max() + 1

		# Select features based on position
		feature_columns_options = {
			1: ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			2: ['T1', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			3: ['T1', 'T2', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			4: ['T1', 'T2', 'T3', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			5: ['T1', 'T2', 'T3', 'T4', 'T6', 'T7', 'T8', 'T9', 'T0'],
			6: ['T1', 'T2', 'T3', 'T4', 'T5', 'T7', 'T8', 'T9', 'T0'],
			7: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T8', 'T9', 'T0'],
			8: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T9', 'T0'],
			9: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T0'],
			10: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9']
		}

		feature_columns = feature_columns_options.get(posicion)
		if not feature_columns:
			raise ValueError("Posición no válida. Debe estar entre 1 y 10.")

		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Validación cruzada
		if not isinstance(n_splits, int) or n_splits <= 2:
			raise ValueError("n_splits debe ser un entero mayor o igual a 2.")

		kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
		accuracies = []
		best_model = None

		for train_index, val_index in kf.split(X):
			X_train, X_val = X.iloc[train_index], X.iloc[val_index]
			y_train, y_val = y.iloc[train_index], y.iloc[val_index]

			if nombre_algoritmo == "log_reg":
				# Inicializar y entrenar Logistic Regression
				log_reg = LogisticRegression(max_iter=300, random_state=42)
				log_reg.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				log_reg_pred = log_reg.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, log_reg_pred)
				accuracies.append(precision_score)

				# Guardar el modelo para predicción final
				best_model = log_reg

			elif nombre_algoritmo == "rf":
				# Inicializar y entrenar Random Forest Classifier
				rf_clf = RandomForestClassifier(random_state=42)
				rf_clf.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				rf_clf_pred = rf_clf.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, rf_clf_pred)
				accuracies.append(precision_score)

				# Guardar el modelo para predicción final
				best_model = rf_clf

		# Promedio de precisión entre pliegues
		average_accuracy = np.mean(accuracies)

		# Crear un registro simulado para el siguiente consecutivo
		nuevo_registro = X.iloc[-1].values.reshape(1, -1)
		valor_prediccion = best_model.predict(nuevo_registro)

		# Actualizar las predicciones en el diccionario
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "12.TERMINACIONES"
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		#prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
		#prediccion_dic["valor_prediccion"] = int(valor_prediccion[0])

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		if posicion == 7:
			prediccion_dic['prediccion_7'] = round(valor_prediccion[0])
			prediccion_dic['precision_7'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_7"] = round(siguiente_sorteo)
		if posicion == 8:
			prediccion_dic['prediccion_8'] = round(valor_prediccion[0])
			prediccion_dic['precision_8'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_8"] = round(siguiente_sorteo)
		if posicion == 9:
			prediccion_dic['prediccion_9'] = round(valor_prediccion[0])
			prediccion_dic['precision_9'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_9"] = round(siguiente_sorteo)
		if posicion == 10:
			prediccion_dic['prediccion_0'] = round(valor_prediccion[0])
			prediccion_dic['precision_0'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_0"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#calculo de predicciones basadas en termnaciones
def prediccion_terminaciones_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS,
								epochs=EPOCHS):
	try:
		# Determine the next draw number based on GAMBLING_ID
		siguiente_sorteo = df['ID'].max() + 1

		# Select features based on position
		feature_columns_options = {
			1: ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			2: ['T1', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			3: ['T1', 'T2', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			4: ['T1', 'T2', 'T3', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			5: ['T1', 'T2', 'T3', 'T4', 'T6', 'T7', 'T8', 'T9', 'T0'],
			6: ['T1', 'T2', 'T3', 'T4', 'T5', 'T7', 'T8', 'T9', 'T0'],
			7: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T8', 'T9', 'T0'],
			8: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T9', 'T0'],
			9: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T0'],
			10: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9']
		}

		feature_columns = feature_columns_options.get(posicion)
		if not feature_columns:
			raise ValueError("Posición no válida. Debe estar entre 1 y 10.")

		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values range from 0 to 6)

		# Preprocessing pipeline: scale numeric features
		preprocessor = ColumnTransformer(
			transformers=[('num', StandardScaler(), feature_columns)]
		)
		X = preprocessor.fit_transform(X)

		# Initialize KFold
		kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
		accuracies = []

		for train_index, val_index in kf.split(X):
			X_train, X_val = X[train_index], X[val_index]
			y_train, y_val = y.iloc[train_index], y.iloc[val_index]

			# Build a neural network model
			model = tf.keras.Sequential([
				tf.keras.layers.InputLayer(input_shape=(X_train.shape[1],)),
				tf.keras.layers.Dense(128, activation='relu'),
				tf.keras.layers.Dense(64, activation='relu'),
				tf.keras.layers.Dense(7, activation='softmax')  # Softmax for 7 classes (0 to 6)
			])

			# Compile the model
			model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

			# Train the model
			model.fit(X_train, y_train, epochs=epochs, batch_size=32, validation_data=(X_val, y_val), verbose=0)

			# Evaluate accuracy on validation data
			y_val_pred_prob = model.predict(X_val)
			y_val_pred = y_val_pred_prob.argmax(axis=1)
			precision_score = accuracy_score(y_val, y_val_pred)
			accuracies.append(precision_score)

		# Average accuracy across folds
		average_accuracy = np.mean(accuracies)

		# Single prediction for the latest record in the dataset
		nuevo_registro = X[-1:].reshape(1, -1)  # Last row for prediction
		single_prediction_prob = model.predict(nuevo_registro)
		valor_prediccion = single_prediction_prob.argmax(axis=1)[0]  # No need to add 1, as range is already 0 to 6

		# Update predictions in the dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "12.TERMINACIONES"
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		#prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
		prediccion_dic[f"prediccion_{posicion}"] = round(valor_prediccion)
		prediccion_dic[f"precision_{posicion}"] = round(precision_score, 3)
		prediccion_dic[f"siguiente_sorteo_{posicion}"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#calculo de predicciones basadas en termnaciones
def prediccion_terminaciones_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, n_splits=N_SPLITS,
								epochs=EPOCHS):
	try:
		# Determine the next draw number based on GAMBLING_ID
		siguiente_sorteo = df['ID'].max() + 1

		# Select features based on position
		feature_columns_options = {
			1: ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			2: ['T1', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			3: ['T1', 'T2', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			4: ['T1', 'T2', 'T3', 'T5', 'T6', 'T7', 'T8', 'T9', 'T0'],
			5: ['T1', 'T2', 'T3', 'T4', 'T6', 'T7', 'T8', 'T9', 'T0'],
			6: ['T1', 'T2', 'T3', 'T4', 'T5', 'T7', 'T8', 'T9', 'T0'],
			7: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T8', 'T9', 'T0'],
			8: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T9', 'T0'],
			9: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T0'],
			10: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9']
		}

		feature_columns = feature_columns_options.get(posicion)
		if not feature_columns:
			raise ValueError("Posición no válida. Debe estar entre 1 y 10.")

		X = df[feature_columns].values
		y = df[label].values

		# Escalar características
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Dividir datos en entrenamiento y prueba
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convertir datos a tensores
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32).view(-1, 1)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32).view(-1, 1)

		# Crear DataLoader para el conjunto de entrenamiento
		train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
		train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

		# Definir la red neuronal
		class MultiClassModel(nn.Module):
			def __init__(self, input_dim):
				super(MultiClassModel, self).__init__()
				self.fc1 = nn.Linear(input_dim, 64)
				self.fc2 = nn.Linear(64, 32)
				self.fc3 = nn.Linear(32, 1)

			def forward(self, x):
				x = torch.relu(self.fc1(x))
				x = torch.relu(self.fc2(x))
				return self.fc3(x)

		# Inicializar el modelo
		model = MultiClassModel(input_dim=X_train.shape[1])
		criterion = nn.CrossEntropyLoss()  # Para clasificación multiclase
		optimizer = optim.Adam(model.parameters(), lr=0.001)

		# Entrenar el modelo
		for epoch in range(epochs):
			model.train()
			for batch_X, batch_y in train_loader:
				outputs = model(batch_X)
				loss = criterion(outputs, batch_y)

				optimizer.zero_grad()
				loss.backward()
				optimizer.step()

		# Evaluar el modelo
		model.eval()
		with torch.no_grad():
			y_pred = model(X_test_tensor).argmax(dim=1)  # Seleccionar la clase con mayor probabilidad
			precision_score = accuracy_score(y_test_tensor.numpy(), y_pred.numpy())
			#precision_score = accuracy_score(y_test_tensor.numpy(), y_pred.numpy().squeeze())

		# Predecir para el siguiente registro
		nuevo_registro = torch.tensor(scaler.transform(X[-1].reshape(1, -1)), dtype=torch.float32)
		with torch.no_grad():
			valor_prediccion = model(nuevo_registro).argmax(dim=1).item()
			#si la funcion continua calculando 0 para todos los labels entonces probar la siguiente linea
			#valor_prediccion = model(nuevo_registro).item()

		print(f'valor_prediccion: {valor_prediccion}, precision_score: {precision_score}')

		# Update predictions in the dictionary
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "12.TERMINACIONES"
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		#prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
		prediccion_dic[f"prediccion_{posicion}"] = round(valor_prediccion)
		prediccion_dic[f"precision_{posicion}"] = round(precision_score, 3)
		prediccion_dic[f"siguiente_sorteo_{posicion}"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de validación: {ve}")
		raise

	except Exception as e:
		print(f"Error inesperado: {e}")
		raise


#calculo de predicciones basadas en entradas genericas
def prediccion_generic(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, generic_option_name, n_splits=N_SPLITS):
	try:
		# Determinar el siguiente sorteo basado en ID
		siguiente_sorteo = df['ID'].max() + 1

		# Select features based on position
		feature_columns_options = {
			1: ['POS2', 'POS3', 'POS4', 'POS5', 'POS6'],
			2: ['POS1', 'POS3', 'POS4', 'POS5', 'POS6'],
			3: ['POS1', 'POS2', 'POS4', 'POS5', 'POS6'],
			4: ['POS1', 'POS2', 'POS3', 'POS5', 'POS6'],
			5: ['POS1', 'POS2', 'POS3', 'POS4', 'POS6'],
			6: ['POS1', 'POS2', 'POS3', 'POS4', 'POS5']
		}

		feature_columns = feature_columns_options.get(posicion)
		if not feature_columns:
			raise ValueError("Posición no válida. Debe estar entre 1 y 6.")

		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Validación cruzada
		if not isinstance(n_splits, int) or n_splits <= 2:
			raise ValueError("n_splits debe ser un entero mayor o igual a 2.")

		# Validación cruzada
		kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
		accuracies = []

		for train_index, val_index in kf.split(X):
			X_train, X_val = X.iloc[train_index], X.iloc[val_index]
			y_train, y_val = y.iloc[train_index], y.iloc[val_index]

			if nombre_algoritmo == "log_reg":
				# Inicializar y entrenar Logistic Regression
				log_reg = LogisticRegression(max_iter=300, random_state=42)
				log_reg.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				log_reg_pred = log_reg.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, log_reg_pred)
				accuracies.append(precision_score)

				# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
				nuevo_registro = X.iloc[-1].values.reshape(1, -1)
				valor_prediccion = log_reg.predict(nuevo_registro)

			elif nombre_algoritmo == "rf":
				# Inicializar y entrenar Random Forest Classifier
				rf_clf = RandomForestClassifier(random_state=42)
				rf_clf.fit(X_train, y_train)

				# Realizar predicciones en el conjunto de validación
				rf_clf_pred = rf_clf.predict(X_val)

				# Calcular la precisión
				precision_score = accuracy_score(y_val, rf_clf_pred)
				accuracies.append(precision_score)

				# Suponer que se tiene un nuevo registro similar al último del conjunto de datos
				nuevo_registro = X.iloc[-1].values.reshape(1, -1)
				valor_prediccion = rf_clf.predict(nuevo_registro)

			# Validar que valor_prediccion esté en el rango permitido
			valor_prediccion = np.clip(valor_prediccion, 1, 3)

		# Promedio de precisión entre pliegues
		average_accuracy = np.mean(accuracies)

		# Actualizar las predicciones en el diccionario
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = generic_option_name
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		#prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
		#prediccion_dic["valor_prediccion"] = int(valor_prediccion[0])

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion[0])
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion[0])
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion[0])
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion[0])
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion[0])
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion[0])
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		if posicion == 7:
			prediccion_dic['prediccion_7'] = round(valor_prediccion[0])
			prediccion_dic['precision_7'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_7"] = round(siguiente_sorteo)
		if posicion == 8:
			prediccion_dic['prediccion_8'] = round(valor_prediccion[0])
			prediccion_dic['precision_8'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_8"] = round(siguiente_sorteo)
		if posicion == 9:
			prediccion_dic['prediccion_9'] = round(valor_prediccion[0])
			prediccion_dic['precision_9'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_9"] = round(siguiente_sorteo)
		if posicion == 10:
			prediccion_dic['prediccion_0'] = round(valor_prediccion[0])
			prediccion_dic['precision_0'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_0"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise



#prediccion generica de ley del tercio en base a tensorflow
def prediccion_tf_generic(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, generic_option_name):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Select features based on position
		feature_columns_options = {
			1: ['POS2', 'POS3', 'POS4', 'POS5', 'POS6'],
			2: ['POS1', 'POS3', 'POS4', 'POS5', 'POS6'],
			3: ['POS1', 'POS2', 'POS4', 'POS5', 'POS6'],
			4: ['POS1', 'POS2', 'POS3', 'POS5', 'POS6'],
			5: ['POS1', 'POS2', 'POS3', 'POS4', 'POS6'],
			6: ['POS1', 'POS2', 'POS3', 'POS4', 'POS5']
		}

		feature_columns = feature_columns_options.get(posicion)
		if not feature_columns:
			raise ValueError("Posición no válida. Debe estar entre 1 y 6.")

		X = df[feature_columns]  # Features
		y = df[label]  # Label

		# Estandarizar las características
		scaler = StandardScaler()
		X_scaled = scaler.fit_transform(X)

		# Dividir los datos en entrenamiento y prueba
		X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

		# Convertir las etiquetas a formato categórico
		y_train_categorical = tf.keras.utils.to_categorical(y_train - 1, num_classes=3)
		y_test_categorical = tf.keras.utils.to_categorical(y_test - 1, num_classes=3)

		# Crear el modelo de TensorFlow
		model = tf.keras.Sequential([
			tf.keras.layers.Dense(16, activation='relu', input_shape=(X_train.shape[1],)),
			tf.keras.layers.Dense(8, activation='relu'),
			tf.keras.layers.Dense(3, activation='softmax')
		])

		# Compilar el modelo
		model.compile(optimizer='adam',
					  loss='categorical_crossentropy',
					  metrics=['accuracy'])

		# Entrenar el modelo
		history = model.fit(X_train, y_train_categorical, epochs=EPOCHS, batch_size=8, verbose=0,
				  validation_data=(X_test, y_test_categorical))

		# Calcular la precisión en el conjunto de prueba
		test_loss, precision_score = model.evaluate(X_test, y_test_categorical, verbose=0)

		# Preparar el nuevo registro para predecir (ID 645)
		nuevo_registro = df[feature_columns].iloc[-1:].values
		nuevo_registro = scaler.transform(nuevo_registro)

		# Hacer la predicción
		prediccion = model.predict(nuevo_registro)
		valor_prediccion = np.argmax(prediccion) + 1

		# Actualizar las predicciones en el diccionario
		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = generic_option_name
		prediccion_dic["prediccion_sorteo"] = sorteo_id
		#prediccion_dic["precision_promedio"] = round(average_accuracy, 3)
		#prediccion_dic["valor_prediccion"] = int(valor_prediccion[0])

		if posicion == 1:
			prediccion_dic['prediccion_1'] = round(valor_prediccion)
			prediccion_dic['precision_1'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
		if posicion == 2:
			prediccion_dic['prediccion_2'] = round(valor_prediccion)
			prediccion_dic['precision_2'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
		if posicion == 3:
			prediccion_dic['prediccion_3'] = round(valor_prediccion)
			prediccion_dic['precision_3'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
		if posicion == 4:
			prediccion_dic['prediccion_4'] = round(valor_prediccion)
			prediccion_dic['precision_4'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
		if posicion == 5:
			prediccion_dic['prediccion_5'] = round(valor_prediccion)
			prediccion_dic['precision_5'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
		if posicion == 6:
			prediccion_dic['prediccion_6'] = round(valor_prediccion)
			prediccion_dic['precision_6'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
		if posicion == 7:
			prediccion_dic['prediccion_7'] = round(valor_prediccion)
			prediccion_dic['precision_7'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_7"] = round(siguiente_sorteo)
		if posicion == 8:
			prediccion_dic['prediccion_8'] = round(valor_prediccion)
			prediccion_dic['precision_8'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_8"] = round(siguiente_sorteo)
		if posicion == 9:
			prediccion_dic['prediccion_9'] = round(valor_prediccion)
			prediccion_dic['precision_9'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_9"] = round(siguiente_sorteo)
		if posicion == 10:
			prediccion_dic['prediccion_0'] = round(valor_prediccion)
			prediccion_dic['precision_0'] = round(precision_score, 3)
			prediccion_dic["siguiente_sorteo_0"] = round(siguiente_sorteo)

		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion ley del tercio en base a torch
def prediccion_torch_generic(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, generic_option_name, epochs=EPOCHS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # Select features based on position
        feature_columns_options = {
            1: ['POS2', 'POS3', 'POS4', 'POS5', 'POS6'],
            2: ['POS1', 'POS3', 'POS4', 'POS5', 'POS6'],
            3: ['POS1', 'POS2', 'POS4', 'POS5', 'POS6'],
            4: ['POS1', 'POS2', 'POS3', 'POS5', 'POS6'],
            5: ['POS1', 'POS2', 'POS3', 'POS4', 'POS6'],
            6: ['POS1', 'POS2', 'POS3', 'POS4', 'POS5']
        }

        feature_columns = feature_columns_options.get(posicion)
        if not feature_columns:
            raise ValueError("Posición no válida. Debe estar entre 1 y 6.")

        X = df[feature_columns]  # Features
        y = df[label]  # Label

        # Preprocessing for scaling
        numeric_features = feature_columns

        # Create preprocessing pipeline for scaling numeric features
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), [feature_columns.index(f) for f in numeric_features])
            ])

        # Split data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Apply preprocessing
        X_train = preprocessor.fit_transform(X_train)
        X_test = preprocessor.transform(X_test)

        # Ensure labels are within bounds for PyTorch (0, 1, 2)
        if y_train.min() < 2 or y_test.min() < 2:
            raise ValueError("Labels must be 1, 2, or 3.")

        # Convert labels to range starting from 0 (PyTorch expects classes starting from 0)
        y_train = y_train - 1
        y_test = y_test - 1

        # Convert data to PyTorch tensors
        X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
        y_train_tensor = torch.tensor(y_train.values, dtype=torch.long)
        X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
        y_test_tensor = torch.tensor(y_test.values, dtype=torch.long)

        # Create DataLoader for training data
        train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
        train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

        # Define the neural network model
        class NeuralNet(nn.Module):
            def __init__(self, input_dim, output_dim):
                super(NeuralNet, self).__init__()
                self.fc1 = nn.Linear(input_dim, 64)
                self.fc2 = nn.Linear(64, 32)
                self.fc3 = nn.Linear(32, output_dim)

            def forward(self, x):
                x = torch.relu(self.fc1(x))
                x = torch.relu(self.fc2(x))
                x = torch.softmax(self.fc3(x), dim=1)
                return x

        # Instantiate the model, loss function, and optimizer
        model = NeuralNet(X_train.shape[1], 3)  # 3 classes for output (1, 2, 3)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001)

        # Training loop
        model.train()
        for epoch in range(epochs):
            for batch_X, batch_y in train_loader:
                # Forward pass
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)

                # Backward pass and optimization
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        # Evaluate the model on test data
        model.eval()
        with torch.no_grad():
            y_pred_prob = model(X_test_tensor)
            y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

        # Calculate accuracy
        precision_score = accuracy_score(y_test, y_pred)

        # Single prediction for the latest record in the dataset
        X_latest = X_test_tensor[-1].unsqueeze(0)  # Assuming you want the prediction for the last test instance
        with torch.no_grad():
            valor_prediccion_prob = model(X_latest)
            valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1  # Add 1 to match original labels (1, 2, 3)

        # Update prediction dictionary
        prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
        prediccion_dic["prediccion_tipo"] = generic_option_name
        prediccion_dic["prediccion_sorteo"] = sorteo_id

        if posicion == 1:
            prediccion_dic['prediccion_1'] = round(valor_prediccion)
            prediccion_dic['precision_1'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_1"] = round(siguiente_sorteo)
        if posicion == 2:
            prediccion_dic['prediccion_2'] = round(valor_prediccion)
            prediccion_dic['precision_2'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_2"] = round(siguiente_sorteo)
        if posicion == 3:
            prediccion_dic['prediccion_3'] = round(valor_prediccion)
            prediccion_dic['precision_3'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_3"] = round(siguiente_sorteo)
        if posicion == 4:
            prediccion_dic['prediccion_4'] = round(valor_prediccion)
            prediccion_dic['precision_4'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_4"] = round(siguiente_sorteo)
        if posicion == 5:
            prediccion_dic['prediccion_5'] = round(valor_prediccion)
            prediccion_dic['precision_5'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_5"] = round(siguiente_sorteo)
        if posicion == 6:
            prediccion_dic['prediccion_6'] = round(valor_prediccion)
            prediccion_dic['precision_6'] = round(precision_score, 3)
            prediccion_dic["siguiente_sorteo_6"] = round(siguiente_sorteo)
        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_torch_generic: {e}")
        return None


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
		elif label == 'PXC_PREF':
			prediccion_pxc_pref(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		#elif label == 'PXC':
		#	prediccion_pxc(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'DECENA':
			prediccion_decena(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones_2(df, nombre_algoritmo:str, label:str, id_base:int):

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
			prediccion_lt_2(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "FR":
			prediccion_fr_2(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


#proceso encargado de ejecutar todos los modelos de prediccion basados en tensorflow
def procesa_predicciones_tf(df, nombre_algoritmo:str, label:str, id_base:int):

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

		# prediccion relacionadas con pxc y numeros favorables basaos en tensorflow
		# prediccion relacionadas al ley del tercio
		if label == "LT":
			prediccion_lt_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "FR":
			prediccion_fr_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "PXC_PREF":
			prediccion_pxc_pref_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "DIGIT":
			prediccion_digit_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "PRIMO":
			prediccion_primo_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'IMPAR':
			prediccion_impar_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'PAR':
			prediccion_par_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'CHNG':
			prediccion_change_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'DECENA':
			prediccion_decena_tf(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


#proceso encargado de ejecutar todos los modelos de prediccion basados en torch
def procesa_predicciones_torch(df, nombre_algoritmo:str, label:str, id_base:int):

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

		# prediccion relacionadas con pxc y numeros favorables basaos en tensorflow
		# prediccion relacionadas al ley del tercio
		if label == "LT":
			prediccion_lt_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "FR":
			prediccion_fr_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "DIGIT":
			prediccion_digit_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "PXC_PREF":
			prediccion_pxc_pref_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == "PRIMO":
			prediccion_primo_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'IMPAR':
			prediccion_impar_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'PAR':
			prediccion_par_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'CHNG':
			prediccion_change_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'DECENA':
			prediccion_decena_torch(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones_pip(df, nombre_algoritmo:str, id_base:int):

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

	for posicion in range(1,4):
		if posicion == 1:
			label = "PN_CNT"
			if nombre_algoritmo in ("rf","log_reg"):
				# prediccion contador de números primos
				prediccion_primo_pip(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			if nombre_algoritmo == "tensorflow":
				# Predicción de contador de números primos basados en tensorflow
				prediccion_primo_pip_tf(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			if nombre_algoritmo == "torch":
				# Predicción de contador de números primos basados en torch
				prediccion_primo_pip_torch(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)
		elif posicion == 2:
			label = "NONE_CNT"
			if nombre_algoritmo in ("rf", "log_reg"):
				# Predicción de contador de números impares
				prediccion_impar_pip(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			if nombre_algoritmo == "tensorflow":
				# Predicción de contador de números impares basados en tensorflow
				prediccion_impar_pip_tf(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			if nombre_algoritmo == "torch":
				# Predicción de contador de números impares basados en torch
				prediccion_impar_pip_torch(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)
		elif posicion == 3:
			label = "PAR_CNT"
			if nombre_algoritmo in ("rf", "log_reg"):
				# Predicción de contador de números pares
				prediccion_par_pip(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			if nombre_algoritmo == "tensorflow":
				# Predicción de contador de números pares basados en tensorflow
				prediccion_par_pip_tf(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

			#comentado debido a que esta lanznado una excepcion
			#if nombre_algoritmo == "torch":
				# Predicción de contador de números pares basados en tensorflow
				#prediccion_par_pip_torch(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if posicion == 3:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)

#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones_terminaciones(df, nombre_algoritmo:str, id_base:int):
	#permite que se guarden los 10 valores de las predicciones
	ejecucion_tipo = "long"

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
		"precision_6": 0.0,
		"siguiente_sorteo_7": 0,
		"prediccion_7": 0,
		"precision_7": 0.0,
		"siguiente_sorteo_8": 0,
		"prediccion_8": 0,
		"precision_8": 0.0,
		"siguiente_sorteo_9": 0,
		"prediccion_9": 0,
		"precision_9": 0.0,
		"siguiente_sorteo_0": 0,
		"prediccion_0": 0,
		"precision_0": 0.0
	}

	for posicion in range(1,11):
		# Select features based on position
		label_options = {
			1: ['T1'],
			2: ['T2'],
			3: ['T3'],
			4: ['T4'],
			5: ['T5'],
			6: ['T6'],
			7: ['T7'],
			8: ['T8'],
			9: ['T9'],
			10: ['T0']
		}

		label = label_options.get(posicion)
		if not label:
			raise ValueError("Posición no válida. Debe estar entre 1 y 10.")

		if nombre_algoritmo in ("rf", "log_reg"):
			# calculo de predicciones basadas en termnaciones
			prediccion_terminaciones(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

		if nombre_algoritmo == "tensorflow":
			# Predicción de contador de números primos basados en tensorflow
			prediccion_terminaciones_tf(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

		if nombre_algoritmo == "torch":
			# Predicción de contador de números primos basados en torch
			prediccion_terminaciones_torch(df, label, id_base, posicion, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if posicion == 10:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl, ejecucion_tipo)


#proceso encargado de ejecutar todos los modelos de prediccion
def procesa_predicciones_generic(df_generic, nombre_algoritmo:str, id_base:int, generic_option_name:str):
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
		"precision_6": 0.0,
		"siguiente_sorteo_7": 0,
		"prediccion_7": 0,
		"precision_7": 0.0,
		"siguiente_sorteo_8": 0,
		"prediccion_8": 0,
		"precision_8": 0.0,
		"siguiente_sorteo_9": 0,
		"prediccion_9": 0,
		"precision_9": 0.0,
		"siguiente_sorteo_0": 0,
		"prediccion_0": 0,
		"precision_0": 0.0
	}

	for posicion in range(1,7):
		#defenicion del label
		label = 'POS' + str(posicion)

		if nombre_algoritmo in ("rf", "log_reg"):
			# Predicción de contador de números primos basados en rf, log_reg
			prediccion_generic(df_generic, label, id_base, posicion, prediccion_gl, nombre_algoritmo, generic_option_name)

		if nombre_algoritmo == "tensorflow":
			# prediccion generica de ley del tercio en base a tensorflow
			prediccion_tf_generic(df_generic, label, id_base, posicion, prediccion_gl, nombre_algoritmo, generic_option_name)

		if nombre_algoritmo == "torch":
			# prediccion generica de ley del tercio en base a tensorflow
			prediccion_torch_generic(df_generic, label, id_base, posicion, prediccion_gl, nombre_algoritmo, generic_option_name)

		#ya que estan completas las predicciones de la jugada se imprimen
		if posicion == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		dh.ejecutar_procedimiento(prediccion_gl)


def procesa_subtarea(id_base, df):
	#ley del tercio
	label = "LT"
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	procesa_predicciones_2(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)
	procesa_predicciones_2(df, nombre_algoritmo, label, id_base)

	#ley del tercio
	label = "FR"
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	procesa_predicciones_2(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)
	procesa_predicciones_2(df, nombre_algoritmo, label, id_base)

	#numeros primos
	label = 'PRIMO'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	#numeros impares
	label = 'IMPAR'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	#numeros pares
	label = 'PAR'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	#numeros con cambio
	label = 'CHNG'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	#digits
	label = 'DIGIT'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	#decenas
	label = 'DECENA'
	print("-------------------------------------")
	nombre_algoritmo = "log_reg"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)

	print("-------------------------------------")
	nombre_algoritmo = "rf"
	procesa_predicciones(df, nombre_algoritmo, label, id_base)



# proceso encargado de ejecutar el modelo de prediccion para el label preferencia_flag
def procesa_tarea_sorteo_base(id_base):
	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe(id_base, SORTEO_BASE)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		#numeros favorables
		label = "PXC_PREF"
		print("-------------------------------------")
		nombre_algoritmo = "log_reg"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "rf"
		procesa_predicciones(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "tensorflow"
		procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)

		print("-------------------------------------")
		nombre_algoritmo = "torch"
		procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)
	else:
		print("Hay valores NaN en el dataset")
		raise


# proceso encargado de ejecutar los modelos de prediccion basados en neural networks
def procesa_subtarea_nn(id_base, df):
	#ley del tercio
	label = "LT"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#frequencia
	label = "FR"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#digitos
	label = "DIGIT"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#primos
	label = "PRIMO"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#impares
	label = "IMPAR"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#pares
	label = "PAR"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#numeros con cambios
	label = "CHNG"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)

	#decenas
	label = "DECENA"
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	procesa_predicciones_tf(df, nombre_algoritmo, label, id_base)
	nombre_algoritmo = "torch"
	procesa_predicciones_torch(df, nombre_algoritmo, label, id_base)


# proceso encargado de ejecutar los modelos de prediccion basados en primos, impares y pares
def procesa_subtarea_pip(id_base, df):
	nombre_algoritmo = "log_reg"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_pip(df, nombre_algoritmo, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "rf"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_pip(df, nombre_algoritmo, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_pip(df, nombre_algoritmo, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "torch"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_pip(df, nombre_algoritmo, id_base)



# proceso encargado de ejecutar los modelos de prediccion basados en el conteo de terminaciones
def procesa_subtarea_terminaciones(id_base, df):
	nombre_algoritmo = "log_reg"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_terminaciones(df, nombre_algoritmo, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "rf"
	#proceso encargado
	# de ejecutar todos los modelos de prediccion
	procesa_predicciones_terminaciones(df, nombre_algoritmo, id_base)
	print("-------------------------------------")
	nombre_algoritmo = "tensorflow"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_terminaciones(df, nombre_algoritmo, id_base)
	#print("-------------------------------------")
	#nombre_algoritmo = "torch"
	#proceso encargado de ejecutar todos los modelos de prediccion
	#procesa_predicciones_terminaciones(df, nombre_algoritmo, id_base)


# proceso encargado de ejecutar los modelos de prediccion basados en el conteo de terminaciones
def procesa_subtarea_generic(id_base, df_generic, generic_option_name):
	nombre_algoritmo = "log_reg"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_generic(df_generic, nombre_algoritmo, id_base, generic_option_name)

	nombre_algoritmo = "rf"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_generic(df_generic, nombre_algoritmo, id_base, generic_option_name)

	nombre_algoritmo = "tensorflow"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_generic(df_generic, nombre_algoritmo, id_base, generic_option_name)
	"""
	nombre_algoritmo = "torch"
	#proceso encargado de ejecutar todos los modelos de prediccion
	procesa_predicciones_generic(df_generic, nombre_algoritmo, id_base, generic_option_name)
	"""
#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
def procesa_tarea(id_base):
	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		# proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
		procesa_subtarea(id_base, df)
		# proceso encargado de ejecutar los modelos de prediccion basados en neural networks
		procesa_subtarea_nn(id_base, df)
	else:
		print("Hay valores NaN en el dataset")
		raise

#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
#basado en primos, impares y pares
def procesa_tarea_pip(id_base):
	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe_pip(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		# proceso encargado de ejecutar los modelos de prediccion basados en primos, impares y pares
		procesa_subtarea_pip(id_base, df)
	else:
		print("Hay valores NaN en el dataset")
		raise


#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
#basado en el conteo de terminaciones
def procesa_tarea_terminaciones(id_base):
	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe_terminaciones(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		# proceso encargado de ejecutar los modelos de prediccion basados en conteo de terminaciones
		procesa_subtarea_terminaciones(id_base, df)
	else:
		print("Hay valores NaN en el dataset")
		raise


#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
#basado en el conteo de terminaciones
def procesa_tarea_terminaciones(id_base):
	#formar el dataframe con la info del histiroc
	df= create_gl_dataframe_terminaciones(id_base)

	# Chequear y visualizar NaNs en el DataFrame
	nan_count = check_nans(df)

	#si no hay valores nulos se procede a realizar la prediccion
	if nan_count == 0:
		# proceso encargado de ejecutar los modelos de prediccion basados en conteo de terminaciones
		procesa_subtarea_terminaciones(id_base, df)
	else:
		print("Hay valores NaN en el dataset")
		raise


#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
#basado en un dataframe generico
def procesa_tarea_genetic(id_base: int):
	for gen_id in range (1,3):

		# Select options based on position
		generic_options = {
			1: 'FR_GEN',
			2: 'LT_GEN'
		}

		generic_option_name = str(gen_id) + '.' + generic_options.get(gen_id)
		if not generic_option_name:
			raise ValueError("Posición no válida. Debe estar entre 1 y 2.")

		#formar el dataframe con la info del histiroc
		df_generic= create_gl_dataframe_generic(gen_id)

		# Chequear y visualizar NaNs en el DataFrame
		nan_count = check_nans(df_generic)
		
		#si no hay valores nulos se procede a realizar la prediccion
		if nan_count == 0:
			# proceso encargado de ejecutar los modelos de prediccion basados en entradas genericas
			procesa_subtarea_generic(id_base, df_generic, generic_option_name)
		else:
			print("Hay valores NaN en el dataset")
			raise


#funcion principal
def main():
	#recupera el maximo ID del sorteo que se va a jugar
	id_base = qry_id_base()

	# proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	procesa_tarea(id_base)

	# proceso encargado de ejecutar el modelo de prediccion para el label preferencia_flag
	procesa_tarea_sorteo_base(id_base)

	#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	#basado en primos, impares y pares
	procesa_tarea_pip(id_base)

	#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	#basado en el conteo de terminaciones
	procesa_tarea_terminaciones(id_base)

	#proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	#basado en un dataframe generico
	procesa_tarea_genetic(id_base)

if __name__ == "__main__":
	main()