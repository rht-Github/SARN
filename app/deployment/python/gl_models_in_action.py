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
import warnings

#valores contantes
DB_USER = 'olap_sys'
DB_PWD = 'Ingenier1a'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'lcl'
SORTEO_BASE = 896
EPOCHS = 13
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
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 1 AND INPAR_NUMBER_FLAG IN (0,1) THEN 1 ELSE 0 END PRIMO"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 1 THEN 1 ELSE 0 END IMPAR"
			query_stmt = query_stmt + " , CASE WHEN PRIME_NUMBER_FLAG = 0 AND INPAR_NUMBER_FLAG = 0 THEN 1 ELSE 0 END PAR"
			query_stmt = query_stmt + " , CASE WHEN CHNG_POSICION IS NULL THEN 0 ELSE 1 END CHNG"
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
				query_stmt = query_stmt + " SELECT ID, B_TYPE, DIGIT, LT, FR, CA, PXC, PRIMO, IMPAR, PAR, CHNG"
				query_stmt = query_stmt + " , CASE WHEN PXC = 0 AND PREF = 0 THEN 0"
				query_stmt = query_stmt + " WHEN PXC = 0 AND PREF = 1 THEN 1"
				query_stmt = query_stmt + " WHEN PXC = 1 AND PREF = 0 THEN 2"
				query_stmt = query_stmt + " WHEN PXC = 1 AND PREF = 1 THEN 3 END PXC_PREF"
			else:
				query_stmt = query_stmt + " SELECT ID, B_TYPE, DIGIT, LT, FR, CA, PXC, PRIMO, IMPAR, PAR, CHNG"
			query_stmt = query_stmt + " FROM GIGA_TBL"

			cursor = conn.cursor()
			cursor.execute(query_stmt)
			# Convirtiendo el resultset en un DataFrame de Pandas
			if sorteo_base > 0:
				columns = ['ID', 'B_TYPE', 'DIGIT', 'LT', 'FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR','CHNG','PXC_PREF']  # Nombres de columnas
			else:
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "2." + label
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


#prediccion ley del tercio
def prediccion_lt_2(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
		feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the current instance (e.g., latest record)
		X_latest = X_test[-1:]  # Assuming you want the prediction for the last test instance
		valor_prediccion_prob = model.predict(X_latest)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[
								0] + 1  # Add 1 to match original class labels (1, 2, 3)

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
		print(f"Error in prediccion_lt: {e}")
	return None


#prediccion ley del tercio en base a torch
def prediccion_lt_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
    try:
        siguiente_sorteo = df['ID'].max() + 1

        # Define feature columns
        feature_columns = ['FR', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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

            print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

        # Evaluate the model on test data
        model.eval()
        with torch.no_grad():
            y_pred_prob = model(X_test_tensor)
            y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

        # Calculate accuracy
        precision_score = accuracy_score(y_test, y_pred)
        print(f"Model Accuracy: {precision_score * 100:.2f}%")

        # Single prediction for the latest record in the dataset
        X_latest = X_test_tensor[-1].unsqueeze(0)  # Assuming you want the prediction for the last test instance
        with torch.no_grad():
            valor_prediccion_prob = model(X_latest)
            valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1  # Add 1 to match original labels (1, 2, 3)

        print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
        print(f"Error in prediccion_lt: {e}")
        return None


#prediccion frecuencia
def prediccion_fr(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# print(f"entrena_modelos {label}, {features}")
		#print(df.count())
		#siguiente_sorteo = sorteo_id
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separar los features y el label (FR)
		feature_columns = ['CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'IMPAR']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "1." + label
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
def prediccion_fr_2(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the current instance (e.g., latest record)
		X_latest = X_test[-1:]  # Assuming you want the prediction for the last test instance
		valor_prediccion_prob = model.predict(X_latest)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[
								0] + 1  # Add 1 to match original class labels (1, 2, 3)

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
		print(f"Error in prediccion_lt: {e}")
	return None


#prediccion de frecuencia en base a torch
def prediccion_fr_torch(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo, epochs=EPOCHS):
	try:
		siguiente_sorteo = df['ID'].max() + 1

		# Define feature columns
		feature_columns = ['LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG', 'B_TYPE', 'DIGIT']

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
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		X_latest = X_test_tensor[-1].unsqueeze(0)  # Assuming you want the prediction for the last test instance
		with torch.no_grad():
			valor_prediccion_prob = model(X_latest)
			valor_prediccion = torch.argmax(valor_prediccion_prob, dim=1).item() + 1  # Add 1 to match original labels (1, 2, 3)

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
		print(f"Error in prediccion_lt: {e}")
		return None


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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "3." + label
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


#prediccion de numeros primos en base a tensorflow
def prediccion_primo_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32)

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
				# Forward pass
				outputs = model(batch_X).squeeze()  # Remove extra dimension
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor).squeeze()  # Squeeze to remove extra dimension
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy())
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

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
		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "5." + label
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


#prediccion de numeros impares en base a tensorflow
def prediccion_impar_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32)

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
				# Forward pass
				outputs = model(batch_X).squeeze()  # Remove extra dimension
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor).squeeze()  # Squeeze to remove extra dimension
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy())
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

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
		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "4." + label
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


#prediccion de numeros pares en base a tensorflow
def prediccion_par_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'CHNG']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32)

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
				# Forward pass
				outputs = model(batch_X).squeeze()  # Remove extra dimension
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor).squeeze()  # Squeeze to remove extra dimension
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy())
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

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
		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "6." + label
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


#prediccion de numeros con cambio en base a tensorflow
def prediccion_change_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR']
		X = df[feature_columns].values  # Features
		y = df[label].values  # Label (expected values 0 or 1)

		# Preprocess data: scale numeric features
		scaler = StandardScaler()
		X = scaler.fit_transform(X)

		# Split data into training and test sets
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

		# Convert data to PyTorch tensors
		X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
		y_train_tensor = torch.tensor(y_train, dtype=torch.float32)
		X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
		y_test_tensor = torch.tensor(y_test, dtype=torch.float32)

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
				# Forward pass
				outputs = model(batch_X).squeeze()  # Remove extra dimension
				loss = criterion(outputs, batch_y)

			# Backward pass and optimization
			optimizer.zero_grad()
			loss.backward()
			optimizer.step()

		print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on the test set
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor).squeeze()  # Squeeze to remove extra dimension
			y_pred = (y_pred_prob > 0.5).float()  # Convert probabilities to binary predictions

		# Calculate accuracy
		precision_score = accuracy_score(y_test, y_pred.numpy())
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

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
		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		return prediccion_dic

	except ValueError as ve:
		print(f"Error de valor: {ve}")
		raise
	except Exception as e:
		print(f"Ocurrió un error durante el procesamiento: {e}")
		raise


#prediccion de digitos
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "7." + label
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


#prediccion de digitos en base a tensorflow
def prediccion_digit_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
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
		print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		nuevo_registro = X_test[-1:]  # Last row for prediction
		single_prediction_prob = model.predict(nuevo_registro)
		valor_prediccion = single_prediction_prob.argmax(axis=1)[0] + 1  # Add 1 to match original range (1 to 39)

		# Print or return the single prediction
		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, valor_prediccion: {valor_prediccion}")

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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
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

			print(f"Epoch [{epoch + 1}/{epochs}], Loss: {loss.item():.4f}")

		# Evaluate the model on test data
		model.eval()
		with torch.no_grad():
			y_pred_prob = model(X_test_tensor)
			y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

		# Compute accuracy
		precision_score = accuracy_score(y_test, y_pred)
		print(f"Test Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset
		nuevo_registro = X_test_tensor[-1].unsqueeze(0)  # Last row for prediction
		with torch.no_grad():
			single_prediction_prob = model(nuevo_registro)
			valor_prediccion = torch.argmax(single_prediction_prob,
											dim=1).item() + 1  # Add 1 to match original range (1 to 39)

		# Print or return the single prediction
		print(
			f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, valor_prediccion: {valor_prediccion}")

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
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'PAR', 'CHNG', 'IMPAR']
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
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
def prediccion_pxc_pref(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
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

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
		#print("Predicción de Logistic Regression:", prediccion_log_reg)
		#print("Predicción de Random Forest:", prediccion_rf)

		prediccion_dic['nombre_algoritmo'] = nombre_algoritmo
		prediccion_dic["prediccion_tipo"] = "9." + label
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


#prediccion de combinacion de pronostico por ciclo junto con numeros favorables en base a tensorflow
def prediccion_pxc_pref_tf(df, label, sorteo_id, posicion, prediccion_dic, nombre_algoritmo):
	try:
		# Determine the next draw number
		siguiente_sorteo = df['ID'].max() + 1

		# 1. Separate features and label
		feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
		X = df[feature_columns]  # Features
		y = df[label]  # Label (expected values 0, 1, 2, or 3)

		# Preprocessing pipeline: scale numeric features
		numeric_features = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
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
		print(f"Model Accuracy: {precision_score * 100:.2f}%")

		# Single prediction for the latest record in the dataset (similar to what was done in the original function)
		nuevo_registro = X_test[-1:]  # Assuming you want to predict for the last instance in the test set
		valor_prediccion_prob = model.predict(nuevo_registro)
		valor_prediccion = valor_prediccion_prob.argmax(axis=1)[0]  # Get the predicted class (0, 1, 2, or 3)

		# Print or return the single prediction
		print(f"Predicción para el registro más reciente: {valor_prediccion}")

		print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")

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
        feature_columns = ['FR', 'LT', 'CA', 'PXC', 'PRIMO', 'IMPAR', 'PAR', 'CHNG']
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

            print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

        # Evaluate the model on the test set
        model.eval()
        with torch.no_grad():
            y_pred_prob = model(X_test_tensor)
            y_pred = torch.argmax(y_pred_prob, dim=1).numpy()

        # Calculate accuracy
        precision_score = accuracy_score(y_test, y_pred)
        print(f"Model Accuracy: {precision_score * 100:.2f}%")

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
        print(f"nombre_algoritmo: {nombre_algoritmo}, siguiente_sorteo: {siguiente_sorteo}, {valor_prediccion}")
        return prediccion_dic

    except Exception as e:
        print(f"Error in prediccion_lt: {e}")
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
		elif label == 'PXC_PREF':
			prediccion_pxc_pref(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)
		elif label == 'PXC':
			prediccion_pxc(df_b_type, label, id_base, b_type_id, prediccion_gl, nombre_algoritmo)

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		ejecutar_procedimiento(prediccion_gl)


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
		ejecutar_procedimiento(prediccion_gl)


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

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		ejecutar_procedimiento(prediccion_gl)


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

		#ya que estan completas las predicciones de la jugada se imprimen
		if b_type_id == 6:
			print(prediccion_gl)

	if GUARDA_PREDICCION:
		# ejecutar procedimiento de base de datos para guardar la info de las predicciones
		ejecutar_procedimiento(prediccion_gl)


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

	#pronostico por ciclo
	label = 'PXC'
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


#funcion principal
def main():
	#recupera el maximo ID del sorteo que se va a jugar
	id_base = qry_id_base()
	#id_base = 1000

	# proceso encargado de ejecutar los modelos de prediccion basados en log_reg, rf
	procesa_tarea(id_base)
	# proceso encargado de ejecutar el modelo de prediccion para el label preferencia_flag
	procesa_tarea_sorteo_base(id_base)




if __name__ == "__main__":
	main()