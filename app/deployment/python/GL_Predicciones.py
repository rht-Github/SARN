import pandas as pd
import numpy as np
import joblib
import os
import sys
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import xgboost as xgb
import gl_dataset_handler as dh
from sklearn.preprocessing import LabelEncoder
import warnings

# Suprimir todas las advertencias (no recomendado a menos que sepas lo que estás haciendo)
warnings.filterwarnings("ignore")

GUARDA_PREDICCION = False

class GL_Predicciones:

    def __init__(self, num_preds=3, mostrar_importancias="N"):
        try:
            self.connection = dh.connect_to_oracle()
            self.df_resultados = dh.qry_resultados(self.connection)
            self.pn_id = dh.qry_id_base()
            self.df_opciones = dh.qry_opciones(self.connection, self.pn_id)
            self.num_preds = num_preds
            self.mostrar_importancias = mostrar_importancias.upper()
            self.predicciones = {}
            self.label_encoders = {}
            self.df_resultados.columns = [col.upper() for col in self.df_resultados.columns]
            self.df_opciones.columns = [col.upper() for col in self.df_opciones.columns]
        except Exception as e:
            print(f"[ERROR __init__] {e}")

    def entrenar_modelo_por_posicion(self, posicion, modelo_tipo):
        try:
            campos = [f"PN_{posicion}", f"NONE_{posicion}", f"PAR_{posicion}", f"M3_{posicion}",
                      f"M4_{posicion}", f"M5_{posicion}", f"M7_{posicion}", f"D_{posicion}"]

            campos = [col for col in campos if col in self.df_resultados.columns]

            if not campos or f"R_{posicion}" not in self.df_resultados.columns:
                raise ValueError(f"Columnas requeridas no encontradas para posicion {posicion}")

            X = self.df_resultados[campos]
            y_raw = self.df_resultados[f"R_{posicion}"].astype(str)

            le = LabelEncoder()
            y = le.fit_transform(y_raw)
            self.label_encoders[f"B{posicion}_{modelo_tipo}"] = le

            if modelo_tipo == "RandomForest":
                modelo = RandomForestClassifier(n_estimators=100, random_state=42)
            elif modelo_tipo == "XGBoost":
                modelo = xgb.XGBClassifier(use_label_encoder=False, eval_metric='mlogloss', random_state=42)
            else:
                raise ValueError(f"Modelo no soportado: {modelo_tipo}")

            modelo.fit(X, y)

            if self.mostrar_importancias == "Y":
                self.visualizar_importancias(modelo, campos, posicion, modelo_tipo)

            return modelo, campos

        except Exception as e:
            print(f"[ERROR entrenar_modelo_por_posicion - Posicion {posicion} - {modelo_tipo}] {e}")
            return None, []

    def visualizar_importancias(self, modelo, campos, posicion, modelo_tipo):
        try:
            importancias = modelo.feature_importances_
            df_importancias = pd.DataFrame({
                'Caracteristica': campos,
                'Importancia': importancias
            }).sort_values(by='Importancia', ascending=True)

            plt.figure(figsize=(8, 5))
            plt.barh(df_importancias['Caracteristica'], df_importancias['Importancia'])
            plt.title(f'Importancia de características - Posición B{posicion} - {modelo_tipo}')
            plt.xlabel('Importancia')
            plt.tight_layout()
            plt.show()

        except Exception as e:
            print(f"[ERROR visualizar_importancias - Posicion {posicion} - {modelo_tipo}] {e}")

    def predecir_por_posicion(self, modelo, campos_entrenamiento, posicion, modelo_nombre):
        try:
            btype = f"B{posicion}"
            df_posicion = self.df_opciones[self.df_opciones["B_TYPE"] == btype].copy()

            campos_uso = [c for c in ["PN", "NONE", "PAR", "M3", "M4", "M5", "M7", "D"] if c in df_posicion.columns]
            X = df_posicion[campos_uso]
            X.columns = campos_entrenamiento

            probabilidades = modelo.predict_proba(X)
            clases_codificadas = modelo.classes_
            le = self.label_encoders.get(f"{btype}_{modelo_nombre}")
            clases = le.inverse_transform(clases_codificadas)

            df_proba = pd.DataFrame(probabilidades, columns=clases)
            df_proba["R"] = df_posicion["R"].astype(str).values

            df_resultado = df_proba.melt(id_vars=["R"], var_name="Pred", value_name="Probabilidad")
            df_resultado = df_resultado[df_resultado["R"] == df_resultado["Pred"]]
            df_resultado = df_resultado.sort_values(by="Probabilidad", ascending=False)
            df_resultado = df_resultado.drop_duplicates(subset="R").head(self.num_preds)
            df_resultado = df_resultado[["R", "Probabilidad"]]
            df_resultado["Probabilidad"] = df_resultado["Probabilidad"].round(3)
            df_resultado["Posicion"] = btype
            df_resultado["Modelo"] = modelo_nombre

            clave = f"{btype}_{modelo_nombre}"
            self.predicciones[clave] = df_resultado.reset_index(drop=True)

        except Exception as e:
            print(f"[ERROR predecir_por_posicion - Posicion {posicion} - {modelo_nombre}] {e}")

    def validar_continuidad_ids(self):
        try:
            ultimo_id = self.df_resultados["ID"].max()
            if (self.pn_id + 1) - ultimo_id != 1:
                raise ValueError("No existe continuidad entre los valores del ID y NEXT_ID")
        except Exception as e:
            print(f"[ERROR validar_continuidad_ids] {e}")
            raise

    def exportar_resultados(self):
        try:
            if not self.predicciones:
                raise ValueError("No hay predicciones generadas para exportar.")

            now = datetime.now().strftime("%m%d%Y_%H%M%S")
            nombre_archivo = f"{self.pn_id + 1}_{now}_predicciones.xlsx"
            ruta = f"C:/Users/ricardo.huerta/Documents/melateRetro/AnalisisJugadas/ml_modelos_export/{nombre_archivo}"

            df_final = pd.concat(self.predicciones.values(), ignore_index=True)

            # Imprimir en pantalla R, Posicion y Modelo del df_final
            for _, row in df_final.iterrows():
                #print(f"R: {row['R']}, Posicion: {row['Posicion']}, Modelo: {row['Modelo']}")
                if GUARDA_PREDICCION:
                    dh.ejecutar_procedimiento_short(row['Modelo'], self.pn_id, "15.GL_PREDICCION", row['R'], round(row['Probabilidad'],3), row['Posicion'])

            with pd.ExcelWriter(ruta) as writer:
                df_final.to_excel(writer, sheet_name="Predicciones", index=False)

            print(f"Predicciones exportadas en: {ruta}")

        except Exception as e:
            print(f"[ERROR exportar_resultados] {e}")

    def ejecutar(self):
        try:
            self.validar_continuidad_ids()
            modelos = ["RandomForest", "XGBoost"]
            for modelo_nombre in modelos:
                for i in range(1, 7):
                    modelo, campos_entrenamiento = self.entrenar_modelo_por_posicion(i, modelo_nombre)
                    if modelo:
                        self.predecir_por_posicion(modelo, campos_entrenamiento, i, modelo_nombre)
            self.exportar_resultados()

        except Exception as e:
            print(f"[ERROR ejecutar] {e}")


if __name__ == "__main__":
    """
    if len(sys.argv) != 3:
        print("Uso: python script.py <num_preds> <mostrar_importancias(Y/N)>")
        sys.exit(1)

    num_preds = int(sys.argv[1])
    mostrar_importancias = sys.argv[2]
    """
    num_preds = 2
    mostrar_importancias = "N"

    predictor = GL_Predicciones(num_preds=num_preds, mostrar_importancias=mostrar_importancias)
    predictor.ejecutar()