# MR_Predecir_Digito.py
# Requisitos: pip install cx_Oracle pandas scikit-learn openpyxl python-dateutil
import os
import math
import argparse
from datetime import datetime
from typing import Dict, List, Tuple

import cx_Oracle
import numpy as np
import pandas as pd
from dateutil import tz
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier


class MR_Predecir_Digito:
    """
    Predice los dígitos (1..39) por posición (B1..B6) para el siguiente sorteo.
    - Entrena 2 modelos por posición: LogisticRegression y RandomForest
    - Genera top-N candidatos por modelo
    - Exporta resultados a Excel con nombre SORTEO_ID_MMDDYYYY_HHMISS_digito.xlsx
    """

    # Features esperadas según el diccionario de datos
    BASE_FEATURES = [
        "PRIMO", "IMPAR", "PAR",
        "MULTIPLO_3", "MULTIPLO_4", "MULTIPLO_5", "MULTIPLO_7",
        "DECENA_1", "DECENA_2", "DECENA_3", "DECENA_4",
        "FRECUENCIA_1", "FRECUENCIA_2", "FRECUENCIA_3",
        "LEY_TERCIO_1", "LEY_TERCIO_2", "LEY_TERCIO_3",
        "ARRIBA_AVG_1", "ABAJO_AVG_2",
        "FAVORABLE", "POSICION_CAMBIO"
    ]

    def __init__(
        self,
        dsn_host: str,
        dsn_port: str,
        service_name: str,
        user: str,
        password: str,
        export_dir: str,
        top_n: int = 2,
        freq_window: int = 100,
        tz_name: str = "America/Merida",
        sorteo_id_next: int = None
    ):
        self.dsn_host = dsn_host
        self.dsn_port = dsn_port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.export_dir = export_dir
        self.top_n = int(top_n)
        self.freq_window = int(freq_window)
        self.tz_name = tz_name
        self.sorteo_id_next = sorteo_id_next  # si None, se calcula como max+1

        # modelos por B_TYPE: { 'B1': {'LogisticRegression': (model, cols), 'RandomForest': (model, cols)}, ... }
        self.models_by_pos: Dict[str, Dict[str, Tuple[object, List[str]]]] = {}

    # -----------------------------
    # Conexión y carga de datos
    # -----------------------------
    def _connect(self):
        try:
            dsn = cx_Oracle.makedsn(self.dsn_host, self.dsn_port, service_name=self.service_name)
            conn = cx_Oracle.connect(user=self.user, password=self.password, dsn=dsn)
            return conn
        except Exception as e:
            raise RuntimeError(f"Error conectando a Oracle: {e}")

    def load_historico(self) -> pd.DataFrame:
        """Carga todo el histórico desde OLAP_SYS.MR_HISTORICO"""
        try:
            conn = self._connect()
            query = "SELECT * FROM OLAP_SYS.MR_HISTORICO_V ORDER BY SORTEO_ID"
            df = pd.read_sql(query, con=conn)
            conn.close()
            # Filtrado básico de integridad
            if "B_TYPE" not in df.columns or "DIGITO" not in df.columns or "SORTEO_ID" not in df.columns:
                raise ValueError("El histórico no contiene columnas mínimas: B_TYPE, DIGITO, SORTEO_ID")
            # Nos quedamos con dígitos válidos
            df = df.copy()
            df = df[df["DIGITO"].between(1, 39)]
            return df
        except Exception as e:
            raise RuntimeError(f"Error cargando histórico: {e}")

    # -----------------------------
    # Utilidades de features
    # -----------------------------
    @staticmethod
    def _is_prime(n: int) -> bool:
        if n < 2:
            return False
        if n % 2 == 0:
            return n == 2
        r = int(math.sqrt(n))
        for i in range(3, r + 1, 2):
            if n % i == 0:
                return False
        return True

    @staticmethod
    def _decena_flags(n: int) -> Tuple[int, int, int, int]:
        # 1–9, 10–19, 20–29, 30–39
        return (
            1 if 1 <= n <= 9 else 0,
            1 if 10 <= n <= 19 else 0,
            1 if 20 <= n <= 29 else 0,
            1 if 30 <= n <= 39 else 0,
        )

    @staticmethod
    def _mult_flags(n: int) -> Tuple[int, int, int, int]:
        return (1 if n % 3 == 0 else 0,
                1 if n % 4 == 0 else 0,
                1 if n % 5 == 0 else 0,
                1 if n % 7 == 0 else 0)

    def _candidate_table(self, df_pos: pd.DataFrame) -> pd.DataFrame:
        """
        Construye una tabla de candidatos 1..39 para una posición dada (df_pos = histórico de esa B_TYPE),
        calculando FRECUENCIA_1/2/3, LEY_TERCIO_1/2/3 y ARRIBA_AVG_1/ABAJO_AVG_2 basadas en una ventana móvil.
        """
        try:
            # Ventana reciente
            df_sorted = df_pos.sort_values("SORTEO_ID")
            win = df_sorted.tail(self.freq_window) if len(df_sorted) > self.freq_window else df_sorted

            # Conteos por dígito en la ventana
            counts = win["DIGITO"].value_counts().reindex(range(1, 40), fill_value=0)

            # Terciles para FRECUENCIA_1/2/3
            q33 = counts.quantile(1/3.0)
            q66 = counts.quantile(2/3.0)

            # Recencia (gap desde última aparición)
            last_sid = df_sorted["SORTEO_ID"].max() if len(df_sorted) else 0
            last_seen = (
                df_sorted.drop_duplicates(subset=["DIGITO"], keep="last")[["DIGITO", "SORTEO_ID"]]
                .set_index("DIGITO")["SORTEO_ID"]
                .reindex(range(1, 40), fill_value=np.nan)
            )

            gaps = last_sid - last_seen
            # Para dígitos nunca vistos, gap = ventana + 1 (muy "atrasados")
            gaps = gaps.fillna(self.freq_window + 1)
            # Promedio de gaps entre dígitos vistos (evita sesgos de NaN)
            avg_gap = gaps.replace(np.inf, np.nan).mean()

            rows = []
            for d in range(1, 40):
                primo = 1 if self._is_prime(d) else 0
                impar = 1 if d % 2 == 1 else 0
                par = 1 - impar
                m3, m4, m5, m7 = self._mult_flags(d)
                dec1, dec2, dec3, dec4 = self._decena_flags(d)

                # FRECUENCIAS
                c = counts.loc[d]
                f1 = 1 if c >= q66 and c > 0 else 0      # alta
                f2 = 1 if (c >= q33) and (c < q66) else 0 # media
                f3 = 1 if c < q33 else 0                  # baja

                # LEY DEL TERCIO
                lt1 = 1 if c > 1 else 0   # más de una vez
                lt2 = 1 if c == 1 else 0  # una vez
                lt3 = 1 if c == 0 else 0  # ninguna

                # ARRIBA/ABAJO PROMEDIO (gap)
                g = gaps.loc[d]
                arriba = 1 if g >= avg_gap else 0
                abajo = 1 - arriba

                rows.append({
                    "DIGITO": d,
                    "PRIMO": primo, "IMPAR": impar, "PAR": par,
                    "MULTIPLO_3": m3, "MULTIPLO_4": m4, "MULTIPLO_5": m5, "MULTIPLO_7": m7,
                    "DECENA_1": dec1, "DECENA_2": dec2, "DECENA_3": dec3, "DECENA_4": dec4,
                    "FRECUENCIA_1": f1, "FRECUENCIA_2": f2, "FRECUENCIA_3": f3,
                    "LEY_TERCIO_1": lt1, "LEY_TERCIO_2": lt2, "LEY_TERCIO_3": lt3,
                    "ARRIBA_AVG_1": arriba, "ABAJO_AVG_2": abajo,
                    # Señales que requieren fuentes externas/etiquetado manual: por ahora 0
                    "FAVORABLE": 0, "POSICION_CAMBIO": 0
                })
            cand = pd.DataFrame(rows)
            return cand
        except Exception as e:
            raise RuntimeError(f"Error construyendo tabla de candidatos: {e}")

    # -----------------------------
    # Entrenamiento
    # -----------------------------
    def _get_feature_cols(self, df_any: pd.DataFrame) -> List[str]:
        cols = [c for c in self.BASE_FEATURES if c in df_any.columns]
        if not cols:
            raise ValueError("No se encontraron columnas de features esperadas en el histórico.")
        return cols

    def train_models(self, df_hist: pd.DataFrame):
        """Entrena LR y RF por B_TYPE."""
        try:
            self.models_by_pos = {}
            feat_cols = self._get_feature_cols(df_hist)
            for b in sorted(df_hist["B_TYPE"].dropna().unique()):
                dfb = df_hist[df_hist["B_TYPE"] == b].copy()
                dfb = dfb[dfb["DIGITO"].between(1, 39)]
                if dfb.empty:
                    continue

                X = dfb[feat_cols].astype(float).values
                y = dfb["DIGITO"].astype(int).values

                # Modelo 1: Logistic Regression (multinomial)
                lr = LogisticRegression(
                    multi_class="multinomial", solver="lbfgs", max_iter=2000, random_state=42
                )
                lr.fit(X, y)

                # Modelo 2: Random Forest
                rf = RandomForestClassifier(n_estimators=600, random_state=42, n_jobs=-1)
                rf.fit(X, y)

                self.models_by_pos[b] = {
                    "LogisticRegression": (lr, feat_cols),
                    "RandomForest": (rf, feat_cols),
                }
        except Exception as e:
            raise RuntimeError(f"Error entrenando modelos: {e}")

    # -----------------------------
    # Predicción
    # -----------------------------
    @staticmethod
    def _extract_self_prob(model, classes_: np.ndarray, probs: np.ndarray, digits: np.ndarray) -> np.ndarray:
        """Devuelve, por fila i, la probabilidad de la clase igual al propio dígito 'digits[i]'. Si no existe, 0."""
        # Mapa clase -> índice
        idx_map = {int(c): j for j, c in enumerate(classes_)}
        out = np.zeros(len(digits), dtype=float)
        for i, d in enumerate(digits):
            j = idx_map.get(int(d), None)
            out[i] = probs[i, j] if j is not None else 0.0
        return out

    def generate_predictions(self, df_hist: pd.DataFrame, sorteo_id_next: int) -> pd.DataFrame:
        """
        Para cada B_TYPE:
          - Construye candidatos 1..39
          - Puntúa con cada modelo
          - Toma top-N por modelo
        """
        try:
            rows = []
            for b, model_pack in self.models_by_pos.items():
                df_pos = df_hist[df_hist["B_TYPE"] == b].copy()
                if df_pos.empty:
                    continue

                cand = self._candidate_table(df_pos)
                # Por cada modelo
                for model_name, (model, feat_cols) in model_pack.items():
                    # Alinea columnas (faltantes -> 0)
                    X_c = cand.copy()
                    for c in feat_cols:
                        if c not in X_c.columns:
                            X_c[c] = 0
                    X_c = X_c[feat_cols].astype(float).values

                    probs = model.predict_proba(X_c)
                    self_probs = self._extract_self_prob(model, model.classes_, probs, cand["DIGITO"].values)

                    cand_scored = cand[["DIGITO"]].copy()
                    cand_scored["SCORE"] = self_probs
                    cand_scored.sort_values("SCORE", ascending=False, inplace=True)
                    top = cand_scored.head(self.top_n)

                    for _, r in top.iterrows():
                        rows.append({
                            "SORTEO_ID": int(sorteo_id_next),
                            "B_TYPE": b,
                            "PREDICCION": int(r["DIGITO"]),
                            "ACCURACY": float(round(r["SCORE"], 3)),
                            "NOMBRE_MODELO": model_name
                        })

            # Orden de salida
            out = pd.DataFrame(rows)
            if not out.empty:
                out = out.sort_values(["B_TYPE", "NOMBRE_MODELO", "ACCURACY"], ascending=[True, True, False])
            return out
        except Exception as e:
            raise RuntimeError(f"Error generando predicciones: {e}")

    # -----------------------------
    # Exportación
    # -----------------------------
    def export_excel(self, df_out: pd.DataFrame, sorteo_id_next: int) -> str:
        """Exporta a la ruta pedida con nombre SORTEO_ID_MMDDYYYY_HHMISS_digito.xlsx"""
        try:
            os.makedirs(self.export_dir, exist_ok=True)
            # Hora local MX (Mérida)
            tzinfo = tz.gettz(self.tz_name)
            now_local = datetime.now(tzinfo)
            ts = now_local.strftime("%m%d%Y_%H%M%S")
            fname = f"{sorteo_id_next}_{ts}_digito.xlsx"
            fpath = os.path.join(self.export_dir, fname)

            # Orden y tipos
            cols = ["SORTEO_ID", "B_TYPE", "PREDICCION", "ACCURACY", "NOMBRE_MODELO"]
            for c in cols:
                if c not in df_out.columns:
                    df_out[c] = np.nan
            df_export = df_out[cols].copy()
            df_export["SORTEO_ID"] = df_export["SORTEO_ID"].astype(int)
            df_export["PREDICCION"] = df_export["PREDICCION"].astype(int)
            # ACCURACY ya viene redondeado a 3 decimales

            with pd.ExcelWriter(fpath, engine="openpyxl") as writer:
                df_export.to_excel(writer, sheet_name="predicciones", index=False)

            return fpath
        except Exception as e:
            raise RuntimeError(f"Error exportando Excel: {e}")

    # -----------------------------
    # Orquestación
    # -----------------------------
    def run(self) -> str:
        """Pipeline completo: carga, entrena, predice y exporta. Devuelve ruta del archivo Excel."""
        try:
            df_hist = self.load_historico()
            # Determinar siguiente SORTEO_ID si no se pasa explícito
            if self.sorteo_id_next is None:
                max_sid = int(df_hist["SORTEO_ID"].max()) if not df_hist.empty else 0
                self.sorteo_id_next = max_sid + 1

            self.train_models(df_hist)
            df_out = self.generate_predictions(df_hist, self.sorteo_id_next)
            if df_out.empty:
                raise RuntimeError("No se generaron predicciones (verifica que existan datos históricos suficientes).")
            fpath = self.export_excel(df_out, self.sorteo_id_next)
            return fpath
        except Exception as e:
            raise RuntimeError(f"Fallo en ejecución global: {e}")


# -----------------------------
# main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Predicción de dígitos por posición (MR_Predecir_Digito).")
    parser.add_argument("--host", required=True, help="Host de Oracle")
    parser.add_argument("--port", default="1521", help="Puerto de Oracle (defecto 1521)")
    parser.add_argument("--service", required=True, help="Service name de Oracle (por ej. lcl)")
    parser.add_argument("--user", required=True, help="Usuario de Oracle (por ej. OLAP_SYS)")
    parser.add_argument("--password", required=True, help="Password de Oracle")
    parser.add_argument("--export_dir", default=r"C:\Users\ricardo.huerta\Documents\melateRetro\AnalisisJugadas\ml_modelos_export",
                        help="Directorio de exportación")
    parser.add_argument("--top_n", type=int, default=2, help="Número de predicciones por modelo (defecto 2)")
    parser.add_argument("--freq_window", type=int, default=100, help="Ventana de frecuencia/recencia (defecto 100)")
    parser.add_argument("--tz", default="America/Merida", help="Zona horaria para el timestamp")
    parser.add_argument("--sorteo_id_next", type=int, default=None, help="SORTEO_ID del siguiente sorteo (opcional)")

    args = parser.parse_args()

    args = parser.parse_args()

    app = MR_Predecir_Digito(
        dsn_host=args.host,
        dsn_port=args.port,
        service_name=args.service,
        user=args.user,
        password=args.password,
        export_dir=args.export_dir,
        top_n=args.top_n,
        freq_window=args.freq_window,
        tz_name=args.tz,
        sorteo_id_next=args.sorteo_id_next
    )
    out_path = app.run()
    print(f"✅ Archivo generado: {out_path}")


if __name__ == "__main__":
    main()
