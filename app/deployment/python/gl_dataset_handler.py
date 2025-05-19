import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

#valores contantes
DB_USER = 'olap_sys'
DB_PWD = 'Ingenier1a'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SERVICE = 'lcl'

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


# Crear el dataframe con la info del histórico de los sorteos basado en primos, impares y pares
def create_dataframe(sorteo_id: int = 0):
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
            query_stmt = "SELECT SEQ"
            query_stmt = query_stmt + ", ID"
            query_stmt = query_stmt + ", B_TYPE"
            query_stmt = query_stmt + ", DECENA"
            query_stmt = query_stmt + ", POS"
            query_stmt = query_stmt + ", J_CNT"
            query_stmt = query_stmt + ", J_CNT_FLAG"
            query_stmt = query_stmt + ", R_CNT"
            query_stmt = query_stmt + ", R_CNT_FLAG"
            query_stmt = query_stmt + ", SORTEO_ID"
            query_stmt = query_stmt + ", DIF"
            query_stmt = query_stmt + ", DIF_FLAG"
            query_stmt = query_stmt + ", DIGIT_TYPE"
            query_stmt = query_stmt + ", COLOR_FR"
            query_stmt = query_stmt + ", COLOR_LT"
            query_stmt = query_stmt + ", CHNG"
            query_stmt = query_stmt + " FROM OLAP_SYS.GL_POSITION_COUNTS_V"
            query_stmt = query_stmt + " WHERE ID = " + str(sorteo_id)
            query_stmt = query_stmt + " AND DIF_FLAG = 1"
            cursor = conn.cursor()
            cursor.execute(query_stmt)
            # Convirtiendo el resultset en un DataFrame de Pandas
            columns = ['SEQ', 'ID', 'B_TYPE', 'DECENA', 'POS', 'J_CNT', 'J_CNT_FLAG', 'R_CNT', 'R_CNT_FLAG', 'SORTEO_ID'
				     , 'DIF', 'DIF_FLAG', 'DIGIT_TYPE', 'COLOR_FR', 'COLOR_LT', 'CHNG']  # Nombres de columnas
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            return df
        except Exception as err:
            print('Exception raised while executing the query', err)
            return None
        finally:
            cursor.close()  # Asegurarse de cerrar el cursor
    finally:
        conn.close()  # Cerrar la conexión


#ejecutar procedimiento de base de datos para guardar la info de las predicciones
def ejecutar_procedimiento(prediccion_info, ejecucion_tipo:str="short"):

	try:
		# conectando a la base de datos
		str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
		conn = cx_Oracle.connect(str_conn)
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			cursor = conn.cursor()

			if ejecucion_tipo == "short":
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
			else:
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
																				  prediccion_info["precision_6"],
																				  prediccion_info["siguiente_sorteo_7"],
																				  prediccion_info["prediccion_7"],
																				  prediccion_info["precision_7"],
																				  prediccion_info["siguiente_sorteo_8"],
																				  prediccion_info["prediccion_8"],
																				  prediccion_info["precision_8"],
																				  prediccion_info["siguiente_sorteo_9"],
																				  prediccion_info["prediccion_9"],
																				  prediccion_info["precision_9"],
																				  prediccion_info["siguiente_sorteo_0"],
																				  prediccion_info["prediccion_0"],
																				  prediccion_info["precision_0"]])
		except Exception as err:
			print('Exception raised while executing the procedure', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()


#ejecutar procedimiento de base de datos para guardar la info de las predicciones
def ejecutar_procedimiento_array(prediccion_info):

	try:
		# conectando a la base de datos
		str_conn = DB_USER + "/" + DB_PWD + "@//" + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE
		conn = cx_Oracle.connect(str_conn)
	except Exception as err:
		print('Exception while creating a oracle connection', err)
	else:
		try:
			cursor = conn.cursor()

			for idx in prediccion_info:
				#print(idx[0]+"-"+str(idx[1])+"-"+idx[2]+"-"+str(idx[4])+"-"+str(idx[3])+"-"+str(idx[5]))
				# Llamar al procedimiento almacenado con dos parámetros de tipo cadena

				cursor.callproc('W_GL_AUTOMATICAS_PKG.PREDICCIONES_ALL_HANDLER', [idx[0],#nombre_algoritmo
																				  idx[1],#prediccion_sorteo
																				  idx[2],#prediccion_tipo
																				  int(idx[4]),#siguiente_sorteo_1
																				  str(idx[3]),#prediccion_1
																				  float(idx[5])])#precision_1

		except Exception as err:
			print('Exception raised while executing the procedure', err)
		finally:
			# Cerrar el cursor
			cursor.close()
	finally:
		# Cerrar la conexion
		conn.close()
