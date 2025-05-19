import pandas as pd

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