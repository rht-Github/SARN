create or replace package olap_sys.w_gl_automaticas_pkg as


--!cursor principal del proceso
cursor c_main (pn_drawing_id		number) is
select drawing_id id
	 , b_type
	 , digit
	 , color_ubicacion fre
	 , color_ley_tercio lt
	 , ciclo_aparicion ca
     , nvl(pronos_ciclo,-1) pxc
	 , decode(CHNG_POSICION,null,'.','C') chg 
	 , nvl(winner_flag,'N') winner
     , case when olap_sys.w_common_pkg.is_prime_number(digit) = 1 then 0 else 
       case when mod(digit,2) = 0 then 2 else 
       case when mod(digit,2) > 0 then 1 end end end primo_impar_par
	 , case when pronos_ciclo is null and preferencia_flag is null then 0
		    when pronos_ciclo is null and preferencia_flag is not null then 1
		    when pronos_ciclo is not null and preferencia_flag is null then 2
		    when pronos_ciclo is not null and preferencia_flag is not null then 3 end pxc_pref	
     , case when preferencia_flag is null then 0 else 1 end pref
     , case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 
       case when b_avg_ciclo_aparicion = 0 and a_avg_ciclo_aparicion = 1 then 'A' else '#' end end avg_ca_flag
  from olap_sys.s_calculo_stats
 where drawing_id = pn_drawing_id
 order by b_type, digit;

cursor c_automaticas_header is
select list_id
  from olap_sys.gl_automaticas_header ah
 where stop_date is null; 

--validacion de repetidos y aciertos
--para repetidos pv_resultado pasar las 6 posiciones ordenadas separadas por comas
--para aciertos pv_resultado pasar el valor de la posicion especifica
--pv_jugada siempre debe de pasar las 6 posiciones ordenadas separadas por comas
cursor c_valida_repetidos_aciertos (pv_resultado	varchar2
								  , pv_jugada	    varchar2) is
with jugadas_tbl as (
select pv_jugada str
  from dual
) --select * from resultado_n_1_tbl;
, resultado_tbl as (
select pv_resultado str
  from dual
) --select * from resultado_tbl;    
, intersect_tbl as (
select regexp_substr((select str from jugadas_tbl),'[^,]+',1,level) digit
				   from dual 
				 connect by level <= length((select str from jugadas_tbl))-length(replace((select str from jugadas_tbl),',',''))+1
intersect
select regexp_substr((select str from resultado_tbl),'[^,]+',1,level) digit
				   from dual 
				 connect by level <= length((select str from resultado_tbl))-length(replace((select str from resultado_tbl),',',''))+1
)
select count(1) inter_cnt
  from intersect_tbl;						 

cursor c_jugadas (pn_drawing_id		 number
                , pn_comb            number) is 
select id
	 , ia1||','||ia2||','||ia3||','||ia4||','||ia5||','||ia6 jugada
  from olap_sys.gl_automaticas_detail
 where sorteo_actual = pn_drawing_id
   and (ia5 = pn_comb or ia6 = pn_comb) ; 
	 
--!handler para insertar jugadas en la tabla GL_AUTOMATICAS_DETAIL
procedure ins_gl_automaticas_handler(pv_gambling_type		varchar2 default 'mrtr'
								   , pn_id					number
								   , pn_ia1					number
								   , pn_ia2					number
								   , pn_ia3					number
								   , pn_ia4					number
								   , pn_ia5					number
								   , pn_ia6					number
								   , pn_list_id             number);
		

--!handler para actualizar jugadas en la tabla GL_AUTOMATICAS_DETAIL
--!en base a la info de gigaloterias
procedure upd_gl_automaticas_handler(pn_drawing_id     number
								   , pv_ca_comb_flag    varchar2 default 'N');

--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure aciertos_repetidos_handler(pn_drawing_id		number
									 -- 0: execute all
									 -- 1: execute contar_repetidos
									 -- 2: execute contar_aciertos
								   , pv_execution_type  number default 0
								   , pv_execution_flag  varchar2 default 'SINGLE');

--!evaluar las predicciones de frecuencia y ley del tercio
procedure evaluate_prediccion_handler(pn_drawing_id               	number);

--!insertar metadatos de los porcentajes de las posiciones de c1, c3, c4 y c6 en la tabla plan_jugada_details
--!utilizar los metadatos para marcar como no optimas en la tabla de gl_automaticas_detail
procedure porcentaje_c1_c3_c4_c6_handler(pb_insert_flag			boolean default true
									   , pb_validator_flag		boolean default false
									   , pn_jpctaccum			number  default 80); 

--!inicializar el status = N en la tabla w_combination_responses_fs
--!aplicar filtros para seleccionar la franja con jugadas mas ganadoras en base a calculos hechos en Excel
--!y marcar status = Y para las jugadas que cumplan
procedure filtrar_jugadas_handler(pv_insert_flag   varchar2 default 'Y');

--!insertar las predicciones en la nueva tabla predicciones
procedure predicciones_all_handler(pv_nombre				varchar2
								 , pn_sorteo				number							 
								 , pv_tipo					varchar2
								 , pn_sig_sorteo1           number
								 , pv_pred1					varchar2
								 , pf_pres1					float
								 , pn_sig_sorteo2           number default 0
								 , pv_pred2					varchar2 default '#'
								 , pf_pres2					float default 0.0
								 , pn_sig_sorteo3           number default 0
								 , pv_pred3					varchar2 default '#'
								 , pf_pres3					float default 0.0
								 , pn_sig_sorteo4           number default 0
								 , pv_pred4					varchar2 default '#'
								 , pf_pres4					float default 0.0
								 , pn_sig_sorteo5           number default 0
								 , pv_pred5					varchar2 default '#'
								 , pf_pres5					float default 0.0
								 , pn_sig_sorteo6           number default 0
								 , pv_pred6					varchar2 default '#'
								 , pf_pres6					float default 0.0
								 , pn_sig_sorteo7           number default 0
								 , pv_pred7					varchar2 default '#'
								 , pf_pres7					float default 0.0
								 , pn_sig_sorteo8           number default 0
								 , pv_pred8					varchar2 default '#'
								 , pf_pres8					float default 0.0
								 , pn_sig_sorteo9           number default 0
								 , pv_pred9					varchar2 default '#'
								 , pf_pres9					float default 0.0
								 , pn_sig_sorteo0           number default 0
								 , pv_pred0					varchar2 default '#'
								 , pf_pres0					float default 0.0);		


--!insertar las predicciones en la nueva tabla predicciones
procedure predicciones_short_handler(pv_nombre				varchar2
								   , pn_sorteo				number							 
								   , pv_tipo				varchar2
								   , pn_sig_sorteo1         number
								   , pv_pred1				varchar2
								   , pf_pres1				float
								   , pv_b_type				varchar2);
								 
--!insertar conteo de los digitos acerca de jugadas y resultados
procedure digit_counts_handler(pn_drawing_id		number);								 

--!mostrar las predicciones para cada b_type para el ultimo sorteo
procedure comparativo_lt_handler;

--!insertar y actualizar el historico de digitos en base al id del sorteo
procedure history_digit_info_handler(pn_drawing_id		number);

--!en base a las predicciones de las terminaciones se actualizan las jugadas en gl_automaticas_detail
procedure upd_terminacion_cnt_handler(pn_drawing_id		number);

--!actualizar la tabla gl_position_counts con los datos del sorteo ganador
procedure upd_gl_pos_counts_handler(pn_drawing_id		number
							      , pn_comb1			number
							      , pn_comb2			number
							      , pn_comb3			number
							      , pn_comb4			number
							      , pn_comb5			number
							      , pn_comb6			number
								  , pf_percentile_ini   float default 0.15
								  , pf_percentile_end   float default 0.9);

--!insertar las probabilidades en la nueva tabla gl_automaticas_prob
procedure probabilidades_all_handler(pv_nombre_modelo	varchar2
								, pn_list_id		number
								, pn_id				number
								, pn_sorteo_actual	number
								, pn_ia1			number
								, pn_ia2			number
								, pn_ia3			number
								, pn_ia4			number
								, pn_ia5			number
								, pn_ia6			number
								, pf_digito_prob1	float default 0.0
								, pf_digito_prob2	float default 0.0
								, pf_digito_prob3	float default 0.0
								, pf_digito_prob4	float default 0.0
								, pf_digito_prob5	float default 0.0
								, pf_digito_prob6	float default 0.0);

--!actualizar la bandera para el LT ganador en la tabla s_gl_mapas_fre_lt_cnt
procedure upd_lt_handler (pn_drawing_id number);

--!calcular el conteo de matches para un patron definido
procedure conteo_patrones_handler(pv_patron_type		varchar2 default 'DIGIT_TYPE'
								, pn_rango_historico	number);

--!calcular y actualizar el promedio de ciclo de aparicion en base a b_type
--!para que se pueda utilizar como metrica para la seleccion de jjugadas
procedure upd_avg_gl_ca(pn_drawing_id		number);

--!en base al rango de historico identificar los patrones de las posicion 2-5
--!adicionalmente posicion 6
procedure get_patron_handler(pn_drawing_id		number default 0
                           , pn_rango_historico number default 100);

--!actualizar las estaditicas almacenadas en la tabla pm_decenas
procedure upd_pm_decenas_handler(pn_drawing_id		number
							   , pv_execution_type  varchar2 default 'S');

--!se marcan las jugadas de la posicion 2 a las 5 que contengan los digitos mas frecuentes
--!se marcan las jugadas de la posicion 6 que contengan los digitos mas frecuentes
procedure upd_digito_25_handler(pn_rango_historico	number default 100);

end w_gl_automaticas_pkg;
/
show errors;