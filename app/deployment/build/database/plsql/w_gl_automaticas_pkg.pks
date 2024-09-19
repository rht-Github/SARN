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
     , nvl(preferencia_flag,'.') pref
	 , decode(CHNG_POSICION,null,'.','C') chg 
	 , nvl(winner_flag,'N') winner
     , case when olap_sys.w_common_pkg.is_prime_number(digit) = 1 then 0 else 
       case when mod(digit,2) = 0 then 2 else 
       case when mod(digit,2) > 0 then 1 end end end primo_impar_par	 
  from olap_sys.s_calculo_stats
 where drawing_id = pn_drawing_id
 order by b_type, digit;


cursor c_automaticas_header is
select list_id
  from olap_sys.gl_automaticas_header ah
 where stop_date is null; 

					 
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
								   , pv_ca_comb_flag    varchar2 default 'Y');

--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure aciertos_repetidos_handler(pn_drawing_id		number);

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
								 , pn_sig_sorteo2           number
								 , pv_pred2					varchar2
								 , pf_pres2					float
								 , pn_sig_sorteo3           number
								 , pv_pred3					varchar2
								 , pf_pres3					float
								 , pn_sig_sorteo4           number
								 , pv_pred4					varchar2
								 , pf_pres4					float
								 , pn_sig_sorteo5           number
								 , pv_pred5					varchar2
								 , pf_pres5					float
								 , pn_sig_sorteo6           number
								 , pv_pred6					varchar2
								 , pf_pres6					float);									   
end w_gl_automaticas_pkg;
/
show errors;