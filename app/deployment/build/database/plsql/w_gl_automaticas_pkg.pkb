create or replace package body olap_sys.w_gl_automaticas_pkg as

--!contantes
CV$ACIERTO 			CONSTANT VARCHAR2(1) := 'Y';
CN$TOTAL_JUGADAS	CONSTANT NUMBER := 3262623;


--!variables globales
gn$upd_cnt			NUMBER := 0;
gn$ins_cnt			NUMBER := 0;



--!borrando el registro de la prediccion que se va a insertar
--!para permitir que el programa en python se pueda ajeuar multiples veces
procedure del_predicciones_all(pv_nombre			varchar2
						 , pn_prediccion_sorteo number
						 , pv_tipo				varchar2) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'del_predicciones_all';
begin

	--!borrando el registro de la prediccion que se va a insertar
	--!para permitir que el programa en python se pueda ajeuar multiples veces
	delete olap_sys.predicciones_all 
	 where prediccion_nombre = pv_nombre
	   and prediccion_sorteo = pn_prediccion_sorteo
	   and prediccion_tipo   = pv_tipo;

exception
  when others then
--	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end del_predicciones_all;


--!insertar un registro a la tabla GL_AUTOMATICAS_DETAIL
procedure ins_gl_automaticas_detail(pv_gambling_type		varchar2 default 'mrtr'
							      , pn_id					number
							      , pn_ia1					number
							      , pn_ia2					number
							      , pn_ia3					number
							      , pn_ia4					number
							      , pn_ia5					number
							      , pn_ia6					number
								  , pn_list_id              number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_gl_automaticas_detail';
	cursor c_main (pn_ia1		number
	             , pn_ia2		number
	             , pn_ia3		number
	             , pn_ia4		number
	             , pn_ia5		number
	             , pn_ia6		number) is
	select seq_id
		 , comb1
		 , comb2
		 , comb3
		 , comb4
		 , comb5
		 , comb6
		 , pn_cnt
		 , none_cnt
		 , par_cnt
		 , t1_cnt
		 , t2_cnt
		 , comb_sum
		 , d1
		 , d2
		 , d3
		 , d4
		 , d5
		 , d6
		 , d01_09
		 , d10_19
		 , d20_29
		 , d30_39
         , pn1
         , pn2
         , pn3
         , pn4
         , pn5
         , pn6
         , none1
         , none2
         , none3
         , none4
         , none5
         , none6
         , par1
         , par2
         , par3
         , par4
         , par5
         , par6
         , m3_1
         , m3_2
         , m3_3
         , m3_4
         , m3_5
         , m3_6
         , m3_cnt
         , m4_1
         , m4_2
         , m4_3
         , m4_4
         , m4_5
         , m4_6
         , m4_cnt
         , m5_1
         , m5_2
         , m5_3
         , m5_4
         , m5_5
         , m5_6
         , m5_cnt
         , m7_1
         , m7_2
         , m7_3
         , m7_4
         , m7_5
         , m7_6
         , m7_cnt	
         , top_digit_cnt
         , top_digit_history 
         , co_1
		 , co_2
		 , co_3
		 , co_4
		 , co_5
		 , co_cnt		 
	  from olap_sys.w_combination_responses_fs 
	 where comb1 = pn_ia1 
	   and comb2 = pn_ia2 
	   and comb3 = pn_ia3 
	   and comb4 = pn_ia4 
	   and comb5 = pn_ia5 
	   and comb6 = pn_ia6;	
begin							   
	
	for c in c_main (pn_ia1 => pn_ia1
				   , pn_ia2 => pn_ia2
				   , pn_ia3 => pn_ia3
				   , pn_ia4 => pn_ia4
				   , pn_ia5 => pn_ia5
				   , pn_ia6 => pn_ia6) loop
		insert into OLAP_SYS.GL_AUTOMATICAS_DETAIL(gambling_type
												 , list_id
												 , id
												 , comb1
												 , comb2
												 , comb3
												 , comb4
												 , comb5
												 , comb6
												 , ia_rojo
												 , seq_id
												 , pn_cnt
												 , none_cnt
												 , par_cnt
												 , term1_cnt
												 , term2_cnt
												 , comb_sum
												 , d1
												 , d2
												 , d3
												 , d4
												 , d5
												 , d6
												 , d01_09
												 , d10_19
												 , d20_29
												 , d30_39
												 , pn1
												 , pn2
												 , pn3
												 , pn4
												 , pn5
												 , pn6
												 , none1
												 , none2
												 , none3
												 , none4
												 , none5
												 , none6
												 , par1
												 , par2
												 , par3
												 , par4
												 , par5
												 , par6
												 , m3_1
												 , m3_2
												 , m3_3
												 , m3_4
												 , m3_5
												 , m3_6
												 , m3_cnt
												 , m4_1
												 , m4_2
												 , m4_3
												 , m4_4
												 , m4_5
												 , m4_6
												 , m4_cnt
												 , m5_1
												 , m5_2
												 , m5_3
												 , m5_4
												 , m5_5
												 , m5_6
												 , m5_cnt
												 , m7_1
												 , m7_2
												 , m7_3
												 , m7_4
												 , m7_5
												 , m7_6
												 , m7_cnt
                                                 , top_digit_cnt
                                                 , top_digit_history
												 , co_1
												 , co_2
												 , co_3
												 , co_4
												 , co_5
												 , co_cnt												 
												  )
		values(pv_gambling_type
			 , pn_list_id
			 , pn_id
			 , c.comb1
			 , c.comb2
			 , c.comb3
			 , c.comb4
			 , c.comb5
			 , c.comb6
			 , 'N'
			 , c.seq_id 
			 , c.pn_cnt
			 , c.none_cnt
			 , c.par_cnt
			 , c.t1_cnt
			 , c.t2_cnt
			 , c.comb_sum 
			 , c.d1
			 , c.d2
			 , c.d3
			 , c.d4
			 , c.d5
			 , c.d6
			 , c.d01_09
			 , c.d10_19
			 , c.d20_29
			 , c.d30_39
			 , c.pn1
			 , c.pn2
			 , c.pn3
			 , c.pn4
			 , c.pn5
			 , c.pn6
			 , c.none1
			 , c.none2
			 , c.none3
			 , c.none4
			 , c.none5
			 , c.none6
			 , c.par1
			 , c.par2
			 , c.par3
			 , c.par4
			 , c.par5
			 , c.par6
			 , c.m3_1
			 , c.m3_2
			 , c.m3_3
			 , c.m3_4
			 , c.m3_5
			 , c.m3_6
			 , c.m3_cnt
			 , c.m4_1
			 , c.m4_2
			 , c.m4_3
			 , c.m4_4
			 , c.m4_5
			 , c.m4_6
			 , c.m4_cnt
			 , c.m5_1
			 , c.m5_2
			 , c.m5_3
			 , c.m5_4
			 , c.m5_5
			 , c.m5_6
			 , c.m5_cnt
			 , c.m7_1
			 , c.m7_2
			 , c.m7_3
			 , c.m7_4
			 , c.m7_5
			 , c.m7_6
			 , c.m7_cnt
             , c.top_digit_cnt
             , c.top_digit_history
			 , c.co_1
			 , c.co_2
			 , c.co_3
			 , c.co_4
			 , c.co_5
			 , c.co_cnt 			 
			  );
	end loop;	  
	commit;
exception
  when others then
--	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end ins_gl_automaticas_detail;


--!handler para insertar jugadas en la tabla GL_AUTOMATICAS_DETAIL
procedure ins_gl_automaticas_handler(pv_gambling_type		varchar2 default 'mrtr'
								   , pn_id					number
								   , pn_ia1					number
								   , pn_ia2					number
								   , pn_ia3					number
								   , pn_ia4					number
								   , pn_ia5					number
								   , pn_ia6					number
								   , pn_list_id             number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_gl_automaticas_handler';
begin							   
	--!insertar un registro a la tabla GL_AUTOMATICAS_DETAIL
	ins_gl_automaticas_detail(pv_gambling_type => pv_gambling_type
						    , pn_id  => pn_id
						    , pn_ia1 => pn_ia1
						    , pn_ia2 => pn_ia2
						    , pn_ia3 => pn_ia3
						    , pn_ia4 => pn_ia4
						    , pn_ia5 => pn_ia5
						    , pn_ia6 => pn_ia6
							, pn_list_id => pn_list_id);
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end ins_gl_automaticas_handler;

--!obtener el ultimo id de la info de gigaloterias
function get_max_id return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_max_id';
	ln$max_id						 number := 0;
begin
	with max_id_tbl as (
	select max(drawing_id) max_id 
	  from olap_sys.s_calculo_stats
	), ultima_jugada_cnt_tbl as (
	select drawing_id
		 , count(1) jcnt
	  from olap_sys.s_calculo_stats
	 where drawing_id = (select max_id from max_id_tbl) 
	 group by drawing_id
	), ultima_jugada_match_cnt_tbl as (
	select drawing_id
		 , count(1) jcnt
	  from olap_sys.s_calculo_stats
	 where drawing_id = (select max_id from max_id_tbl) 
	   and winner_flag is null
	 group by drawing_id 
	) select case when jcnt = (select jcnt from ultima_jugada_match_cnt_tbl) then drawing_id else drawing_id-1 end drawing_id
	    into ln$max_id
		from ultima_jugada_cnt_tbl;  
	return ln$max_id;	
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    return 0;
end get_max_id;

--!actualizar en el header el ID del sorteo que se esta procesando
procedure upd_gl_automaticas_header(pn_drawing_id		number
                                  , pn_list_id          number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_gl_automaticas_header';
begin
	update OLAP_SYS.GL_AUTOMATICAS_HEADER
	   set SORTEO_ACTUAL = pn_drawing_id
	 where sorteo_final is null
	   and list_id = pn_list_id;  
exception
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end upd_gl_automaticas_header;


--!actualizar los valores de frecuencia, ley del tercio, ciclo de aparicion, pronos por ciclo y numeros preferidos, rango ley del tercio
procedure upd_gl_automaticas_detail(pn_drawing_id		number
								  , pv_ca_comb_flag		varchar2
								  , pn_list_id          number) is 
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_gl_automaticas_detail';
	--contadores de ley del tercio
	ln$null_cnt 					 number := 0;
	ln$red_cnt 						 number := 0;
	ln$green_cnt 					 number := 0;
	ln$blue_cnt 					 number := 0;
	--contadores de frecuencia
	ln$fr_null_cnt 					 number := 0;
	ln$fr_red_cnt 					 number := 0;
	ln$fr_green_cnt 				 number := 0;
	ln$fr_blue_cnt 					 number := 0;
	--!cursor principal del proceso
	cursor c_upd_main (pn_drawing_id		number) is
	select drawing_id id
		 , b_type
		 , digit
		 , color_ubicacion fre
		 , color_ley_tercio lt
		 , case when b_type = 'B1' and ciclo_aparicion between 4 and 10 then 1
			when b_type = 'B2' and ciclo_aparicion between 8 and 17 then 1
			when b_type = 'B3' and ciclo_aparicion between 9 and 22 then 1
			when b_type = 'B4' and ciclo_aparicion between 9 and 22 then 1
			when b_type = 'B5' and ciclo_aparicion between 7 and 18 then 1
			when b_type = 'B6' and ciclo_aparicion between 4 and 12 then 1 else 0 end ca
		 , nvl(pronos_ciclo,-1) pxc
		 , decode(CHNG_POSICION,null,0,1) chg
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
	     , rango_ley_tercio rlt 
         , ubicacion fre_cnt
		 , ley_tercio lt_cnt
	  from olap_sys.s_calculo_stats
	 where drawing_id = pn_drawing_id
	 order by b_type, rlt;
 
	cursor c_jugadas_mas_ganadoras (pn_list_id  number) is
	with jugadas_mas_ganadoras_tbl as (
	select list_id, id, aciertos_history, len_aciertos_history, incidencia_cnt, aciertos_accum, incidencia from (
	select list_id
		 , id
		 , aciertos_history
		 , length(aciertos_history) len_aciertos_history
		 , regexp_count(aciertos_history, '\(3\)') as incidencia_cnt
		 , aciertos_accum
		 , rank() over (partition by aciertos_accum order by length(aciertos_history)) len_rank 
		 , 3 incidencia
	  from olap_sys.gl_automaticas_detail 
	 where list_id = pn_list_id 
	) where len_rank = 1  
		and incidencia_cnt > 0
	union     
	select list_id, id, aciertos_history, len_aciertos_history, incidencia_cnt, aciertos_accum, incidencia from (
	select list_id
		 , id
		 , aciertos_history
		 , length(aciertos_history) len_aciertos_history
		 , regexp_count(aciertos_history, '\(4\)') as incidencia_cnt
		 , aciertos_accum
		 , rank() over (partition by aciertos_accum order by length(aciertos_history)) len_rank
		 , 4 incidencia
	  from olap_sys.gl_automaticas_detail 
	 where list_id = pn_list_id 
	) where len_rank = 1  
		and incidencia_cnt > 0
	union     
	select list_id, id, aciertos_history, len_aciertos_history, incidencia_cnt, aciertos_accum, incidencia from (
	select list_id
		 , id
		 , aciertos_history
		 , length(aciertos_history) len_aciertos_history
		 , regexp_count(aciertos_history, '\(5\)') as incidencia_cnt
		 , aciertos_accum
		 , rank() over (partition by aciertos_accum order by length(aciertos_history)) len_rank
		 , 5 incidencia
	  from olap_sys.gl_automaticas_detail
     where list_id = pn_list_id	  
	) where len_rank = 1  
		and incidencia_cnt > 0
	union     
	select list_id, id, aciertos_history, len_aciertos_history, incidencia_cnt, aciertos_accum, incidencia from (
	select list_id
		 , id
		 , aciertos_history
		 , length(aciertos_history) len_aciertos_history
		 , regexp_count(aciertos_history, '\(6\)') as incidencia_cnt
		 , aciertos_accum
		 , rank() over (partition by aciertos_accum order by length(aciertos_history)) len_rank
		 , 6 incidencia
	  from olap_sys.gl_automaticas_detail
     where list_id = pn_list_id		  
	) where len_rank = 1  
		and incidencia_cnt > 0
	) 
	select list_id, id, incidencia, incidencia_cnt
	  from jugadas_mas_ganadoras_tbl jg 
	 where exists (select 1 
					 from olap_sys.gl_automaticas_header ah
					where ah.list_id = jg.list_id
					  and ah.sorteo_final is null
					  and ah.list_id = pn_list_id);

/*
	cursor c_ca_comb_validacion is
	with jugadas_tbl as (
	select comb_sum jcomb_sum 
		 , count(1) jcnt
	  from olap_sys.w_combination_responses_fs
	 group by comb_sum
	)
	, jugadas_percentil_tbl as (
	select percentile_disc(0.38) within group (order by jcomb_sum) per_jcomb_sum_ini
		 , percentile_disc(0.62) within group (order by jcomb_sum) per_jcomb_sum_end
	  from jugadas_tbl
	)
	, jugadas_rango_tbl as (
	select *
	  from jugadas_tbl
	 where jcomb_sum between (select per_jcomb_sum_ini from jugadas_percentil_tbl) and (select per_jcomb_sum_end from jugadas_percentil_tbl)
	)
	, ca_tbl as (
	select sum_ca ca_sum
		 , count(1) ca_cnt
	  from olap_sys.pm_mr_resultados_v2
	 where GAMBLING_ID > 594
	 group by sum_ca
	)
	, ca_percentil_tbl as (
	select percentile_disc(0.38) within group (order by ca_cnt) per_ca_cnt_ini
	  from ca_tbl
	)
	, ca_rango_tbl as (
	select *
	  from ca_tbl
	 where ca_cnt >= (select per_ca_cnt_ini from ca_percentil_tbl)
	)
	, ca_comb_output_tbl as (
	select sum_ca
		 , comb_sum
		 , count(1) rcnt
	  from olap_sys.pm_mr_resultados_v2
	 where GAMBLING_ID > 594 
	   and comb_sum in (select jcomb_sum from jugadas_rango_tbl)
	   and sum_ca in (select ca_sum from ca_rango_tbl)
	 group by sum_ca
		 , comb_sum  
	)
	, automaticas_detail_tbl as (
	select list_id
		 , id 
		 , decode(C1_CA,-1,0,C1_CA)+decode(C2_CA,-1,0,C2_CA)+decode(C3_CA,-1,0,C3_CA)+decode(C4_CA,-1,0,C4_CA)+decode(C5_CA,-1,0,C5_CA)+decode(C6_CA,-1,0,C6_CA) ca_sum
		 , comb_sum
	  from olap_sys.gl_automaticas_detail
     where list_id = pn_list_id		  
	)
	select list_id
		 , id 
	  from automaticas_detail_tbl 
	 where (ca_sum,comb_sum) in (select sum_ca, comb_sum
								  from ca_comb_output_tbl);		
*/

	/*
	Este cursor va a habilitar las tercias C1, C5 y C6 que mas aparecen
	*/
/*	cursor c_patron_c1_c5_c6 is
	with jugadas_tbl as ( 
	select global_index, comb1, comb2, comb3, comb4, comb5, comb6
		 , olap_sys.w_common_pkg.get_c1_c5_c6_rank(comb1, comb5, comb6) j_c1_c5_c6_rank
	  from olap_sys.w_combination_responses_fs
	 where pn1 = 1
	)
	, resultados_tbl as (
	select comb1, comb2, comb3, comb4, comb5, comb6
		 , olap_sys.w_common_pkg.get_c1_c5_c6_rank(comb1, comb5, comb6) r_c1_c5_c6_rank
	  from olap_sys.pm_mr_resultados_v2             
	 where pn1 > 0
	)
	, automaticas_tbl as (
	select comb1 ia1, comb2 ia2, comb3 ia3, comb4 ia4, comb5 ia5, comb6 ia6
		 , olap_sys.w_common_pkg.get_c1_c5_c6_rank(comb1, comb5, comb6) a_c1_c5_c6_rank
	  from olap_sys.gl_automaticas_detail             
	)
	, jugadas_cnt_tbl as (
	select comb1
		 , comb5
		 , comb6 
		 , j_c1_c5_c6_rank
		 , count(1) j_cnt
	  from jugadas_tbl
	 group by comb1
		 , comb5
		 , comb6
		 , j_c1_c5_c6_rank
	)
	, resultados_cnt_tbl as (
	select comb1 r_c1
		 , comb5 r_c5
		 , comb6 r_c6
		 , r_c1_c5_c6_rank
		 , count(1) r_cnt
	  from resultados_tbl
	 group by comb1
		 , comb5
		 , comb6
		 , r_c1_c5_c6_rank
	)
	, automaticas_cnt_tbl as (
	select ia1 a_c1
		 , ia5 a_c5
		 , ia6 a_c6
		 , a_c1_c5_c6_rank
		 , count(1) a_cnt
	  from automaticas_tbl
	 group by ia1
		 , ia5
		 , ia6
		 , a_c1_c5_c6_rank
	)
	, output_tbl as (
	select comb1
		 , comb5
		 , comb6
		 , j_cnt
		 , nvl((select r_cnt from resultados_cnt_tbl where r_c1=comb1 and r_c5=comb5 and r_c6=comb6 and j_c1_c5_c6_rank = r_c1_c5_c6_rank),0) r_cnt
		 , nvl((select a_cnt from automaticas_cnt_tbl where a_c1=comb1 and a_c5=comb5 and a_c6=comb6 and j_c1_c5_c6_rank = a_c1_c5_c6_rank),0) a_cnt
	  from jugadas_cnt_tbl  
	)
	select *
	  from output_tbl
	 where r_cnt > 0 
	   and a_cnt > 0;
*/	   
	cursor c_automaticas_detail is
	select list_id, id, CLT1, CLT2, CLT3, CLT4, CLT5, CLT6, CU1, CU2, CU3, CU4, CU5, CU6
	  from olap_sys.gl_automaticas_detail;
	
	cursor c_gl_resultados (pn_drawing_id		number) is	
	with gl_resultados_tbl as (
	select b_type
		 , digit
		 , color_ley_tercio clt
	  from olap_sys.s_calculo_stats
	 where winner_flag is not null
	   and drawing_id = pn_drawing_id - 1
	)   
	select gambling_id
		 , comb1
		 , comb2
		 , comb3
		 , comb4
		 , comb5
		 , comb6
		 , nvl((select clt from gl_resultados_tbl where b_type='B1' and digit=comb1),0) clt1
		 , nvl((select clt from gl_resultados_tbl where b_type='B2' and digit=comb2),0) clt2
		 , nvl((select clt from gl_resultados_tbl where b_type='B3' and digit=comb3),0) clt3
		 , nvl((select clt from gl_resultados_tbl where b_type='B4' and digit=comb4),0) clt4
		 , nvl((select clt from gl_resultados_tbl where b_type='B5' and digit=comb5),0) clt5
		 , nvl((select clt from gl_resultados_tbl where b_type='B6' and digit=comb6),0) clt6
	  from olap_sys.sl_gamblings
	 where gambling_id = pn_drawing_id; 	  
begin	
	dbms_output.enable(null);
	dbms_output.put_line('----------------------------');
	dbms_output.put_line(LV$PROCEDURE_NAME);
	
	--!limpiar todos los campos
	update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
	   set CU1 = -1
		 , CLT1 = -1
		 , C1_CA = 0
		 , PXC1 = -1
		 , PRE1 = -1
		 , CHNG_POS1 = -1
		 , AVG_CA_FLAG1 = '#'
		 , CU1_CNT = -1
		 , CLT1_CNT = -1
		 , CU2 = -1
		 , CLT2 = -1
		 , C2_CA = 0
		 , PXC2 = -1
		 , PRE2 = -1
		 , CHNG_POS2 = -1
		 , AVG_CA_FLAG2 = '#'
		 , CU2_CNT = -1
		 , CLT2_CNT = -1		 
		 , CU3 = -1
		 , CLT3 = -1
		 , C3_CA = 0
		 , PXC3 = -1
		 , PRE3 = -1
		 , CHNG_POS3 = -1
		 , AVG_CA_FLAG3 = '#'
		 , CU3_CNT = -1
		 , CLT3_CNT = -1		 
	     , CU4 = -1
		 , CLT4 = -1
		 , C4_CA = 0
		 , PXC4 = -1
		 , PRE4 = -1
		 , CHNG_POS4 = -1
		 , AVG_CA_FLAG4 = '#'
		 , CU4_CNT = -1
		 , CLT4_CNT = -1		 
	     , CU5 = -1
		 , CLT5 = -1
		 , C5_CA = 0
		 , PXC5 = -1
		 , PRE5 = -1
		 , CHNG_POS5 = -1
		 , AVG_CA_FLAG5 = '#'
		 , CU5_CNT = -1
		 , CLT5_CNT = -1		 
		 , CU6 = -1
		 , CLT6 = -1
		 , C6_CA = 0
		 , PXC6 = -1
		 , PRE6 = -1
		 , CHNG_POS6 = -1
		 , AVG_CA_FLAG6 = '#'
		 , CU6_CNT = -1
		 , CLT6_CNT = -1		 
		 , GAMBLING_ID = pn_drawing_id
		 , INCIDENCIA = 0
		 , INCIDENCIA_CNT = 0
		 , JUGAR_FLAG = 'N'
		 , NULL_CNT = 0
		 , RED_CNT = 0
		 , GREEN_CNT = 0
		 , BLUE_CNT = 0
		 , SUM_CA = 0
         , PXC_CNT = 0
         , PRECNT = 0
         , CHNG_CNT = 0
		 , RLT1 = 0
		 , RLT2 = 0
		 , RLT3 = 0
		 , RLT4 = 0
		 , RLT5 = 0
		 , RLT6 = 0
		 , FR_NULL_CNT = 0
		 , FR_RED_CNT = 0
		 , FR_GREEN_CNT = 0
		 , FR_BLUE_CNT = 0		 
	 WHERE LIST_ID = pn_list_id
	   AND EXISTS (SELECT 1
                     FROM OLAP_SYS.GL_AUTOMATICAS_HEADER AH
                    WHERE AH.LIST_ID = AD.LIST_ID
                      AND AH.SORTEO_FINAL IS NULL
					  AND AH.LIST_ID = pn_list_id);
	
	for i in c_upd_main (pn_drawing_id => pn_drawing_id) loop
		dbms_output.put_line(i.id||' - '||i.b_type||' - '||i.digit||' - '||i.rlt||' - '||i.avg_ca_flag);
		--!posicion1
		if i.b_type = 'B1' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set CU1 = i.fre
			     , CLT1 = i.lt
				 , C1_CA = i.ca
				 , PXC1 = i.pxc
--				 , PRE1 = i.pxc_pref
				 , PRE1 = i.pref
				 , CHNG_POS1 = i.CHG
				 , AVG_CA_FLAG1 = i.avg_ca_flag
--				 , SORTEO_ACTUAL = pn_drawing_id
                 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT1 = CASE WHEN i.rlt BETWEEN 1 AND 7 THEN 1 ELSE 0 END
				 , CU1_CNT = i.fre_cnt
				 , CLT1_CNT = i.lt_cnt
		     where COMB1 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);		 		
		end if;

		--!posicion2
		if i.b_type = 'B2' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD 
			   set CU2 = i.fre
			     , CLT2 = i.lt
				 , C2_CA = i.ca
				 , PXC2 = i.pxc
--				 , PRE2 = i.pxc_pref
				 , PRE2 = i.pref				 
				 , CHNG_POS2 = i.CHG
				 , AVG_CA_FLAG2 = i.avg_ca_flag
				 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT2 = CASE WHEN i.rlt BETWEEN 3 AND 10 THEN 1 ELSE 0 END
				 , CU2_CNT = i.fre_cnt
				 , CLT2_CNT = i.lt_cnt				 
		     where COMB2 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);		
		end if;
		
		--!posicion3
		if i.b_type = 'B3' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set CU3 = i.fre
			     , CLT3 = i.lt
				 , C3_CA = i.ca
				 , PXC3 = i.pxc
--				 , PRE3 = i.pxc_pref
				 , PRE3 = i.pref				 
				 , CHNG_POS3 = I.CHG
				 , AVG_CA_FLAG3 = i.avg_ca_flag
				 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT3 = CASE WHEN i.rlt BETWEEN 3 AND 15 THEN 1 ELSE 0 END
				 , CU3_CNT = i.fre_cnt
				 , CLT3_CNT = i.lt_cnt				 
		     where COMB3 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);
		end if;		

		--!posicion4
		if i.b_type = 'B4' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set CU4 = i.fre
			     , CLT4 = i.lt
				 , C4_CA = i.ca
				 , PXC4 = i.pxc
--				 , PRE4 = i.pxc_pref
				 , PRE4 = i.pref				 
				 , CHNG_POS4 = I.CHG
				 , AVG_CA_FLAG4 = i.avg_ca_flag
				 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT4 = CASE WHEN i.rlt BETWEEN 3 AND 12 THEN 1 ELSE 0 END
				 , CU4_CNT = i.fre_cnt
				 , CLT4_CNT = i.lt_cnt				 
		     where COMB4 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);

		end if;	

		--!posicion5
		if i.b_type = 'B5' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set CU5 = i.fre
			     , CLT5 = i.lt
				 , C5_CA = i.ca
				 , PXC5 = i.pxc
--				 , PRE5 = i.pxc_pref
				 , PRE5 = i.pref				 
				 , CHNG_POS5 = I.CHG
				 , AVG_CA_FLAG5 = i.avg_ca_flag
				 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT5 = CASE WHEN i.rlt BETWEEN 2 AND 10 THEN 1 ELSE 0 END
				 , CU5_CNT = i.fre_cnt
				 , CLT5_CNT = i.lt_cnt				 
		     where COMB5 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);
		end if;	

		--!posicion6
		if i.b_type = 'B6' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set CU6 = i.fre
			     , CLT6 = i.lt
				 , C6_CA = i.ca
				 , PXC6 = i.pxc
--				 , PRE6 = i.pxc_pref
				 , PRE6 = i.pref				 
				 , CHNG_POS6 = I.CHG
				 , AVG_CA_FLAG6 = i.avg_ca_flag
				 --rango definido en la hoja de excel ins_s_gigaloterias_stats_MACROS
				 , RLT6 = CASE WHEN i.rlt BETWEEN 1 AND 7 THEN 1 ELSE 0 END
				 , CU6_CNT = i.fre_cnt
				 , CLT6_CNT = i.lt_cnt				 
		     where COMB6 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);
		end if;	
	end loop;

	--!actualizar los resultados de la tabla GL_HISTORY_PERCENTAGES
	for t in c_gl_resultados (pn_drawing_id => pn_drawing_id) loop
		--!posicion1
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B1'
		   and lt_next_color = t.clt1;	

		--!posicion2
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B2'
		   and lt_next_color = t.clt2;	

		--!posicion3
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B3'
		   and lt_next_color = t.clt3;	

		--!posicion4
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B4'
		   and lt_next_color = t.clt4;
		   
		--!posicion5
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B5'
		   and lt_next_color = t.clt5;		

		--!posicion6
		update olap_sys.gl_history_percentages
		   set winner_flag = 'Y'
		 where gambling_id = pn_drawing_id
		   and b_type = 'B6'
		   and lt_next_color = t.clt6;		   
	end loop;
	
    --!actualizar el ca_sum
    update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
	   set SUM_CA = C1_CA + C2_CA + C3_CA + C4_CA + C5_CA + C6_CA
	     , PXC_CNT = PXC1 + PXC2 + PXC3 + PXC4 + PXC5 + PXC6
		 , PRECNT = PRE1 + PRE2 + PRE3 + PRE4 + PRE5 + PRE6
		 , CHNG_CNT = CHNG_POS1 + CHNG_POS2 + CHNG_POS3 + CHNG_POS4 + CHNG_POS5 + CHNG_POS6
		 , RLT_CNT = RLT1 + RLT2 + RLT3 + RLT4 + RLT5 + RLT6
     where LIST_ID = pn_list_id
	   and exists (select 1
					 from olap_sys.gl_automaticas_header ah
					where ah.list_id = ad.list_id
					  and ah.sorteo_final is null
					  and ah.list_id = pn_list_id);	
						  
	--!actualizacion de las jugadas mas ganadoras 
	for t in c_jugadas_mas_ganadoras (pn_list_id => pn_list_id) loop
		update OLAP_SYS.GL_AUTOMATICAS_DETAIL
		   set INCIDENCIA = t.incidencia
			 , INCIDENCIA_CNT = t.incidencia_cnt
		 where list_id = t.list_id
		   and id = t.id
		   and list_id = pn_list_id;
	end loop;

	for c in c_automaticas_detail loop
		--contadores de ley del tercio
		ln$null_cnt := 0;
		ln$red_cnt := 0;
		ln$green_cnt := 0;
		ln$blue_cnt := 0;

		--contadores de frecuencia
		ln$fr_null_cnt := 0;
		ln$fr_red_cnt := 0;
		ln$fr_green_cnt := 0;
		ln$fr_blue_cnt := 0;
		
		--B1
		--ley tercio
		if c.CLT1 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT1 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT1 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT1 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU1 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU1 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU1 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU1 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		--B2
		--ley tercio
		if c.CLT2 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT2 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT2 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT2 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU2 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU2 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU2 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU2 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		--B3
		--ley tercio
		if c.CLT3 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT3 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT3 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT3 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU3 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU3 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU3 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU3 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		--B4
		--ley tercio
		if c.CLT4 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT4 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT4 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT4 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU4 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU4 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU4 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU4 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		--B5
		--ley tercio
		if c.CLT5 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT5 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT5 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT5 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU5 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU5 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU5 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU5 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		--B6
		--ley tercio
		if c.CLT6 = -1 then ln$null_cnt := ln$null_cnt + 1; end if;
		if c.CLT6 = 1 then ln$red_cnt := ln$red_cnt + 1; end if;
		if c.CLT6 = 2 then ln$green_cnt := ln$green_cnt + 1; end if;
		if c.CLT6 = 3 then ln$blue_cnt := ln$blue_cnt + 1; end if;
		--frecuencia
		if c.CU6 = -1 then ln$fr_null_cnt := ln$fr_null_cnt + 1; end if;
		if c.CU6 = 1 then ln$fr_red_cnt := ln$fr_red_cnt + 1; end if;
		if c.CU6 = 2 then ln$fr_green_cnt := ln$fr_green_cnt + 1; end if;
		if c.CU6 = 3 then ln$fr_blue_cnt := ln$fr_blue_cnt + 1; end if;

		update olap_sys.gl_automaticas_detail
           set null_cnt	 = ln$null_cnt
			 , red_cnt   = ln$red_cnt
			 , green_cnt = ln$green_cnt
			 , blue_cnt  = ln$blue_cnt
			 , fr_null_cnt	= ln$fr_null_cnt
			 , fr_red_cnt   = ln$fr_red_cnt
			 , fr_green_cnt = ln$fr_green_cnt
			 , fr_blue_cnt  = ln$fr_blue_cnt
		 where list_id = c.list_id 
		   and id = c.id; 
	end loop;
exception
  when others then
--	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end upd_gl_automaticas_detail;

	 
--!contar numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure contar_repetidos(pn_drawing_id	  number
						 , pn_list_id         number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'contar_repetidos';
begin							   
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	
	--!limpiando el contador de numeros repetidos
	olap_sys.w_common_pkg.g_dml_stmt := 'UPDATE OLAP_SYS.GL_AUTOMATICAS_DETAIL';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' SET REPETIDOS_CNT = 0';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' WHERE LIST_ID = :1';
	
	execute immediate olap_sys.w_common_pkg.g_dml_stmt using pn_list_id;
	
	--!actualizando el campo de numeros repetidos
	--!en base a los resultados del sorteo anterior
	update olap_sys.gl_automaticas_detail d
	   set d.repetidos_cnt = (
		   select count(*)
			 from olap_sys.s_calculo_stats r
			where r.drawing_id = pn_drawing_id - 2
			  and r.winner_flag is not null
			  and r.digit in (d.comb1, d.comb2, d.comb3, d.comb4, d.comb5, d.comb6)
		   )
	 where d.gambling_id = pn_drawing_id;
	dbms_output.put_line(sql%rowcount||' repetidos_cnt actualizados');	 
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end contar_repetidos;


--!contar los aciertos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure contar_aciertos(pn_drawing_id		 number
					    , pn_list_id         number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'contar_aciertos';
	ln$acierto_cnt					 number := 0;
 
    cursor c_4_aciertos is
    select list_id
         , 4 aciertos
         , count(1) aciertos_cnt 
      from olap_sys.gl_automaticas_detail
     where aciertos_accum > 0
       and instr(aciertos_history,'(4)') > 0 
     group by list_id;

    cursor c_5_aciertos is     
    select list_id
         , 5 aciertos
         , count(1) aciertos_cnt
      from olap_sys.gl_automaticas_detail
     where aciertos_accum > 0
       and instr(aciertos_history,'(5)') > 0 
     group by list_id;

    cursor c_6_aciertos is    
    select list_id
         , 6 aciertos
         , count(1) aciertos_cnt
      from olap_sys.gl_automaticas_detail
     where aciertos_accum > 0
       and instr(aciertos_history,'(6)') > 0 
     group by list_id;
begin							   
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	
	--!limpiando el contador de aciertos
	olap_sys.w_common_pkg.g_dml_stmt := 'UPDATE OLAP_SYS.GL_AUTOMATICAS_DETAIL';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' SET ACIERTOS_CNT = 0';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' WHERE LIST_ID = :1';
	
	execute immediate olap_sys.w_common_pkg.g_dml_stmt using pn_list_id;
	
	--!actualizando el campo de aciertos cnt	
	update olap_sys.gl_automaticas_detail d
	   set d.aciertos_cnt = (
		   select count(*)
			 from olap_sys.s_calculo_stats r
			where r.drawing_id = pn_drawing_id - 1
			  and r.winner_flag is not null
			  and r.digit in (d.comb1, d.comb2, d.comb3, d.comb4, d.comb5, d.comb6)
		   )
	 where d.gambling_id = pn_drawing_id
	   and d.list_id = pn_list_id; 
	dbms_output.put_line(sql%rowcount||' aciertos_cnt actualizados');

	--!actualizando el campo de aciertos history
	update olap_sys.gl_automaticas_detail ad
	   set ad.aciertos_history = ad.aciertos_history||'~'||ad.gambling_id||'('||ad.aciertos_cnt||')'
		 , ad.aciertos_accum = nvl(ad.aciertos_accum,0) + ad.aciertos_cnt
		 , ad.creation_date = sysdate
	 where ad.aciertos_cnt > 0
	   and ad.gambling_id = pn_drawing_id
	   and ad.list_id = pn_list_id
	   and exists (select 1
					 from olap_sys.gl_automaticas_header ah
					where ah.gambling_type = ad.gambling_type 
					  and ah.list_id = ad.list_id
					  and ah.sorteo_final is null);
	dbms_output.put_line(sql%rowcount||' aciertos_history actualizados');

	--!actualiando el header con jugadas con 4 aciertos
	for a in c_4_aciertos loop
		update olap_sys.gl_automaticas_header
		   set acierto4_cnt = a.aciertos_cnt
			 , sorteo_actual = pn_drawing_id
			 , creation_date = sysdate			 
		  where list_id = pn_list_id;        
	end loop;
	
	--!actualiando el header con jugadas con 4 aciertos
	for a in c_5_aciertos loop
		update olap_sys.gl_automaticas_header
		   set acierto5_cnt = a.aciertos_cnt
			 , sorteo_actual = pn_drawing_id
			 , creation_date = sysdate
		  where list_id = pn_list_id;        
	end loop;
	
	--!actualiando el header con jugadas con 4 aciertos
	for a in c_6_aciertos loop
		update olap_sys.gl_automaticas_header
		   set acierto6_cnt = a.aciertos_cnt
			 , sorteo_actual = pn_drawing_id
			 , creation_date = sysdate
		  where list_id = pn_list_id;        
	end loop;   
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end contar_aciertos;


--!contar los aciertos que hizo el master prompt del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure contar_mp_aciertos(pn_drawing_id		 number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'contar_mp_aciertos';
	ln$acierto_cnt					 number := 0;
	
	cursor c_resultados (pn_drawing_id		 number) is
	select comb1, comb2, comb3, comb4, comb5, comb6   
	  from olap_sys.pm_mr_resultados_v3
	 where gambling_id = pn_drawing_id;	
begin							   
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	
	--!actualizando el campo de match_cnt	
	update olap_sys.mr_mp_predictions d
	   set d.match_cnt = (
		   select count(*)
			 from olap_sys.s_calculo_stats r
			where r.drawing_id = pn_drawing_id - 1
			  and r.winner_flag is not null
			  and r.digit in (d.comb1, d.comb2, d.comb3, d.comb4, d.comb5, d.comb6)
		   )
	 where d.gambling_id = pn_drawing_id; 
	dbms_output.put_line(sql%rowcount||' aciertos_cnt actualizados');

    --!actualizando los match de cada posicion
	for k in c_resultados (pn_drawing_id => pn_drawing_id) loop
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id||' -> '||k.comb1||'-'||k.comb2||'-'||k.comb3||'-'||k.comb4||'-'||k.comb5||'-'||k.comb6);
		update olap_sys.mr_mp_predictions d
	       set d.match_comb1 = case when d.comb1=k.comb1 then 1 else 0 end
		     , d.match_comb2 = case when d.comb2=k.comb2 then 1 else 0 end
			 , d.match_comb3 = case when d.comb3=k.comb3 then 1 else 0 end
			 , d.match_comb4 = case when d.comb4=k.comb4 then 1 else 0 end
			 , d.match_comb5 = case when d.comb5=k.comb5 then 1 else 0 end
			 , d.match_comb6 = case when d.comb6=k.comb6 then 1 else 0 end
		 where d.gambling_id = pn_drawing_id;	 
	end loop;
	
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end contar_mp_aciertos;


--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure aciertos_repetidos_handler(pn_drawing_id		number
									 -- 0: execute all
									 -- 1: execute contar_repetidos
									 -- 2: execute contar_aciertos
								   , pv_execution_type  number default 0
								   , pv_execution_flag  varchar2 default 'SINGLE') is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'aciertos_repetidos_handler';
	ln$row_cnt                       number := 0;
	ln$max_jugada_id				 number := 0;
	ln$jugada_drawing_id		 	 number := 0;
begin	
	dbms_output.enable(null);
	update olap_sys.gl_automaticas_detail
	   set gambling_id = pn_drawing_id;
	
	for k in c_automaticas_header loop
		--!verificando existencia de registros en la tabla detalle
		select count(1) cnt
		  into ln$row_cnt
		  from olap_sys.gl_automaticas_detail
		 where list_id = k.list_id; 
		
		--!el proceso continua solo si hay registros en la tabla detalle
		--!de esta forma aunque solo exista el registro valido en la tabla maestra
		--!no se continua con el proceso
		if ln$row_cnt > 0 then
			if pv_execution_type = 0 then
				--!contar los aciertos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_aciertos(pn_drawing_id	=> pn_drawing_id
							  , pn_list_id    => k.list_id);

				--!contar los aciertos que hizo el master prompt del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_mp_aciertos(pn_drawing_id => pn_drawing_id); 

				--!contar los numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_repetidos(pn_drawing_id	=> pn_drawing_id
							   , pn_list_id    => k.list_id);
			end if;	

			if pv_execution_type = 1 then
				--!contar los numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_repetidos(pn_drawing_id	=> pn_drawing_id
							   , pn_list_id    => k.list_id);
			end if;	

			if pv_execution_type = 2 then
				--!contar los aciertos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_aciertos(pn_drawing_id	=> pn_drawing_id
							  , pn_list_id    => k.list_id);
							  
				--!contar los aciertos que hizo el master prompt del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				contar_mp_aciertos(pn_drawing_id => pn_drawing_id); 							  
			end if;				
		end if;					  
	end loop;
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 		
end aciertos_repetidos_handler; 	


--!handler para actualizar jugadas en la tabla GL_AUTOMATICAS_DETAIL
--!en base a la info de gigaloterias
procedure upd_gl_automaticas_handler(pn_drawing_id     number
								   , pv_ca_comb_flag    varchar2 default 'N') is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_gl_automaticas_handler';
begin							   
	for k in c_automaticas_header loop 
		--!actualizar en el header el ID del sorteo que se esta procesando
		upd_gl_automaticas_header(pn_drawing_id => pn_drawing_id
		                        , pn_list_id    => k.list_id); 
		--!actualizar los valores de frecuencia, ley del tercio, ciclo de aparicion, pronos por ciclo y numeros preferidos
		upd_gl_automaticas_detail(pn_drawing_id => pn_drawing_id
								, pv_ca_comb_flag => pv_ca_comb_flag
								, pn_list_id    => k.list_id);							
	end loop;						
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end upd_gl_automaticas_handler;



procedure upd_predicciones_all(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_predicciones_all'; 
	lv$prediccion_nombre		  varchar2(30);
	ln$sorteo_anterior            number := pn_drawing_id -1;
	
	cursor c_resultados(pn_drawing_id		number) is
	with resultados_tbl as (
	select comb1, comb2, comb3, comb4, comb5, comb6
		 , nvl(cu1,'#') fr1, nvl(cu2,'#') fr2, nvl(cu3,'#') fr3, nvl(cu4,'#') fr4, nvl(cu5,'#') fr5, nvl(cu6,'#') fr6
		 , nvl(clt1,'#') lt1, nvl(clt2,'#') lt2, nvl(clt3,'#') lt3, nvl(clt4,'#') lt4, nvl(clt5,'#') lt5, nvl(clt6,'#') lt6
		 , case when nvl(clt1,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then '1' else '0' end end primo1
		 , case when nvl(clt2,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb2) = 1 then '1' else '0' end end primo2       
		 , case when nvl(clt3,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then '1' else '0' end end primo3
		 , case when nvl(clt4,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then '1' else '0' end end primo4 
		 , case when nvl(clt5,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb5) = 1 then '1' else '0' end end primo5       
		 , case when nvl(clt6,'#') = '#' then '#' else case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then '1' else '0' end end primo6
		 , case when nvl(clt1,'#') = '#' then '#' else case when mod(comb1,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb1) = 0 then '1' else '0' end end impar1
		 , case when nvl(clt2,'#') = '#' then '#' else case when mod(comb2,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb2) = 0 then '1' else '0' end end impar2       
		 , case when nvl(clt3,'#') = '#' then '#' else case when mod(comb3,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb3) = 0 then '1' else '0' end end impar3
		 , case when nvl(clt4,'#') = '#' then '#' else case when mod(comb4,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb4) = 0 then '1' else '0' end end impar4 
		 , case when nvl(clt5,'#') = '#' then '#' else case when mod(comb5,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb5) = 0 then '1' else '0' end end impar5       
		 , case when nvl(clt6,'#') = '#' then '#' else case when mod(comb6,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb6) = 0 then '1' else '0' end end impar6   
		 , case when nvl(clt1,'#') = '#' then '#' else case when mod(comb1,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb1) = 0 then '1' else '0' end end par1
		 , case when nvl(clt2,'#') = '#' then '#' else case when mod(comb2,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb2) = 0 then '1' else '0' end end par2       
		 , case when nvl(clt3,'#') = '#' then '#' else case when mod(comb3,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb3) = 0 then '1' else '0' end end par3
		 , case when nvl(clt4,'#') = '#' then '#' else case when mod(comb4,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb4) = 0 then '1' else '0' end end par4 
		 , case when nvl(clt5,'#') = '#' then '#' else case when mod(comb5,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb5) = 0 then '1' else '0' end end par5       
		 , case when nvl(clt6,'#') = '#' then '#' else case when mod(comb6,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb6) = 0 then '1' else '0' end end par6             
         , case when nvl(clt1,'#') = '#' then '#' else case when chng_pos1 is null then '1' else '0' end end chng1 
         , case when nvl(clt2,'#') = '#' then '#' else case when chng_pos2 is null then '1' else '0' end end chng2 
         , case when nvl(clt3,'#') = '#' then '#' else case when chng_pos3 is null then '1' else '0' end end chng3 
         , case when nvl(clt4,'#') = '#' then '#' else case when chng_pos4 is null then '1' else '0' end end chng4 
         , case when nvl(clt5,'#') = '#' then '#' else case when chng_pos5 is null then '1' else '0' end end chng5 
         , case when nvl(clt6,'#') = '#' then '#' else case when chng_pos6 is null then '1' else '0' end end chng6 
         , case when nvl(clt1,'#') = '#' then '#' else case when pre1 is null then '0' else '1' end end pref1
         , case when nvl(clt2,'#') = '#' then '#' else case when pre2 is null then '0' else '1' end end pref2
         , case when nvl(clt3,'#') = '#' then '#' else case when pre3 is null then '0' else '1' end end pref3
         , case when nvl(clt4,'#') = '#' then '#' else case when pre4 is null then '0' else '1' end end pref4
         , case when nvl(clt5,'#') = '#' then '#' else case when pre5 is null then '0' else '1' end end pref5
         , case when nvl(clt6,'#') = '#' then '#' else case when pre6 is null then '0' else '1' end end pref6
         /*, case when pxc1 is null then 0 else 1 end pxc1
         , case when pxc2 is null then 0 else 1 end pxc2
         , case when pxc3 is null then 0 else 1 end pxc3
         , case when pxc4 is null then 0 else 1 end pxc4
         , case when pxc5 is null then 0 else 1 end pxc5
         , case when pxc6 is null then 0 else 1 end pxc6*/
         , d1,d2,d3,d4,d5,d6
		 , pn_cnt, none_cnt, par_cnt
         , t1, t2, t3, t4, t5, t6, t7, t8, t9, t0		 
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id = pn_drawing_id
	)
	select comb1, comb2, comb3, comb4, comb5, comb6, 
           fr1, fr2, fr3, fr4, fr5, fr6, 
           lt1, lt2, lt3, lt4, lt5, lt6, 
           primo1, primo2, primo3, primo4, primo5, primo6, 
           impar1, impar2, impar3, impar4, impar5, impar6, 
           par1, par2, par3, par4, par5, par6,
           chng1, chng2, chng3, chng4, chng5, chng6,
		   pref1, pref2, pref3, pref4, pref5, pref6,
--           pxc1, pxc2, pxc3, pxc4, pxc5, pxc6,
		   d1,d2,d3,d4,d5,d6,
		   pn_cnt, none_cnt, par_cnt,
		   t1, t2, t3, t4, t5, t6, t7, t8, t9, t0	
	  from resultados_tbl;
begin 
--DBMS_OUTPUT.PUT_LINE('-------------------------------');
--DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
--DBMS_OUTPUT.PUT_LINE('pn_drawing_id: '||pn_drawing_id);
	--!limpiando la tabla
	update olap_sys.predicciones_all
	   set match1 = 0
		 , match2 = 0
		 , match3 = 0
		 , match4 = 0
		 , match5 = 0
		 , match6 = 0
		 , match7 = 0
		 , match8 = 0
		 , match9 = 0
		 , match0 = 0
	 where prediccion_sorteo = ln$sorteo_anterior;	 
	
	for k in c_resultados(pn_drawing_id => pn_drawing_id) loop
		dbms_output.put_line('ley del tercio');
		dbms_output.put_line(k.lt1||' - '||k.lt2||' - '||k.lt3||' - '||k.lt4||' - '||k.lt5||' - '||k.lt6);
		--ley del tercio
		update olap_sys.predicciones_all
		   set res1 = k.lt1
		     , res2 = k.lt2
			 , res3 = k.lt3
			 , res4 = k.lt4
			 , res5 = k.lt5
			 , res6 = k.lt6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'LT') > 0;

		dbms_output.put_line('frecuencia');
		dbms_output.put_line(k.fr1||' - '||k.fr2||' - '||k.fr3||' - '||k.fr4||' - '||k.fr5||' - '||k.fr6);		
		--frecuencia
		update olap_sys.predicciones_all
		   set res1 = k.fr1
		     , res2 = k.fr2
			 , res3 = k.fr3
			 , res4 = k.fr4
			 , res5 = k.fr5
			 , res6 = k.fr6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'FR') > 0;
		
		dbms_output.put_line('numeros primos');
		dbms_output.put_line(k.primo1||' - '||k.primo2||' - '||k.primo3||' - '||k.primo4||' - '||k.primo5||' - '||k.primo6);		
		--numeros primos
		update olap_sys.predicciones_all
		   set res1 = k.primo1
		     , res2 = k.primo2
			 , res3 = k.primo3
			 , res4 = k.primo4
			 , res5 = k.primo5
			 , res6 = k.primo6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'PRIMO') > 0;

		dbms_output.put_line('numeros impares');
		dbms_output.put_line(k.impar1||' - '||k.impar2||' - '||k.impar3||' - '||k.impar4||' - '||k.impar5||' - '||k.impar6);
		--numeros impares
		update olap_sys.predicciones_all
		   set res1 = k.impar1
		     , res2 = k.impar2
			 , res3 = k.impar3
			 , res4 = k.impar4
			 , res5 = k.impar5
			 , res6 = k.impar6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'IMPAR') > 0;			   

		dbms_output.put_line('numeros pares');
		dbms_output.put_line(k.par1||' - '||k.par2||' - '||k.par3||' - '||k.par4||' - '||k.par5||' - '||k.par6);
		--numeros pares
		update olap_sys.predicciones_all
		   set res1 = k.par1
		     , res2 = k.par2
			 , res3 = k.par3
			 , res4 = k.par4
			 , res5 = k.par5
			 , res6 = k.par6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'PAR') > 0;			   

		dbms_output.put_line('numeros con cambio de posicion');
		dbms_output.put_line(k.chng1||' - '||k.chng2||' - '||k.chng3||' - '||k.chng4||' - '||k.chng5||' - '||k.chng6);
		--numeros pares
		update olap_sys.predicciones_all
		   set res1 = k.chng1
		     , res2 = k.chng2
			 , res3 = k.chng3
			 , res4 = k.chng4
			 , res5 = k.chng5
			 , res6 = k.chng6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'CHNG') > 0;	

		dbms_output.put_line('digits');
		dbms_output.put_line(k.comb1||' - '||k.comb2||' - '||k.comb3||' - '||k.comb4||' - '||k.comb5||' - '||k.comb6);
		--digits
		update olap_sys.predicciones_all
		   set res1 = k.comb1
		     , res2 = k.comb2
			 , res3 = k.comb3
			 , res4 = k.comb4
			 , res5 = k.comb5
			 , res6 = k.comb6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and (instr(prediccion_tipo,'DIGIT') > 0 or instr(prediccion_tipo,'POS') > 0);	

		dbms_output.put_line('numeros favorables');
		dbms_output.put_line(k.pref1||' - '||k.pref2||' - '||k.pref3||' - '||k.pref4||' - '||k.pref5||' - '||k.pref6);
		--numeros favorables
		update olap_sys.predicciones_all
		   set res1 = k.pref1
		     , res2 = k.pref2
			 , res3 = k.pref3
			 , res4 = k.pref4
			 , res5 = k.pref5
			 , res6 = k.pref6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'PXC_PREF') > 0;

/*		dbms_output.put_line('PXC');
		dbms_output.put_line(k.pxc1||' - '||k.pxc2||' - '||k.pxc3||' - '||k.pxc4||' - '||k.pxc5||' - '||k.pxc6);
		--pronostico por ciclo
		update olap_sys.predicciones_all
		   set res1 = k.pxc1
		     , res2 = k.pxc2
			 , res3 = k.pxc3
			 , res4 = k.pxc4
			 , res5 = k.pxc5
			 , res6 = k.pxc6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'PXC') > 0;*/

		dbms_output.put_line('DECENA');
		dbms_output.put_line(k.d1||' - '||k.d2||' - '||k.d3||' - '||k.d4||' - '||k.d5||' - '||k.d6);
		--decena
		update olap_sys.predicciones_all
		   set res1 = k.d1
		     , res2 = k.d2
			 , res3 = k.d3
			 , res4 = k.d4
			 , res5 = k.d5
			 , res6 = k.d6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'DECENA') > 0;	

		dbms_output.put_line('PRIMO, IMPAR, PAR');
		dbms_output.put_line(k.pn_cnt||' - '||k.none_cnt||' - '||k.par_cnt);
		--primos, impar, par
		update olap_sys.predicciones_all
		   set res1 = k.pn_cnt
		     , res2 = k.none_cnt
			 , res3 = k.par_cnt
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'PIP') > 0;			   

		dbms_output.put_line('TERMINACIONES');
		dbms_output.put_line(k.t1||' - '||k.t2||' - '||k.t3||' - '||k.t4||' - '||k.t5||' - '||k.t6||' - '||k.t7||' - '||k.t8||' - '||k.t9||' - '||k.t0);
		--terminaciones
		update olap_sys.predicciones_all
		   set res1 = k.t1
		     , res2 = k.t2
			 , res3 = k.t3
			 , res4 = k.t4
			 , res5 = k.t5
			 , res6 = k.t6
			 , res7 = k.t7
			 , res8 = k.t8
			 , res9 = k.t9
			 , res0 = k.t0
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'TERMINACIONES') > 0;	
		   
		--b1
		update olap_sys.predicciones_all
		   set res1 = k.comb1
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B1') > 0 or b_type = 'B1';

		--b2
		update olap_sys.predicciones_all
		   set res1 = k.comb2
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B2') > 0 or b_type = 'B2';

		--b3
		update olap_sys.predicciones_all
		   set res1 = k.comb3
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B3') > 0 or b_type = 'B3';

		--b4
		update olap_sys.predicciones_all
		   set res1 = k.comb4
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B4') > 0 or b_type = 'B4';

		--b5
		update olap_sys.predicciones_all
		   set res1 = k.comb5
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B5') > 0 or b_type = 'B5';

		--b6
		update olap_sys.predicciones_all
		   set res1 = k.comb6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and instr(prediccion_tipo,'B6') > 0 or b_type = 'B6';
	end loop;

    --actualizando las columnas match para las predicciones relacionadas a impar, par y primo
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = '0' and res1 = '0' then 0 when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = '0' and res2 = '0' then 0 when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = '0' and res3 = '0' then 0 when pred3 = res3 then 1 else 0 end
		 , match4 = case when pred4 = '0' and res4 = '0' then 0 when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = '0' and res5 = '0' then 0 when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = '0' and res6 = '0' then 0 when pred6 = res6 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and (instr(prediccion_tipo,'IMPAR') > 0
	    or  instr(prediccion_tipo,'PAR') > 0
		or  instr(prediccion_tipo,'PRIMO') > 0);
		
    --actualizando las columnas match para las predicciones relacionadas a CHNG, DIGIT, PXC_PREF, DECENA 
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = res3 then 1 else 0 end
		 , match4 = case when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = res6 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and (instr(prediccion_tipo,'CHNG') > 0
	    or  instr(prediccion_tipo,'DIGIT') > 0
        or  instr(prediccion_tipo,'PXC_PREF') > 0
        or  instr(prediccion_tipo,'PXC') > 0
		or  instr(prediccion_tipo,'DECENA') > 0
		or  instr(prediccion_tipo,'POS') > 0);

    --actualizando las columnas match para las predicciones relacionadas a PIP 
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = res3 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and instr(prediccion_tipo,'PIP') > 0;
		
    --actualizando las columnas match para las predicciones relacionadas a LT y FR
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = '#' and res1 = '#' then 0 when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = '#' and res2 = '#' then 0 when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = '#' and res3 = '#' then 0 when pred3 = res3 then 1 else 0 end
		 , match4 = case when pred4 = '#' and res4 = '#' then 0 when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = '#' and res5 = '#' then 0 when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = '#' and res6 = '#' then 0 when pred6 = res6 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and (instr(prediccion_tipo,'LT') > 0
	    or  instr(prediccion_tipo,'FR') > 0);

    --actualizando las columnas match para las predicciones relacionadas a TERMINACIONES
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = res1 then 1 else 0 end 
		 , match2 = case when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = res3 then 1 else 0 end 
		 , match4 = case when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = res6 then 1 else 0 end
		 , match7 = case when pred7 = res7 then 1 else 0 end
		 , match8 = case when pred8 = res8 then 1 else 0 end
		 , match9 = case when pred9 = res9 then 1 else 0 end
		 , match0 = case when pred0 = res0 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and instr(prediccion_tipo,'TERMINACIONES') > 0;

    --B1, B2, B3, B4, B5, B6
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = res1 then 1 else 0 end
	     , match_cnt = case when pred1 = res1 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and (instr(prediccion_tipo,'B1') > 0
	    or  instr(prediccion_tipo,'B2') > 0
        or  instr(prediccion_tipo,'B3') > 0
        or  instr(prediccion_tipo,'B4') > 0
		or  instr(prediccion_tipo,'B5') > 0
		or  instr(prediccion_tipo,'B6') > 0
		or  b_type = 'B1'
		or  b_type = 'B2'
		or  b_type = 'B3'
		or  b_type = 'B4'
		or  b_type = 'B5'
		or  b_type = 'B6');
		
    --actualizando el contador de match
	update olap_sys.predicciones_all
	   set match_cnt = match1 + match2 + match3 + match4 + match5 + match6
	     , prediccion_fecha = (select to_date(gambling_date,'DD-MM-YYYY') from olap_sys.pm_mr_resultados_v2 where gambling_id = ln$sorteo_anterior) 
	 where prediccion_sorteo = ln$sorteo_anterior;	

    --actualizando el contador de match de TERMINACIONES
	update olap_sys.predicciones_all
	   set match_cnt = match1 + match2 + match3 + match4 + match5 + match6 + match7 + match8 + match9 + match0
	     , prediccion_fecha = (select to_date(gambling_date,'DD-MM-YYYY') from olap_sys.pm_mr_resultados_v2 where gambling_id = ln$sorteo_anterior) 
	 where prediccion_sorteo = ln$sorteo_anterior
	   and instr(prediccion_tipo,'TERMINACIONES') > 0;		 
exception
 when others then
	DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
end upd_predicciones_all; 


procedure evaluate_prediccion_all(pn_drawing_id		number) is
LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'evaluate_prediccion_all'; 
ln$match_cnt				  number:= 0;
lf$match_pct				  float := 0.0;

begin 
DBMS_OUTPUT.PUT_LINE('-------------------------------');
DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);

	update olap_sys.predicciones_all
	   set match_cnt = case when pred1 = '#' then 0 else case when pred1 = res1 then 1 else 0 end end
					 + case when pred2 = '#' then 0 else case when pred2 = res2 then 1 else 0 end end
					 + case when pred3 = '#' then 0 else case when pred3 = res3 then 1 else 0 end end
					 + case when pred4 = '#' then 0 else case when pred4 = res4 then 1 else 0 end end
					 + case when pred5 = '#' then 0 else case when pred5 = res5 then 1 else 0 end end
					 + case when pred6 = '#' then 0 else case when pred6 = res6 then 1 else 0 end end
		 , match1 = case when pred1 = '#' then 0 else case when pred1 = res1 then 1 else 0 end end
		 , match2 = case when pred2 = '#' then 0 else case when pred2 = res2 then 1 else 0 end end
		 , match3 = case when pred3 = '#' then 0 else case when pred3 = res3 then 1 else 0 end end
		 , match4 = case when pred4 = '#' then 0 else case when pred4 = res4 then 1 else 0 end end
		 , match5 = case when pred5 = '#' then 0 else case when pred5 = res5 then 1 else 0 end end
		 , match6 = case when pred6 = '#' then 0 else case when pred6 = res6 then 1 else 0 end end
		 , updated_date = sysdate
	 where prediccion_sorteo = pn_drawing_id
	   and prediccion_tipo in ('LT');
	/*   
	update olap_sys.predicciones_all
	   set match_cnt = olap_sys.w_common_pkg.contar_igualdades (pred1||','||pred2||','||pred3||','||pred4||','||pred5||','||pred6
						  , res1||','||res2||','||res3||','||res4||','||res5||','||res6)
		 , updated_date = sysdate
	 where prediccion_sorteo = pn_drawing_id
	   and prediccion_tipo = 'Numerica';		 
	*/
	commit;
exception
 when others then
	DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
end evaluate_prediccion_all; 


procedure evaluate_prediccion_handler(pn_drawing_id               	number) is
LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'evaluate_prediccion_handler'; 
begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	upd_predicciones_all(pn_drawing_id => pn_drawing_id);

	--!se evaluan las predicciones con el resultado del sorteo			 
	evaluate_prediccion_all(pn_drawing_id => pn_drawing_id);
	
exception
 when others then
	DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
end evaluate_prediccion_handler; 


--!utilizar los metadatos para marcar como no optimas en la tabla de gl_automaticas_detail
procedure exe_porcentaje_c1_c3_c4_c6 (pn_list_id     number) is
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'exe_porcentaje_c1_c3_c4_c6'; 
	ln$upd_cnt					  NUMBER := 0;
	
	cursor c_jugadas_no_optimas is
	with jugadas_tbl as (
	select list_id
         , id
         , case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then 'PR' else 
		   case when mod(comb1,2) = 0 then 'PA' else 
		   case when mod(comb1,2) > 0 then 'IN' end end end c1
		 , case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then 'PR' else 
		   case when mod(comb3,2) = 0 then 'PA' else 
		   case when mod(comb3,2) > 0 then 'IN' end end end c3
		 , case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then 'PR' else 
		   case when mod(comb4,2) = 0 then 'PA' else 
		   case when mod(comb4,2) > 0 then 'IN' end end end c4       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then 'PR' else 
		   case when mod(comb6,2) = 0 then 'PA' else 
		   case when mod(comb6,2) > 0 then 'IN' end end end c6 
	  from olap_sys.gl_automaticas_detail
	 where list_id = pn_list_id 
      ) select *
          from jugadas_tbl;	
		  
begin
	--!inicializando los valores de la columna
	update olap_sys.gl_automaticas_detail
	   set jugar_flag = 'Y'
	 where list_id = pn_list_id;
		   
	gn$upd_cnt := 0;
	for k in c_jugadas_no_optimas loop
		update olap_sys.gl_automaticas_detail
		   set jugar_flag = 'N'
		 where list_id = k.list_id 
		   and id = k.id
		   and list_id = pn_list_id
		   and (k.c1, k.c3, k.c4, k.c6) in (select pos1, pos3, pos4,  pos6
									  from olap_sys.plan_jugada_details
									 where status = 'A'
									   and description = 'PCT_C1_C3_C4_C6');
		
		if sql%found then
			gn$upd_cnt := gn$upd_cnt + sql%rowcount;
		end if;	
	end loop;
	dbms_output.put_line(gn$upd_cnt||' registros actualizados.');
	commit;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end exe_porcentaje_c1_c3_c4_c6;	


--!insertar metadatos de los porcentajes de las posiciones de c1, c3, c4 y c6 en la tabla plan_jugada_details
procedure ins_porcentaje_c1_c3_c4_c6 (pn_jpctaccum		number) is
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_porcentaje_c1_c3_c4_c6'; 
	ln$ultimo_sorteo			  NUMBER := 0;
	
	cursor c_jugadas_no_optimas (pn_jpctaccum		number
							   , pn_total_jugadas   number) is
	with jugadas_tbl as (
	select case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then 'PR' else 
		   case when mod(comb1,2) = 0 then 'PA' else 
		   case when mod(comb1,2) > 0 then 'IN' end end end c1
		 , case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then 'PR' else 
		   case when mod(comb3,2) = 0 then 'PA' else 
		   case when mod(comb3,2) > 0 then 'IN' end end end c3
		 , case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then 'PR' else 
		   case when mod(comb4,2) = 0 then 'PA' else 
		   case when mod(comb4,2) > 0 then 'IN' end end end c4       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then 'PR' else 
		   case when mod(comb6,2) = 0 then 'PA' else 
		   case when mod(comb6,2) > 0 then 'IN' end end end c6 
	  from olap_sys.w_combination_responses_fs
	), resultados_tbl as (
	select case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then 'PR' else 
		   case when mod(comb1,2) = 0 then 'PA' else 
		   case when mod(comb1,2) > 0 then 'IN' end end end c1
		 , case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then 'PR' else 
		   case when mod(comb3,2) = 0 then 'PA' else 
		   case when mod(comb3,2) > 0 then 'IN' end end end c3
		 , case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then 'PR' else 
		   case when mod(comb4,2) = 0 then 'PA' else 
		   case when mod(comb4,2) > 0 then 'IN' end end end c4       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then 'PR' else 
		   case when mod(comb6,2) = 0 then 'PA' else 
		   case when mod(comb6,2) > 0 then 'IN' end end end c6 
	  from olap_sys.sl_gamblings
	), resultados_group_tbl as (
	select 
			 c1,
			 c3,
			 c4,
			 c6,
			count(1) rcnt
		from resultados_tbl
	   group by 
			 c1,
			 c3,
			 c4,
			 c6
	), salidas_tbl as ( 
	select 
			 j.c1,
			 j.c3,
			 j.c4,
			 j.c6,
			count(1) jcnt,
			nvl((select rcnt from resultados_group_tbl r where r.c1=j.c1 and r.c3=j.c3 and r.c4=j.c4 and r.c6=j.c6),0) rcnt
		from jugadas_tbl j
	   group by 
			 j.c1,
			 j.c3,
			 j.c4,
			 j.c6
	), salidas_pct_tbl as ( select c1
		   , c3
		   , c4
		   , c6
		   , jcnt
		   , rcnt
		   , round(((jcnt/pn_total_jugadas)*100)) jpct
		   , round(sum(round(((jcnt/pn_total_jugadas)*100),2)) over (order by jcnt desc) - round(((jcnt/pn_total_jugadas)*100),2) + round(((jcnt/pn_total_jugadas)*100),2)) jpctaccum
		from salidas_tbl
	)
	select 
		   c1, 
		   c3, 
		   c4, 
		   c6, 
		   jcnt, 
		   rcnt, 
		   jpct,
		   jpctaccum
	  from salidas_pct_tbl
	 where jpctaccum >= pn_jpctaccum 
	 order by jcnt desc;  	
begin
	--!recuperando el ultimo ID del sorteo solo como referencia
	select max(gambling_id)
      into ln$ultimo_sorteo
      from olap_sys.sl_gamblings;
	
	--!limpiando info de la tabla
	delete olap_sys.plan_jugada_details
	 where description = 'PCT_C1_C3_C4_C6';
	
	for k in c_jugadas_no_optimas (pn_jpctaccum	    => pn_jpctaccum
							     , pn_total_jugadas => CN$TOTAL_JUGADAS) loop
		olap_sys.w_new_pick_panorama_pkg.ins_plan_jugada_details(
								  pn_plan_jugada_id		=> 503
								, pv_pos1				=> k.c1
								, pv_pos2				=> '#'
								, pv_pos3				=> k.c3
								, pv_pos4				=> k.c4
								, pv_pos5				=> '#'
								, pv_pos6				=> k.c6
								, pv_comments			=> k.jpctaccum
								, pn_jugadas_cnt        => k.jcnt
								, pn_resultados_cnt     => k.rcnt
								, pv_descripcion		=> 'PCT_C1_C3_C4_C6'
								, pv_seq_no				=> ln$ultimo_sorteo
							 	 );			
		gn$ins_cnt := gn$ins_cnt + sql%rowcount;								
	end loop;
	dbms_output.put_line(gn$ins_cnt||' registros insertados.');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_porcentaje_c1_c3_c4_c6;	

	
--!insertar metadatos de los porcentajes de las posiciones de c1, c3, c4 y c6 en la tabla plan_jugada_details
--!utilizar los metadatos para marcar como no optimas en la tabla de gl_automaticas_detail
procedure porcentaje_c1_c3_c4_c6_handler(pb_insert_flag			boolean default true
									   , pb_validator_flag		boolean default false
									   , pn_jpctaccum			number  default 80) is
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'porcentaje_c1_c3_c4_c6_handler'; 
begin
	if pb_insert_flag then
		ins_porcentaje_c1_c3_c4_c6(pn_jpctaccum => pn_jpctaccum);
	end if;	
	
	for k in c_automaticas_header loop
		if pb_validator_flag then
			exe_porcentaje_c1_c3_c4_c6(pn_list_id => k.list_id);
		end if;
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end porcentaje_c1_c3_c4_c6_handler;	


--!aplicar filtros para seleccionar la franja con jugadas mas ganadoras en base a calculos hechos en Excel
--!y marcar status = Y para las jugadas que cumplan
procedure filtrar_jugadas_decenas is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'filtrar_jugadas_decenas'; 
	ln$upd_cnt					  NUMBER := 0;
	ln$loop_cnt					  NUMBER := 0;
	ln$upd_cnt_total			  NUMBER := 0;
	
	
	cursor c_main is
	with jugadas_tbl as (
	select comb_sum j_comb_sum
		 , count(1) jcnt 
	  from olap_sys.w_combination_responses_fs
	 --!solo las decenas que interesan
	 where (d1, d2, d3, d4, d5, d6) in (
										('1-9','10-19','10-19','20-29','20-29','30-39')
									  , ('1-9','10-19','10-19','20-29','30-39','30-39')
									  , ('1-9','10-19','20-29','20-29','30-39','30-39')
									  , ('1-9','1-9','10-19','10-19','20-29','30-39')
									  , ('1-9','1-9','10-19','20-29','20-29','30-39')
									  , ('1-9','1-9','10-19','20-29','30-39','30-39')
									  , ('1-9','10-19','10-19','10-19','20-29','30-39')
									  , ('1-9','10-19','20-29','20-29','20-29','30-39')
									  , ('1-9','10-19','20-29','30-39','30-39','30-39')
									  , ('1-9','1-9','1-9','10-19','20-29','30-39')
									  , ('1-9','1-9','10-19','10-19','30-39','30-39')
									  , ('1-9','1-9','20-29','20-29','30-39','30-39')
									  , ('1-9','10-19','10-19','10-19','30-39','30-39')
									  , ('1-9','10-19','10-19','30-39','30-39','30-39')
									  , ('1-9','1-9','10-19','30-39','30-39','30-39')
									  , ('1-9','1-9','20-29','20-29','20-29','30-39')
									  , ('1-9','1-9','1-9','20-29','30-39','30-39')) 
	 group by comb_sum
	)
	, resultados_tbl as (
	select comb_sum r_comb_sum
		 , count(1) rcnt 
	  from olap_sys.sl_gamblings
	 group by comb_sum
	)
	, jugadas_filtradas_tbl as (
	select j_comb_sum
		 , jcnt
		 , nvl((select rcnt from resultados_tbl where r_comb_sum = j_comb_sum),0) rcnt
	  from jugadas_tbl   
	 --!valor fijo derivado de hoja de excel
	 where jcnt >= 33135
	) 
	, percentile_jugadas_tbl as (
	select percentile_disc(0.4) within group (order by rcnt) per_rcnt
	  from jugadas_filtradas_tbl
	)
	, decenas_jugadas_tbl as (
	select d1
		 , d2
		 , d3
		 , d4
		 , d5
		 , d6
		 , comb_sum
		 , count(1) jcnt
	  from olap_sys.w_combination_responses_fs
	 where 1=1
	   and co_cnt = 1 
	   and comb1 between 1 and 9
	   and comb6 between 30 and 39
	   and comb_sum in (select j_comb_sum
						  from jugadas_filtradas_tbl
						 where rcnt >= (select per_rcnt from percentile_jugadas_tbl))
	 group by d1
		 , d2
		 , d3
		 , d4
		 , d5
		 , d6
		 , comb_sum
	)
	, decenas_resultados_tbl as (
	select d1
		 , d2
		 , d3
		 , d4
		 , d5
		 , d6
		 , comb_sum
		 , count(1) rcnt 
	  from olap_sys.pm_mr_resultados_v2
	 where comb1 between 1 and 9
	   and comb6 between 30 and 39
	   and comb_sum in (select j_comb_sum
						  from jugadas_filtradas_tbl
						 where rcnt >= (select per_rcnt from percentile_jugadas_tbl))
	 group by d1
		 , d2
		 , d3
		 , d4
		 , d5
		 , d6
		 , comb_sum                     
	)
	, jugadas_finales_tbl as (
	select j.d1
		 , j.d2
		 , j.d3
		 , j.d4
		 , j.d5
		 , j.d6
		 , j.comb_sum
		 , jcnt
		 , nvl((select rcnt from decenas_resultados_tbl r where r.d1=j.d1 and r.d2=j.d2 and r.d3=j.d3 and r.d4=j.d4 and r.d5=j.d5 and r.d6=j.d6 and r.comb_sum=j.comb_sum),0) rcnt
	  from decenas_jugadas_tbl j
	)
	select *
	  from jugadas_finales_tbl
	 where rcnt > 0
     order by rcnt desc;  	 

	cursor c_details (pv_d1      	varchar2
					, pv_d2      	varchar2
					, pv_d3      	varchar2
					, pv_d4      	varchar2
					, pv_d5      	varchar2
					, pv_d6      	varchar2
					, pn_comb_sum 	number) is
	with jugadas_tbl as (
	select seq_id, comb1, comb2, comb3, comb4, comb5, comb6
		 , case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then 'PR' else 
		   case when mod(comb1,2) = 0 then 'PA' else 
		   case when mod(comb1,2) > 0 then 'IN' end end end s_comb1
		 , case when olap_sys.w_common_pkg.is_prime_number(comb2) = 1 then 'PR' else 
		   case when mod(comb2,2) = 0 then 'PA' else 
		   case when mod(comb2,2) > 0 then 'IN' end end end s_comb2       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then 'PR' else 
		   case when mod(comb3,2) = 0 then 'PA' else 
		   case when mod(comb3,2) > 0 then 'IN' end end end s_comb3
		 , case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then 'PR' else 
		   case when mod(comb4,2) = 0 then 'PA' else 
		   case when mod(comb4,2) > 0 then 'IN' end end end s_comb4 
		 , case when olap_sys.w_common_pkg.is_prime_number(comb5) = 1 then 'PR' else 
		   case when mod(comb5,2) = 0 then 'PA' else 
		   case when mod(comb5,2) > 0 then 'IN' end end end s_comb5       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then 'PR' else 
		   case when mod(comb6,2) = 0 then 'PA' else 
		   case when mod(comb6,2) > 0 then 'IN' end end end s_comb6
	  from olap_sys.w_combination_responses_fs
	 where 1=1
	   --!solo jugadas con numeros consecutivos
	   and co_cnt = 1 
	   --!solo jugadas donde la posicion1 este entre 1 y 9
	   and comb1 between 1 and 9
	   --!solo jugadas donde la posicion6 este entre 30 y 39
	   and comb6 between 30 and 39
	   --!basicamente filtrar las terminaciones = 3 y las otras con menos incidencias 
	   and (pn_cnt, none_cnt, par_cnt, t2_cnt) not in ((2,3,1,1)
												 , (3,2,1,2)
												 , (3,2,1,0)
												 , (2,3,1,2)
												 , (2,3,1,0)
												 , (2,2,2,3)
												 , (3,1,2,3)
												 , (2,1,3,3)
												 , (3,0,3,3)
												 , (3,2,1,3)
												 , (2,3,1,3))
	   and d1 = pv_d1
	   and d2 = pv_d2
	   and d3 = pv_d3
	   and d4 = pv_d4
	   and d5 = pv_d5
	   and d6 = pv_d6
	   and comb_sum = pn_comb_sum 
	)
	, primo_impar_par_tbl as (
	select seq_id, comb1, comb2, comb3, comb4, comb5, comb6
		 , decode(s_comb1,'PR',1,0) + decode(s_comb2,'PR',1,0) + decode(s_comb3,'PR',1,0) + decode(s_comb4,'PR',1,0) + decode(s_comb5,'PR',1,0) + decode(s_comb6,'PR',1,0) primos
		 , decode(s_comb1,'IN',1,0) + decode(s_comb2,'IN',1,0) + decode(s_comb3,'IN',1,0) + decode(s_comb4,'IN',1,0) + decode(s_comb5,'IN',1,0) + decode(s_comb6,'IN',1,0) impar
		 , decode(s_comb1,'PA',1,0) + decode(s_comb2,'PA',1,0) + decode(s_comb3,'PA',1,0) + decode(s_comb4,'PA',1,0) + decode(s_comb5,'PA',1,0) + decode(s_comb6,'PA',1,0) par
	  from jugadas_tbl
	)
	select *
	  from primo_impar_par_tbl
	 --!patron encontrado en las 1000 jugadas de gl
	 where (primos, impar, par) in ((2,2,2)                              
								  , (2,1,3)
								  , (2,3,1)
								  , (3,1,2)
								  , (3,0,3)
								  , (3,2,1));
begin
	ln$upd_cnt_total := 0;
	for m in c_main loop
		ln$upd_cnt := 0;
		for d in c_details (pv_d1      	=> m.D1 
						  , pv_d2      	=> m.D2 
						  , pv_d3      	=> m.D3 
						  , pv_d4      	=> m.D4 
						  , pv_d5      	=> m.D5 
						  , pv_d6      	=> m.D6 
						  , pn_comb_sum => m.COMB_SUM) loop
		
			--!status temporal
			update olap_sys.w_combination_responses_fs
			   set status = 'T'	
			 where seq_id = d.seq_id; 
			
			ln$upd_cnt := ln$upd_cnt + 1;
			ln$upd_cnt_total := ln$upd_cnt_total + 1;		
		end loop;
		ln$loop_cnt := ln$loop_cnt + 1;	
		dbms_output.put_line(m.D1||' - '||m.D2||' - '||m.D3||' - '||m.D4||' - '||m.D5||' - '||m.D6||' - '||m.COMB_SUM||' - '||ln$upd_cnt||' rows updated.');
		commit;		
	end loop;
	
	--!el siguiente patron de decenas pertenecen los resultados de 5 o 6 aciertos hechas por gigaloterias
	update olap_sys.w_combination_responses_fs
	   set status = 'Y'	
	 where status = 'T'	 
	   and D1 = '1-9'
       and D2 IN ('1-9','10-19')
       and D3 IN ('10-19','20-29')
       and D4 = '20-29'
       and D5 = '30-39'
       and D6 = '30-39';
	   
	commit;
	dbms_output.put_line(ln$upd_cnt_total||' total rows updated.');
	dbms_output.put_line(ln$loop_cnt||' loops.');
exception
  when others then
--    rollback;
	--!habilitando el trigger de la tabla
	EXECUTE IMMEDIATE 'ALTER TRIGGER OLAP_SYS.AIU_W_COMBINATION_RESPONSES_FS ENABLEN';
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end filtrar_jugadas_decenas;		

--!Ingesta de jugadas en tabla gl_automaticas_detail en base a jugadas en tabla w_combination_responses_fs
procedure ins_gl_automaticas is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_gl_automaticas'; 
	ln$oop_cnt      number := 1;
    
    cursor c_main is
    select comb1
         , comb2
         , comb3
         , comb4
         , comb5
         , comb6
         , 2 list_id
     from olap_sys.w_combination_responses_fs
    where status = 'Y'
    order by comb1
         , comb2
         , comb3
         , comb4
         , comb5
         , comb6;
begin
    for k in c_main loop
        OLAP_SYS.W_GL_AUTOMATICAS_PKG.INS_GL_AUTOMATICAS_HANDLER(pn_id => ln$oop_cnt
                                                               , pn_ia1 => k.comb1
                                                               , pn_ia2 => k.comb2
                                                               , pn_ia3 => k.comb3
                                                               , pn_ia4 => k.comb4
                                                               , pn_ia5 => k.comb5
                                                               , pn_ia6 => k.comb6
                                                               , pn_list_id => k.list_id);
        ln$oop_cnt := ln$oop_cnt + 1;
    end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_gl_automaticas;

--!inicializar el status = N en la tabla w_combination_responses_fs
--!aplicar filtros para seleccionar la franja con jugadas mas ganadoras en base a calculos hechos en Excel
--!y marcar status = Y para las jugadas que cumplan
procedure filtrar_jugadas_handler (pv_insert_flag   varchar2 default 'Y') is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'filtrar_jugadas_handler'; 
begin
	--!deshabilitando el trigger de la tabla
	EXECUTE IMMEDIATE 'ALTER TRIGGER OLAP_SYS.AIU_W_COMBINATION_RESPONSES_FS DISABLE';
	
	--inicializando el status de cada jugada
	UPDATE OLAP_SYS.W_COMBINATION_RESPONSES_FS
	   SET STATUS = 'N'
	 WHERE STATUS IN ('Y','N');  
	
	--!aplicar filtros y marcar status = Y para las jugadas que cumplan
	filtrar_jugadas_decenas;
	
	if pv_insert_flag = 'Y' then
		--!Ingesta de jugadas en tabla gl_automaticas_detail en base a jugadas en tabla w_combination_responses_fs
		ins_gl_automaticas;
	end if;
	
	--!habilitando el trigger de la tabla
	EXECUTE IMMEDIATE 'ALTER TRIGGER OLAP_SYS.AIU_W_COMBINATION_RESPONSES_FS ENABLE';
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end filtrar_jugadas_handler;	

--!insertar las predicciones en la nueva tabla predicciones
procedure ins_predicciones_all(pv_nombre				varchar2
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
							 , pf_pres0					float default 0.0
							 , pv_b_type				varchar2 default null) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_predicciones_all';
begin
	--!insertando el nuevo registro de la prediccion
	insert into olap_sys.predicciones_all (prediccion_id,
										   prediccion_nombre,
										   prediccion_sorteo,
										   prediccion_tipo,
										   siguiente_sorteo1,
										   pred1,
										   pres1,
										   siguiente_sorteo2,
										   pred2,
										   pres2,
										   siguiente_sorteo3,
										   pred3,
										   pres3,
										   siguiente_sorteo4,
										   pred4,
										   pres4,
										   siguiente_sorteo5,
										   pred5,
										   pres5,
										   siguiente_sorteo6,
										   pred6,
										   pres6,
										   siguiente_sorteo7,
										   pred7,
										   pres7,
										   siguiente_sorteo8,
										   pred8,
										   pres8,
										   siguiente_sorteo9,
										   pred9,
										   pres9,
										   siguiente_sorteo0,
										   pred0,
										   pres0,
										   b_type)
	values ((select nvl(max(prediccion_id),0)+1 from olap_sys.predicciones_all)
	      , pv_nombre
	      , pn_sorteo
	      , pv_tipo
		  , case when instr(pv_tipo,'14.') > 0 or instr(pv_tipo,'15.') > 0 then pn_sorteo + 1 else pn_sig_sorteo1 end
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred1)	
	      , pf_pres1
		  , pn_sig_sorteo2
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred2)
	      , pf_pres2
		  , pn_sig_sorteo3
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred3)
	      , pf_pres3
		  , pn_sig_sorteo4
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred4)
	      , pf_pres4
		  , pn_sig_sorteo5
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred5)
	      , pf_pres5
		  , pn_sig_sorteo6
	      , olap_sys.w_common_pkg.transformar_valor_posicion(pv_tipo => pv_tipo, pv_pred => pv_pred6)
	      , pf_pres6
		  , pn_sig_sorteo7
		  , pv_pred7
		  , pf_pres7
		  , pn_sig_sorteo8
		  , pv_pred8
		  , pf_pres8
		  , pn_sig_sorteo9
		  , pv_pred9
		  , pf_pres9
		  , pn_sig_sorteo0
		  , pv_pred0
		  , pf_pres0
		  , case when instr(pv_tipo,'14.') > 0 then substr(pv_tipo, instr(pv_tipo,'.',1,1) + 1) else pv_b_type end);
	commit;		  
exception
  when others then
--	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end ins_predicciones_all;

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
								 , pf_pres0					float default 0.0) is	
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'predicciones_all_handler'; 
begin		
	--!insertar las predicciones en la nueva tabla predicciones
	ins_predicciones_all(pv_nombre => pv_nombre
					   , pn_sorteo => pn_sorteo
					   , pv_tipo   => pv_tipo
					   , pn_sig_sorteo1 => pn_sig_sorteo1
					   , pv_pred1  => pv_pred1
					   , pf_pres1  => pf_pres1
					   , pn_sig_sorteo2 => pn_sig_sorteo2
					   , pv_pred2  => pv_pred2
					   , pf_pres2  => pf_pres2
					   , pn_sig_sorteo3 => pn_sig_sorteo3
					   , pv_pred3  => pv_pred3
					   , pf_pres3  => pf_pres3
					   , pn_sig_sorteo4 => pn_sig_sorteo4
					   , pv_pred4  => pv_pred4
					   , pf_pres4  => pf_pres4
					   , pn_sig_sorteo5 => pn_sig_sorteo5
					   , pv_pred5  => pv_pred5
					   , pf_pres5  => pf_pres5
					   , pn_sig_sorteo6 => pn_sig_sorteo6
					   , pv_pred6  => pv_pred6
					   , pf_pres6  => pf_pres6					   
					   , pn_sig_sorteo7 => pn_sig_sorteo7
					   , pv_pred7  => pv_pred7
					   , pf_pres7  => pf_pres7
					   , pn_sig_sorteo8 => pn_sig_sorteo8
					   , pv_pred8  => pv_pred8
					   , pf_pres8  => pf_pres8
					   , pn_sig_sorteo9 => pn_sig_sorteo9
					   , pv_pred9  => pv_pred9
					   , pf_pres9  => pf_pres9
					   , pn_sig_sorteo0 => pn_sig_sorteo0
					   , pv_pred0  => pv_pred0
					   , pf_pres0  => pf_pres0);			   
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end predicciones_all_handler;								 


--!insertar las predicciones en la nueva tabla predicciones
procedure predicciones_short_handler(pv_nombre				varchar2
								   , pn_sorteo				number							 
								   , pv_tipo				varchar2
								   , pn_sig_sorteo1         number
								   , pv_pred1				varchar2
								   , pf_pres1				float
								   , pv_b_type				varchar2) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'predicciones_short_handler'; 
begin		
	--!insertar las predicciones en la nueva tabla predicciones
	ins_predicciones_all(pv_nombre => pv_nombre
					   , pn_sorteo => pn_sorteo
					   , pv_tipo   => pv_tipo
					   , pn_sig_sorteo1 => pn_sig_sorteo1
					   , pv_pred1  => pv_pred1
					   , pf_pres1  => pf_pres1
					   , pv_b_type => pv_b_type);	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end predicciones_short_handler;	
								 
								 
--!insertar n registros en la tabla, los cuales seran actualizados en procesos posteriores
procedure ins_digit_counts(pn_rec_intertados		number
						 , pn_drawing_id			number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_digit_counts'; 
begin
	for i in 1..pn_rec_intertados loop
		insert into olap_sys.digit_counts (id, drawing_id) values ((select nvl(max(id),0) + 1 from olap_sys.digit_counts), pn_drawing_id);
	end loop;
	commit;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_digit_counts;


--!actualizar registros en la tabla
procedure upd_digit_data(pn_drawing_id			number
					   , pn_column_num		    number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_digit_data'; 
	ln$loop_index		          number := 1;
	lv$upd_stmt				      varchar2(10000);	
	lrc$ref_cursor          	  SYS_REFCURSOR;	
	pn$comb 					  number := 0;	
	pn$j_cnt 					  number := 0;
	pn$r_cnt					  number := 0;
	pn$r_probabilidad			  number := 0;
	pn$r_last_id				  number := 0;
	pn$r_diff					  number := 0;
begin
	--!inicializando index
	olap_sys.w_common_pkg.g_index := 1;
	
	--!construir select statement
	olap_sys.w_common_pkg.g_dml_stmt := 'with jugadas_tbl as (';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' select ia'||pn_column_num;
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , count(1) j_cnt';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.gl_automaticas_detail';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' group by ia'||pn_column_num||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , resultados_tbl as (';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' select comb'||pn_column_num;
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , max(gambling_id) r_last_id';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , count(1) r_cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.pm_mr_resultados_v2';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' group by comb'||pn_column_num||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' select ia'||pn_column_num;
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , j_cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , (select r_cnt from resultados_tbl where comb'||pn_column_num||'=ia'||pn_column_num||') r_cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , round(((select r_cnt from resultados_tbl where comb'||pn_column_num||'=ia'||pn_column_num||')/j_cnt)*100,2) r_prob';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , (select r_last_id from resultados_tbl where comb'||pn_column_num||'=ia'||pn_column_num||') r_last_id';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , '||pn_drawing_id||' - (select r_last_id from resultados_tbl where comb'||pn_column_num||'=ia'||pn_column_num||') r_diff';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'   from jugadas_tbl';   
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by j_cnt desc';

	--!cerrando cursor
	if lrc$ref_cursor%isopen then
		close lrc$ref_cursor;
	end if;	

   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into pn$comb, pn$j_cnt, pn$r_cnt, pn$r_probabilidad, pn$r_last_id, pn$r_diff;
		exit when lrc$ref_cursor%notfound;	
		
		--!construir update statement
		lv$upd_stmt := 'update olap_sys.digit_counts';
		lv$upd_stmt := 	lv$upd_stmt || ' set comb'||pn_column_num||' = '||pn$comb;
		lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_j_cnt = '||pn$j_cnt;
		lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_r_cnt = '||pn$r_cnt;
		lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_r_prob = '||pn$r_probabilidad;
		lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_r_last_id = '||pn$r_last_id;
		lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_r_diff = '||pn$r_diff;
		lv$upd_stmt := 	lv$upd_stmt || ' where id = '||olap_sys.w_common_pkg.g_index; 	
		--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => lv$upd_stmt);
		
		--!ejecutar el update
		execute immediate lv$upd_stmt;
		
		--!incrementar el index
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   close lrc$ref_cursor;
   commit;
exception
  when others then
    close lrc$ref_cursor;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_digit_data;


--!actualizar secuencias y topn en la tabla
procedure upd_digit_counts(pn_topn					number
                         , pn_column_num		    number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_digit_counts'; 
	ln$loop_index		          number := 1;
	lv$upd_stmt				      varchar2(10000);	
	lrc$ref_cursor          	  SYS_REFCURSOR;	
	pn$comb 					  number := 0;	
	pn$j_cnt 					  number := 0;
	pn$r_cnt					  number := 0;
begin
	--!JUGADAS
	--!inicializando index
	olap_sys.w_common_pkg.g_index := 1;
	
	--!construir select statement
	olap_sys.w_common_pkg.g_dml_stmt := 'select comb'||pn_column_num||'_j_cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.digit_counts';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where comb'||pn_column_num||'_j_cnt > 0';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by 1 desc';

	--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);

	--!cerrando cursor
	if lrc$ref_cursor%isopen then
		close lrc$ref_cursor;
	end if;	

   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into pn$j_cnt;
		exit when lrc$ref_cursor%notfound;	
		
		--!construir update statement
		lv$upd_stmt := 'update olap_sys.digit_counts';
		lv$upd_stmt := 	lv$upd_stmt || ' set comb'||pn_column_num||'_seq_j_cnt = '||olap_sys.w_common_pkg.g_index;
		if olap_sys.w_common_pkg.g_index <= pn_topn then
			lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_topn_seq_j_cnt = '||chr(39)||'Y'||chr(39);
		end if;
		lv$upd_stmt := 	lv$upd_stmt || ' where comb'||pn_column_num||'_j_cnt = '||pn$j_cnt; 	
		--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => lv$upd_stmt);
		
		--!ejecutar el update
		execute immediate lv$upd_stmt;
		
		--!incrementar el index
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   close lrc$ref_cursor;

	--!RESULTADOS
	--!inicializando index
	olap_sys.w_common_pkg.g_index := 1;
	
	--!construir select statement
	olap_sys.w_common_pkg.g_dml_stmt := 'select comb'||pn_column_num||'_r_cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.digit_counts';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where comb'||pn_column_num||'_r_cnt > 0';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by 1 desc';

	--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);

	--!cerrando cursor
	if lrc$ref_cursor%isopen then
		close lrc$ref_cursor;
	end if;	

   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into pn$r_cnt;
		exit when lrc$ref_cursor%notfound;	
		
		--!construir update statement
		lv$upd_stmt := 'update olap_sys.digit_counts';
		lv$upd_stmt := 	lv$upd_stmt || ' set comb'||pn_column_num||'_seq_r_cnt = '||olap_sys.w_common_pkg.g_index;
		if olap_sys.w_common_pkg.g_index <= pn_topn then
			lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_topn_seq_r_cnt = '||chr(39)||'Y'||chr(39);
		end if;
		lv$upd_stmt := 	lv$upd_stmt || ' where comb'||pn_column_num||'_r_cnt = '||pn$r_cnt;	
		--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => lv$upd_stmt);
		
		--!ejecutar el update
		execute immediate lv$upd_stmt;
		
		--!incrementar el index
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   close lrc$ref_cursor;

	--!PROBABILIDAD
	--!inicializando index
	olap_sys.w_common_pkg.g_index := 1;
	
	--!construir select statement para diferencias
	olap_sys.w_common_pkg.g_dml_stmt := 'select comb'||pn_column_num||'_r_prob';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.digit_counts';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where comb'||pn_column_num||'_r_prob > 0';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by comb'||pn_column_num||'_r_prob, comb'||pn_column_num||'_r_cnt desc';

	--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);

	--!cerrando cursor
	if lrc$ref_cursor%isopen then
		close lrc$ref_cursor;
	end if;	

   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into pn$r_cnt;
		exit when lrc$ref_cursor%notfound;	
		
		--!construir update statement
		lv$upd_stmt := 'update olap_sys.digit_counts';
		lv$upd_stmt := 	lv$upd_stmt || ' set comb'||pn_column_num||'_seq_r_prob = '||olap_sys.w_common_pkg.g_index;
		if olap_sys.w_common_pkg.g_index <= pn_topn then
			lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_topn_seq_r_prob = '||chr(39)||'Y'||chr(39);
		end if;
		lv$upd_stmt := 	lv$upd_stmt || ' where comb'||pn_column_num||'_r_prob = '||pn$r_cnt;	
		--olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => lv$upd_stmt);
		
		--!ejecutar el update
		execute immediate lv$upd_stmt;
		
		--!incrementar el index
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   close lrc$ref_cursor;

	--!DIFERENCIA GAMBLING_ID - LAST_DRAWING_ID
	--!inicializando index
	olap_sys.w_common_pkg.g_index := 1;
	
	--!construir select statement para diferencias
	olap_sys.w_common_pkg.g_dml_stmt := 'select comb'||pn_column_num||'_r_diff';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.digit_counts';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where comb'||pn_column_num||'_r_diff > 0';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by comb'||pn_column_num||'_r_diff, comb'||pn_column_num||'_r_cnt';

	olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);

	--!cerrando cursor
	if lrc$ref_cursor%isopen then
		close lrc$ref_cursor;
	end if;	

   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into pn$r_cnt;
		exit when lrc$ref_cursor%notfound;	
		
		--!construir update statement
		lv$upd_stmt := 'update olap_sys.digit_counts';
		lv$upd_stmt := 	lv$upd_stmt || ' set comb'||pn_column_num||'_seq_r_diff = '||olap_sys.w_common_pkg.g_index;
		if olap_sys.w_common_pkg.g_index <= pn_topn then
			lv$upd_stmt := 	lv$upd_stmt || ', comb'||pn_column_num||'_topn_r_diff = '||chr(39)||'Y'||chr(39);
		end if;
		lv$upd_stmt := 	lv$upd_stmt || ' where comb'||pn_column_num||'_r_diff = '||pn$r_cnt;	
		olap_sys.w_new_pick_panorama_pkg.ins_tmp_testing (pv_valor => lv$upd_stmt);
		
		--!ejecutar el update
		execute immediate lv$upd_stmt;
		
		--!incrementar el index
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   close lrc$ref_cursor;
   
   commit;
exception
  when others then
    close lrc$ref_cursor;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_digit_counts;

--!insertar conteo de los digitos acerca de jugadas y resultados
procedure digit_counts_handler(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'digit_counts_handler'; 
	CN$REC_INSERTED		 CONSTANT NUMBER := 30;
	CN$TOPN				 CONSTANT NUMBER := 10;
begin
	--!limpiar el contenido de la tabla	
	delete olap_sys.digit_counts;
	
	--!insertar n registros en la tabla, los cuales seran actualizados en procesos posteriores
	ins_digit_counts(pn_rec_intertados => CN$REC_INSERTED
				   , pn_drawing_id => pn_drawing_id);
				   
	--!actualizar registros en la tabla
	for i in 1..6 loop
		--!actualizar registros en la tabla
		upd_digit_data(pn_drawing_id => pn_drawing_id
					   , pn_column_num => i);
					   
		--!actualizar secuencias y topn en la tabla
		upd_digit_counts(pn_topn => CN$TOPN
                       , pn_column_num => i);
	end loop;			   
						 
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end digit_counts_handler;

--!mostrar las predicciones para cada b_type para el ultimo sorteo
procedure comparativo_lt_handler is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'comparativo_lt_handler'; 
	CN$HISTORY		 	 CONSTANT NUMBER := 100;

	cursor c_primary (pn_history	number) is
	with resultado_tbl as (
	select max(gambling_id) max_id from olap_sys.sl_gamblings
	)
	, lt_history_tbl as (
	select *
	  from olap_sys.ley_tercio_history_dtl
	 where winner_flag is not null
	   and next_drawing_id >= (select max_id - pn_history from resultado_tbl)
	) 
	, lt_history_cnt_tbl as (
	select b_type
		 , lt
		 , lt_cnt_rank rank_cnt 
		 , count(1) cnt
	  from olap_sys.ley_tercio_history_dtl
	 where winner_flag is not null
	group by b_type
		 , lt
		 , lt_cnt_rank
	)
	, output_tbl as (
	select b_type
		 , lt
		 , rank_cnt
		 , cnt
		 , (select min(next_drawing_id) from lt_history_tbl) min_id
		 , (select max(next_drawing_id) from lt_history_tbl) max_id
		 ,  pn_history history_cnt 
	  from lt_history_cnt_tbl
	) 
	, output_sum_tbl as (
	select b_type b_typex
		 , rank_cnt rank_cntx
		 , sum(cnt) sum_cnt
	  from output_tbl
	 group by b_type
		 , rank_cnt 
	)
	, output_rank_tbl as (
	select DISTINCT b_type
		 , rank_cnt
		 , round(((select sum_cnt from output_sum_tbl where b_typex=b_type and rank_cntx=rank_cnt)/(select sum(sum_cnt) from output_sum_tbl where b_typex=b_type))*100) pct_cnt
	  from output_tbl   
	)
	, output_final_tbl as (
	select b_type
		 , rank_cnt
		 , pct_cnt
		 , dense_rank() over (partition by b_type order by pct_cnt) as b_type_rank
	  from output_rank_tbl
	)
	select (select max_id from resultado_tbl) id
		 , b_type
		 , rank_cnt
		 , pct_cnt
		 , b_type_rank
	  from output_final_tbl
	 where b_type_rank > 1 
	 order by b_type
		 , pct_cnt desc;

	cursor c_secondary (pv_b_type		varchar2
					  , pn_rank_cnt		number) is
	with resultado_tbl as (
	select max(gambling_id) max_id from olap_sys.sl_gamblings
	)
	, rank_tbl as (
	select h.drawing_id drawing_idx
		 , d.b_type b_typex
		 , d.id idx
		 , d.lt ltx
		 , d.lt_cnt
		 , dense_rank() over (partition by d.drawing_id, d.b_type order by d.lt_cnt, d.id) as rank_cnt
	  from olap_sys.ley_tercio_history_header h
		 , olap_sys.ley_tercio_history_dtl d
	 where h.drawing_id = d.drawing_id
	   and d.drawing_id = (select max_id from resultado_tbl)
	   and d.lt in ('R','G','B')
	)
	, rank_all_tbl as (
	select h.drawing_id drawing_idx
		 , d.b_type b_typex
		 , d.id idx
		 , d.lt ltx
		 , d.lt_cnt
		 , dense_rank() over (partition by d.drawing_id, d.b_type order by d.lt_cnt, d.id) as rank_cnt
	  from olap_sys.ley_tercio_history_header h
		 , olap_sys.ley_tercio_history_dtl d
	 where  h.drawing_id = d.drawing_id
	   and d.drawing_id between (select max_id - 5 from resultado_tbl) and (select max_id - 1 from resultado_tbl) 
	   and d.lt in ('R','G','B')
	) 
	, output_final_tbl as (
	select d.drawing_id
		 , d.b_type
		 , d.lt
		 , d.lt_cnt
		 , nvl((select rank_cnt from rank_tbl where drawing_idx=h.drawing_id and b_typex=d.b_type and idx=d.id and ltx=d.lt),0) rank_cnt
	  from olap_sys.ley_tercio_history_header h
		 , olap_sys.ley_tercio_history_dtl d
	 where h.drawing_id = d.drawing_id
	   and d.drawing_id = (select max_id from resultado_tbl)
	)
	select drawing_id
		 , b_type
		 , lt
	  from output_final_tbl
	 where rank_cnt = pn_rank_cnt
	   and b_type = pv_b_type;
begin
	olap_sys.w_common_pkg.g_index := 1;
	for p in c_primary (pn_history => CN$HISTORY) loop
		--dbms_output.put_line('p: '||p.id||' - '||p.b_type||' - '||p.rank_cnt||' - '||p.b_type_rank||' - '||p.pct_cnt);
		for s in c_secondary (pv_b_type => p.b_type
		                    , pn_rank_cnt => p.rank_cnt) loop
			dbms_output.put_line(s.drawing_id||' - '||s.b_type||' - '||s.lt||' - P'||olap_sys.w_common_pkg.g_index);	
			
			--!actualizacion de la tabla que almacena el conteo de LT por color
			update olap_sys.s_gl_mapas_fre_lt_cnt
			   set predicted_flag_end = 'P'||olap_sys.w_common_pkg.g_index
			 where gl_type = 'LT'
			   and drawing_id_end = to_number(s.drawing_id)
			   and b_type_end = s.b_type
			   and gl_color_end = decode(s.lt,'R',1,'G',2,'B',3);
		end loop;
		if olap_sys.w_common_pkg.g_index < 2 then
			olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
		else
			olap_sys.w_common_pkg.g_index := 1;
		end if;		
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end comparativo_lt_handler;

--!insertar registros en la tabla history_digit_info del sorteo actual
procedure ins_history_digit_info(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_history_digit_info'; 
begin
	insert into olap_sys.history_digit_info (drawing_id
										   , b_type
										   , digit_hdr
										   , history_digit
										   , match_cnt
										   , drawing_cnt
										   , next_drawing_id
										   , id
										   , fr
										   , lt
										   , ca
										   , pxc
										   , preferencia
										   , chng
										   , pxc_pref
										   , jugar_flag)
	with posicion_tbl as (
	select hh.drawing_id, hh.b_type, hh.digit digit_hdr
		 , hd.id, hd.history_digit, hd.match_cnt, hd.drawing_cnt, hd.drawing_list, hd.next_drawing_id, hd.winner_flag
	  from olap_sys.position_digit_history_header hh
		 , olap_sys.position_digit_history_dtl hd
	 where hh.drawing_id = pn_drawing_id 
	   and hh.header_id = hd.header_id
	   and hh.b_type = hd.b_type
	)
	, resultados_tbl as (
	select hd.id, hh.b_type, hh.drawing_id, hh.digit digit_hdr
		 , hd.next_drawing_id, hd.history_digit, hd.winner_flag
	  from olap_sys.position_digit_history_header hh
		 , olap_sys.position_digit_history_dtl hd
	 where hd.winner_flag is not null
	   and hh.header_id = hd.header_id
	   and hh.b_type = hd.b_type
	)
	, cnt_resultados_tbl as (
	select b_type
		 , id
		 , count(1) cnt
	  from resultados_tbl
	 where drawing_id >=  pn_drawing_id -100
	 group by b_type
		 , id 
	)
	, rango_resultados_tbl as (
	select b_type rango_b_type
		 , min(id) min_id
		 , round(avg(id)) avg_id
	  from cnt_resultados_tbl
	 group by b_type
	)
	, output_tbl as (
	select p.drawing_id, p.b_type, p.digit_hdr, p.history_digit, p.match_cnt, p.drawing_cnt, p.next_drawing_id, p.id
		 , nvl(decode(c.color_ubicacion,-1,'#',1,'R',2,'G',3,'B'),'#') fr
		 , nvl(decode(c.color_ley_tercio,-1,'#',1,'R',2,'G',3,'B'),'#') lt
		 , nvl(c.ciclo_aparicion,-1) ca
		 , decode(c.pronos_ciclo,null,0,1) pxc
		 , nvl(c.preferencia_flag,'.') pre
		 , case when c.chng_posicion is null then '.' else 'C' end chng
		 , case when c.pronos_ciclo is null and c.preferencia_flag is null then 0
				when c.pronos_ciclo is null and c.preferencia_flag is not null then 1
				when c.pronos_ciclo is not null and c.preferencia_flag is null then 2
				when c.pronos_ciclo is not null and c.preferencia_flag is not null then 3 end pxc_pref
		 , case when id between (select min_id from rango_resultados_tbl where rango_b_type=p.b_type) and (select avg_id from rango_resultados_tbl where rango_b_type=p.b_type) then 'Y' else 'N' end jugar_flag
	  from posicion_tbl p
		 , olap_sys.s_calculo_stats c
	 where c.drawing_id(+) = p.drawing_id
	   and c.b_type(+) = p.b_type
	   and c.digit(+) = p.history_digit
	)
	select drawing_id, b_type, digit_hdr, history_digit, match_cnt, drawing_cnt, next_drawing_id, id, fr, lt, ca, pxc, pre, chng, pxc_pref, jugar_flag
	  from output_tbl;
	 --where jugar_flag = 'Y';	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_history_digit_info;	

--!actualizar registros en la tabla history_digit_info del sorteo anterior
procedure upd_history_digit_info(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_history_digit_info'; 
	cursor c_resultados (pn_drawing_id		number) is
	select gambling_id, comb1, comb2, comb3, comb4, comb5, comb6
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id = pn_drawing_id;
	
begin
	--!actualizando el campo de numeros repetidos
	--!en base a los resultados del sorteo anterior
	for r in c_resultados (pn_drawing_id	=> pn_drawing_id) loop
		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B1'
		   and history_digit = r.comb1;
		   
		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B2'
		   and history_digit = r.comb2;		   
		   
		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B3'
		   and history_digit = r.comb3;

		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B4'
		   and history_digit = r.comb4;

		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B5'
		   and history_digit = r.comb5;

		update olap_sys.history_digit_info
		   set winner_flag = 'Y'
		     , updated_date = sysdate
		 where drawing_id = pn_drawing_id
           and b_type = 'B6'
		   and history_digit = r.comb6;		   
	end loop;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_history_digit_info;	

--!insertar y actualizar el historico de digitos en base al id del sorteo
procedure history_digit_info_handler(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'history_digit_info_handler'; 
begin

	--!insertar registros en la tabla history_digit_info del sorteo actual
	ins_history_digit_info(pn_drawing_id => pn_drawing_id);
	
	--!actualizar registros en la tabla history_digit_info del sorteo anterior
	upd_history_digit_info(pn_drawing_id => pn_drawing_id -1);
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end history_digit_info_handler;	
							   
--!en base a las predicciones de las terminaciones se actualizan las jugadas en gl_automaticas_detail
procedure upd_terminacion_cnt_handler(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_terminacion_cnt_handler'; 
    ln$match_cnt        number := 0;
	lv$terminaciones    varchar2(1000);
    cursor c_prediccion (pn_drawing_id   number) is
    select distinct '1|'||pred1||','||
          '2|'||pred2||','||
          '3|'||pred3||','||
          '4|'||pred4||','||
          '5|'||pred5||','||
          '6|'||pred6||','||
          '7|'||pred7||','||
          '8|'||pred8||','||
          '9|'||pred9||','||
          '0|'||pred0 prediccion
      from olap_sys.predicciones_all
     where prediccion_sorteo = pn_drawing_id
       and instr(prediccion_tipo,'TERMINACIONES') > 0;
	
	cursor c_prediccion_dtl (pv_prediccion   varchar2) is
    with to_row_tbl as (
    select regexp_substr(pv_prediccion,'[^,]+',1,level) prediccion
      from dual 
      connect by level <= length(pv_prediccion)-length(replace(pv_prediccion,',',''))+1
    )
    select to_number(substr(prediccion,1,1)) prediccion
      from to_row_tbl
     where substr(prediccion,3,1) = '0'; 
    
    cursor c_automaticas (pn_drawing_id   number) is
    select list_id
         , id
         , substr(lpad(comb1,2,'0'),2,1) ia1
         , substr(lpad(comb2,2,'0'),2,1) ia2
         , substr(lpad(comb3,2,'0'),2,1) ia3
         , substr(lpad(comb4,2,'0'),2,1) ia4
         , substr(lpad(comb5,2,'0'),2,1) ia5
         , substr(lpad(comb6,2,'0'),2,1) ia6
         , substr(lpad(comb1,2,'0'),2,1)||','||
           substr(lpad(comb2,2,'0'),2,1)||','||
           substr(lpad(comb3,2,'0'),2,1)||','||
           substr(lpad(comb4,2,'0'),2,1)||','||
           substr(lpad(comb5,2,'0'),2,1)||','||
           substr(lpad(comb6,2,'0'),2,1) automatica
      from olap_sys.gl_automaticas_detail
     where gambling_id = pn_drawing_id; 

    cursor c_to_row (pv_string      varchar2) is
    with distinct_tbl as (
    select regexp_substr(pv_string,'[^,]+',1,level) terminacion
      from dual 
    connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1)
    select distinct terminacion
      from distinct_tbl;    
begin
    --!inicializando la columna
    update olap_sys.gl_automaticas_detail
       set terminacion_cnt = 0;
     
    for p in c_prediccion (pn_drawing_id => pn_drawing_id) loop
        --dbms_output.put_line('prediccion: '||p.prediccion);
        lv$terminaciones := p.prediccion ||',';
    end loop;    
	--dbms_output.put_line('prediccion: '||p.prediccion);
	for pdtl in c_prediccion_dtl (pv_prediccion => lv$terminaciones) loop
		for a in c_automaticas (pn_drawing_id => pn_drawing_id) loop
			--dbms_output.put_line('automaticas: '||a.id||'-'||a.ia1||'-'||a.ia2||'-'||a.ia3||'-'||a.ia4||'-'||a.ia5||'-'||a.ia6);
			ln$match_cnt := 0;
			for t in c_to_row (pv_string => a.automatica) loop 
				--dbms_output.put_line('terminacion: '||t.terminacion);
				if pdtl.prediccion = t.terminacion then
					ln$match_cnt := ln$match_cnt + 1;
				end if;    
			end loop;
			if ln$match_cnt > 0 then
				--dbms_output.put_line('update olap_sys.gl_automaticas_detail '||ln$match_cnt);
				update olap_sys.gl_automaticas_detail
				   set terminacion_cnt = nvl(terminacion_cnt,0) + ln$match_cnt
				 where list_id = a.list_id
				   and id = a.id;
			end if;            
		end loop;
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_terminacion_cnt_handler;	

--!actualizar la tabla gl_position_counts con los datos del sorteo ganador
procedure upd_gl_position_counts(pn_drawing_id		number
							   , pn_comb1			number
							   , pn_comb2			number
							   , pn_comb3			number
							   , pn_comb4			number
							   , pn_comb5			number
							   , pn_comb6			number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_gl_position_counts'; 							   
begin
	--!posicion
	update olap_sys.gl_position_counts
	   set sorteo = pn_drawing_id
     where not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo,0) = pn_drawing_id);
						
	--!posicion1
	update olap_sys.gl_position_counts
	   set r_cnt1 = nvl(r_cnt1,0) + 1
         , sorteo1_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos1,0) = pn_comb1
	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo1_id,0) = pn_drawing_id);

	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb1: '||pn_comb1||' -> '||sql%rowcount||' rows updated')	;
	
	--!posicion2
	update olap_sys.gl_position_counts
	   set r_cnt2 = nvl(r_cnt2,0) + 1
         , sorteo2_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos2,0) = pn_comb2
	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo2_id,0) = pn_drawing_id);

	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb2: '||pn_comb2||' -> '||sql%rowcount||' rows updated')	;
	
	--!posicion3
	update olap_sys.gl_position_counts
	   set r_cnt3 = nvl(r_cnt3,0) + 1
         , sorteo3_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos3,0) = pn_comb3
 	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo3_id,0) = pn_drawing_id);


	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb3: '||pn_comb3||' -> '||sql%rowcount||' rows updated')	;
	
	--!posicion4
	update olap_sys.gl_position_counts
	   set r_cnt4 = nvl(r_cnt4,0) + 1
         , sorteo4_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos4,0) = pn_comb4
	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo4_id,0) = pn_drawing_id);

	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb4: '||pn_comb4||' -> '||sql%rowcount||' rows updated')	;
	
	--!posicion5
	update olap_sys.gl_position_counts
	   set r_cnt5 = nvl(r_cnt5,0) + 1
         , sorteo5_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos5,0) = pn_comb5
	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo5_id,0) = pn_drawing_id);

	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb5: '||pn_comb5||' -> '||sql%rowcount||' rows updated')	;
	
	--!posicion6
	update olap_sys.gl_position_counts
	   set r_cnt6 = nvl(r_cnt6,0) + 1
         , sorteo6_id = pn_drawing_id
		 , updated_date = sysdate
     where nvl(pos6,0) = pn_comb6
	   and not exists (select 1
						 from olap_sys.gl_position_counts
						where nvl(sorteo6_id,0) = pn_drawing_id);
	 
	dbms_output.put_line('sorteo: '||pn_drawing_id||' comb6: '||pn_comb6||' -> '||sql%rowcount||' rows updated')	;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_gl_position_counts;

--!actualizar el calculo de la diferencia entre sorteos en la tabla gl_position_counts con los datos del sorteo ganador
procedure upd_gl_position_counts_dif is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_gl_position_counts_dif'; 
	
	cursor c_dif is
	select seq
		 , case when nvl(sorteo1_id,0) = 0 then 0 else case when sorteo1_id > 0 then sorteo - sorteo1_id else -1 end end dif1
		 , case when nvl(sorteo2_id,0) = 0 then 0 else case when sorteo2_id > 0 then sorteo - sorteo2_id else -1 end end dif2
		 , case when nvl(sorteo3_id,0) = 0 then 0 else case when sorteo3_id > 0 then sorteo - sorteo3_id else -1 end end dif3
		 , case when nvl(sorteo4_id,0) = 0 then 0 else case when sorteo4_id > 0 then sorteo - sorteo4_id else -1 end end dif4
		 , case when nvl(sorteo5_id,0) = 0 then 0 else case when sorteo5_id > 0 then sorteo - sorteo5_id else -1 end end dif5
		 , case when nvl(sorteo6_id,0) = 0 then 0 else case when sorteo6_id > 0 then sorteo - sorteo6_id else -1 end end dif6
	 from olap_sys.gl_position_counts
	 order by seq;	
begin
	for d in c_dif loop						
		dbms_output.put_line('seq: '||d.seq||' dif1: '||d.dif1||' dif2: '||d.dif2||' dif3: '||d.dif3||' dif4: '||d.dif4||' dif5: '||d.dif5||' dif6: '||d.dif6);
		--!posicion1
		update olap_sys.gl_position_counts
		   set dif1 = d.dif1
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos1 is not null
		   and sorteo1_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif1: '||d.dif1||' -> '||sql%rowcount||' rows updated');

		--!posicion2
		update olap_sys.gl_position_counts
		   set dif2 = d.dif2
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos2 is not null
		   and sorteo2_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif2: '||d.dif2||' -> '||sql%rowcount||' rows updated');

		--!posicion3
		update olap_sys.gl_position_counts
		   set dif3 = d.dif3
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos3 is not null
		   and sorteo3_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif3: '||d.dif3||' -> '||sql%rowcount||' rows updated');

		--!posicion4
		update olap_sys.gl_position_counts
		   set dif4 = d.dif4
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos4 is not null
		   and sorteo4_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif4: '||d.dif4||' -> '||sql%rowcount||' rows updated');

		--!posicion5
		update olap_sys.gl_position_counts
		   set dif5 = d.dif5
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos5 is not null
		   and sorteo5_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif5: '||d.dif5||' -> '||sql%rowcount||' rows updated');

		--!posicion6
		update olap_sys.gl_position_counts
		   set dif6 = d.dif6
			 , updated_date = sysdate
		 where seq = d.seq
		   and pos6 is not null
		   and sorteo6_id is not null;

		dbms_output.put_line('seq: '||d.seq||' dif6: '||d.dif6||' -> '||sql%rowcount||' rows updated');

	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_gl_position_counts_dif;

--!actualizar flags en base al promedio del valor de las columnas dif
procedure upd_gl_position_counts_flags (pf_percentile_ini   float
								      , pf_percentile_end   float) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_gl_position_counts_flags'; 
	
	cursor c_pos1 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt1),1) avg_j_cnt1
		 , round(avg(r_cnt1),1) avg_r_cnt1
         , percentile_cont(pf_percentil_ini) within group (order by dif1) perc_ini_dif1
         , percentile_cont(pf_percentil_end) within group (order by dif1) perc_end_dif1 
	  from olap_sys.gl_position_counts
	 where nvl(pos1,0) > 0;  
	 
	cursor c_pos2 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt2),1) avg_j_cnt2
		 , round(avg(r_cnt2),1) avg_r_cnt2
         , percentile_cont(pf_percentil_ini) within group (order by dif2) perc_ini_dif2
         , percentile_cont(pf_percentil_end) within group (order by dif2) perc_end_dif2 
	  from olap_sys.gl_position_counts	
	 where nvl(pos2,0) > 0;
	 
	cursor c_pos3 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt3),1) avg_j_cnt3
		 , round(avg(r_cnt3),1) avg_r_cnt3
         , percentile_cont(pf_percentil_ini) within group (order by dif3) perc_ini_dif3
         , percentile_cont(pf_percentil_end) within group (order by dif3) perc_end_dif3 
	  from olap_sys.gl_position_counts	
	 where nvl(pos3,0) > 0;
	 
	cursor c_pos4 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt4),1) avg_j_cnt4
		 , round(avg(r_cnt4),1) avg_r_cnt4
         , percentile_cont(pf_percentil_ini) within group (order by dif4) perc_ini_dif4
         , percentile_cont(pf_percentil_end) within group (order by dif4) perc_end_dif4 
	  from olap_sys.gl_position_counts
	 where nvl(pos4,0) > 0;
	 
	cursor c_pos5 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt5),1) avg_j_cnt5
		 , round(avg(r_cnt5),1) avg_r_cnt5
         , percentile_cont(pf_percentil_ini) within group (order by dif5) perc_ini_dif5
         , percentile_cont(pf_percentil_end) within group (order by dif5) perc_end_dif5 
	  from olap_sys.gl_position_counts
	 where nvl(pos5,0) > 0;

	cursor c_pos6 (pf_percentil_ini float
                 , pf_percentil_end float) is
	select round(avg(j_cnt6),1) avg_j_cnt6
		 , round(avg(r_cnt6),1) avg_r_cnt6   
         , percentile_cont(pf_percentil_ini) within group (order by dif6) perc_ini_dif6
         , percentile_cont(pf_percentil_end) within group (order by dif6) perc_end_dif6 
	  from olap_sys.gl_position_counts
	 where nvl(pos6,0) > 0;
	 
begin
	--!inicializando banderas
	update olap_sys.gl_position_counts
	   set j_cnt1_flag = 0
	     , j_cnt2_flag = 0
		 , j_cnt3_flag = 0
		 , j_cnt4_flag = 0
		 , j_cnt5_flag = 0
		 , j_cnt6_flag = 0
		 , r_cnt1_flag = 0
		 , r_cnt2_flag = 0
		 , r_cnt3_flag = 0
		 , r_cnt4_flag = 0
		 , r_cnt5_flag = 0
		 , r_cnt6_flag = 0
		 , dif1_flag = 0
		 , dif2_flag = 0
		 , dif3_flag = 0
		 , dif4_flag = 0
		 , dif5_flag = 0
		 , dif6_flag = 0;		 
		 
	for a in c_pos1 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop						
		--!posicion1
		update olap_sys.gl_position_counts
		   set j_cnt1_flag = 1
			 , updated_date = sysdate
		 where j_cnt1 > a.avg_j_cnt1;

		dbms_output.put_line('> j_cnt1: '||a.avg_j_cnt1||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt1_flag = 1
			 , updated_date = sysdate
		 where r_cnt1 > a.avg_r_cnt1;

		dbms_output.put_line('> r_cnt1: '||a.avg_r_cnt1||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif1_flag = 1
			 , updated_date = sysdate
		 where dif1 between a.perc_ini_dif1 and a.perc_end_dif1;

		dbms_output.put_line('between dif1: '||a.perc_ini_dif1||' and '||a.perc_end_dif1||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;

	for a in c_pos2 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop		
		--!posicion2
		update olap_sys.gl_position_counts
		   set j_cnt2_flag = 1
			 , updated_date = sysdate
		 where j_cnt2 > a.avg_j_cnt2;

		dbms_output.put_line('> j_cnt2: '||a.avg_j_cnt2||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt2_flag = 1
			 , updated_date = sysdate
		 where r_cnt2 > a.avg_r_cnt2;

		dbms_output.put_line('> r_cnt2: '||a.avg_r_cnt2||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif2_flag = 1
			 , updated_date = sysdate
		 where dif2 between a.perc_ini_dif2 and a.perc_end_dif2;

		dbms_output.put_line('between dif2: '||a.perc_ini_dif2||' and '||a.perc_end_dif2||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;

	for a in c_pos3 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop	
		--!posicion3
		update olap_sys.gl_position_counts
		   set j_cnt3_flag = 1
			 , updated_date = sysdate
		 where j_cnt3 > a.avg_j_cnt3;

		dbms_output.put_line('> j_cnt3: '||a.avg_j_cnt3||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt3_flag = 1
			 , updated_date = sysdate
		 where r_cnt3 > a.avg_r_cnt3;

		dbms_output.put_line('> r_cnt3: '||a.avg_r_cnt3||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif3_flag = 1
			 , updated_date = sysdate
		 where dif3 between a.perc_ini_dif3 and a.perc_end_dif3;

		dbms_output.put_line('between dif3: '||a.perc_ini_dif3||' and '||a.perc_end_dif3||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;

	for a in c_pos4 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop	
		--!posicion4
		update olap_sys.gl_position_counts
		   set j_cnt4_flag = 1
			 , updated_date = sysdate
		 where j_cnt4 > a.avg_j_cnt4;

		dbms_output.put_line('> j_cnt4: '||a.avg_j_cnt4||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt4_flag = 1
			 , updated_date = sysdate
		 where r_cnt4 > a.avg_r_cnt4;

		dbms_output.put_line('> r_cnt4: '||a.avg_r_cnt4||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif4_flag = 1
			 , updated_date = sysdate
		 where dif4 between a.perc_ini_dif4 and a.perc_end_dif4;

		dbms_output.put_line('between dif4: '||a.perc_ini_dif4||' and '||a.perc_end_dif4||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;

	for a in c_pos5 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop	
		--!posicion5
		update olap_sys.gl_position_counts
		   set j_cnt5_flag = 1
			 , updated_date = sysdate
		 where j_cnt5 > a.avg_j_cnt5;

		dbms_output.put_line('> j_cnt5: '||a.avg_j_cnt5||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt5_flag = 1
			 , updated_date = sysdate
		 where r_cnt5 > a.avg_r_cnt5;

		dbms_output.put_line('> r_cnt5: '||a.avg_r_cnt5||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif5_flag = 1
			 , updated_date = sysdate
		 where dif5 between a.perc_ini_dif5 and a.perc_end_dif5;

		dbms_output.put_line('between dif5: '||a.perc_ini_dif5||' and '||a.perc_end_dif5||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;

	for a in c_pos6 (pf_percentil_ini => pf_percentile_ini
	               , pf_percentil_end => pf_percentile_end) loop	
		--!posicion6
		update olap_sys.gl_position_counts
		   set j_cnt6_flag = 1
			 , updated_date = sysdate
		 where j_cnt6 > a.avg_j_cnt6;

		dbms_output.put_line('> j_cnt6: '||a.avg_j_cnt6||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set r_cnt6_flag = 1
			 , updated_date = sysdate
		 where r_cnt6 > a.avg_r_cnt6;

		dbms_output.put_line('> r_cnt6: '||a.avg_r_cnt6||' flag = 1  -> '||sql%rowcount||' rows updated');

		update olap_sys.gl_position_counts
		   set dif6_flag = 1
			 , updated_date = sysdate
		 where dif6 between a.perc_ini_dif6 and a.perc_end_dif6;

		dbms_output.put_line('between dif: '||a.perc_ini_dif6||' and '||a.perc_end_dif6||' flag = 1  -> '||sql%rowcount||' rows updated');
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_gl_position_counts_flags;


--!actualiza los campos con las probabilidades calculadas
procedure upd_gl_position_counts_prob(pn_b_type			number
									, pn_digito			number
									, pf_general_lambda	float
									, pf_general_prob	float
									, pv_lambda_type	varchar2 default 'G') is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'calcula_prob_lambda_general'; 							   
begin
	--!lambda general
	if pv_lambda_type = 'G' then
		olap_sys.w_common_pkg.g_dml_stmt:= 'UPDATE OLAP_SYS.GL_POSITION_COUNTS';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' SET BAYES'||pn_b_type||'_GENERAL_LAMBDA = :PN_LAMBDA';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' , BAYES'||pn_b_type||'_GENERAL_PROB = :PN_PROB';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' WHERE POS'||pn_b_type||' = :PN_DIGITO';	
	else
	--!lambda por b_type
		olap_sys.w_common_pkg.g_dml_stmt:= 'UPDATE OLAP_SYS.GL_POSITION_COUNTS';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' SET BAYES'||pn_b_type||'_B_TYPE_LAMBDA = :PN_LAMBDA';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' , BAYES'||pn_b_type||'_B_TYPE_PROB = :PN_PROB';
		olap_sys.w_common_pkg.g_dml_stmt:= olap_sys.w_common_pkg.g_dml_stmt||' WHERE POS'||pn_b_type||' = :PN_DIGITO';	
	end if;
	--dbms_output.put_line(olap_sys.w_common_pkg.g_dml_stmt);
	execute immediate olap_sys.w_common_pkg.g_dml_stmt using pf_general_lambda, pf_general_prob, pn_digito;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end upd_gl_position_counts_prob;


--calcula las probabilidades de la vista en base a un lambda general
procedure calcula_prob_lambda_general is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'calcula_prob_lambda_general'; 							   
	CF$LAMBDA			 CONSTANT FLOAT := 0.15;

	cursor c_calcula_prob (pf_lambda	float) is
	with base as (
	  select b_type, pos, dif
	  from   olap_sys.vw_position_counts_vert
	),
	lik as (
	  select
		b_type,
		pos,
		dif,
		exp(-pf_lambda * dif) as likelihood
	  from base
	),
	norm as (
	  select
		b_type,
		pos,
		dif,
		likelihood,
		likelihood / sum(likelihood) over (partition by b_type) as prob
	  from lik
	)
	select b_type, pos, dif, round(prob,3) prob
	from norm
	order by b_type, prob desc, dif asc;
begin
	--limpiando los campos
	update olap_sys.gl_position_counts
	   set bayes1_general_lambda = 0.0
		 , bayes1_general_prob = 0.0
		 , bayes1_general_flag = 'N'
		 , bayes2_general_lambda = 0.0
		 , bayes2_general_prob = 0.0
		 , bayes2_general_flag = 'N'
		 , bayes3_general_lambda = 0.0
		 , bayes3_general_prob = 0.0
		 , bayes3_general_flag = 'N'
		 , bayes4_general_lambda = 0.0
		 , bayes4_general_prob = 0.0
		 , bayes4_general_flag = 'N'
		 , bayes5_general_lambda = 0.0
		 , bayes5_general_prob = 0.0
		 , bayes5_general_flag = 'N'
		 , bayes6_general_lambda = 0.0
		 , bayes6_general_prob = 0.0
		 , bayes6_general_flag = 'N';
		 
	for l in c_calcula_prob (pf_lambda => CF$LAMBDA) loop
		dbms_output.put_line('b_type: '||l.b_type||' pos: '||l.pos||' prob: '||l.prob);
		--!actualiza los campos con las probabilidades calculadas
		upd_gl_position_counts_prob(pn_b_type => l.b_type
							 	  , pn_digito	=> l.pos
								  , pf_general_lambda	=> CF$LAMBDA
								  , pf_general_prob => l.prob);
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end calcula_prob_lambda_general;


--!calcula las probabilidades de la vista en base a un lambda por b_type
procedure calcula_prob_lambda_b_type is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'calcula_prob_lambda_b_type'; 							   
	CF$LAMBDA			 CONSTANT FLOAT := 0.15;

	cursor c_calcula_prob (pf_lambda	float) is
	with med as (
	  select
		b_type,
		percentile_cont(0.5) within group (order by dif) as median_dif
	  from   olap_sys.vw_position_counts_vert
	  group  by b_type
	), lambdas as (
	  select
		b_type,
		round(case when median_dif > 0 then ln(2)/median_dif else pf_lambda end, 3) as lambda_sugerida
	  from med
	) 
	, base as (
	  select b_type, pos, dif
	  from   olap_sys.vw_position_counts_vert
	) 
	, lik as (
	  select
		b.b_type,
		b.pos,
		b.dif,
		exp( - l.lambda_sugerida * b.dif ) as likelihood
	  from base b
	  join lambdas l
		on l.b_type = b.b_type
	) 
	, norm as (
	  select
		b_type,
		pos,
		dif,
		likelihood,
		likelihood / sum(likelihood) over (partition by b_type) as prob
	  from lik
	)
	select n.b_type, n.pos, n.dif, round(n.prob, 3) as prob, l.lambda_sugerida
	  from norm n
		 , lambdas l
	 where n.b_type = l.b_type   
	 order by n.b_type, n.prob desc, n.dif asc;
begin
	--limpiando los campos
	update olap_sys.gl_position_counts
	   set bayes1_b_type_lambda = 0.0
		 , bayes1_b_type_prob = 0.0
		 , bayes1_b_type_flag = 'N'
		 , bayes2_b_type_lambda = 0.0
		 , bayes2_b_type_prob = 0.0
		 , bayes2_b_type_flag = 'N'
		 , bayes3_b_type_lambda = 0.0
		 , bayes3_b_type_prob = 0.0
		 , bayes3_b_type_flag = 'N'
		 , bayes4_b_type_lambda = 0.0
		 , bayes4_b_type_prob = 0.0
		 , bayes4_b_type_flag = 'N'
		 , bayes5_b_type_lambda = 0.0
		 , bayes5_b_type_prob = 0.0
		 , bayes5_b_type_flag = 'N'
		 , bayes6_b_type_lambda = 0.0
		 , bayes6_b_type_prob = 0.0
		 , bayes6_b_type_flag = 'N';
		 
	for l in c_calcula_prob (pf_lambda => CF$LAMBDA) loop
		dbms_output.put_line('b_type: '||l.b_type||' pos: '||l.pos||' prob: '||l.prob);
		--!actualiza los campos con las probabilidades calculadas
		upd_gl_position_counts_prob(pn_b_type => l.b_type
								  , pn_digito	=> l.pos
								  , pf_general_lambda	=> CF$LAMBDA
								  , pf_general_prob => l.prob
								  , pv_lambda_type => 'B');
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end calcula_prob_lambda_b_type;


--!actualizacion de las banderas de las probabilidades
procedure upd_bayes_probabilidades is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_bayes_probabilidades'; 	
	lv$general_dml_stmt			  VARCHAR2(500);
	lv$b_type_dml_stmt			  VARCHAR2(500);
	
	cursor c_probabilidades is
	select b_type
		 , round(AVG(general_prob),3) general_prob
		 , round(AVG(b_type_prob),3) b_type_prob
	  from olap_sys.vw_position_counts_vert
	 group by b_type
	 order by b_type;	
begin
	--!inicializando banderas
	update OLAP_SYS.GL_POSITION_COUNTS
		   set BAYES1_GENERAL_FLAG = 'N'
		     , BAYES1_B_TYPE_FLAG  = 'N'
			 , BAYES2_GENERAL_FLAG = 'N'
		     , BAYES2_B_TYPE_FLAG  = 'N'
			 , BAYES3_GENERAL_FLAG = 'N'
		     , BAYES3_B_TYPE_FLAG  = 'N'
			 , BAYES4_GENERAL_FLAG = 'N'
		     , BAYES4_B_TYPE_FLAG  = 'N'
			 , BAYES5_GENERAL_FLAG = 'N'
		     , BAYES5_B_TYPE_FLAG  = 'N'
			 , BAYES6_GENERAL_FLAG = 'N'
		     , BAYES6_B_TYPE_FLAG  = 'N';
	for p in c_probabilidades loop
		--!banderas generales
		lv$general_dml_stmt := 'UPDATE OLAP_SYS.GL_POSITION_COUNTS';
		lv$general_dml_stmt := lv$general_dml_stmt||' SET BAYES'||p.b_type||'_GENERAL_FLAG = '||CHR(39)||'Y'||CHR(39);
		lv$general_dml_stmt := lv$general_dml_stmt||' WHERE BAYES'||p.b_type||'_GENERAL_PROB > :pn_general_prob';	
		dbms_output.put_line(lv$general_dml_stmt);
		execute immediate lv$general_dml_stmt using p.general_prob;

		--!banderas b_type
		lv$b_type_dml_stmt := 'UPDATE OLAP_SYS.GL_POSITION_COUNTS';
		lv$b_type_dml_stmt := lv$b_type_dml_stmt||' SET BAYES'||p.b_type||'_B_TYPE_FLAG = '||CHR(39)||'Y'||CHR(39);
		lv$b_type_dml_stmt := lv$b_type_dml_stmt||' WHERE BAYES'||p.b_type||'_B_TYPE_PROB > :pn_b_type_prob';	
		dbms_output.put_line(lv$b_type_dml_stmt);
		execute immediate lv$b_type_dml_stmt using p.b_type_prob;

	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_bayes_probabilidades;


--!actualizar la tabla gl_position_counts con los datos del sorteo ganador
procedure upd_gl_pos_counts_handler(pn_drawing_id	number
							      , pn_comb1			number
							      , pn_comb2			number
							      , pn_comb3			number
							      , pn_comb4			number
							      , pn_comb5			number
							      , pn_comb6			number
								  , pf_percentile_ini   float default 0.15
								  , pf_percentile_end   float default 0.9) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_gl_pos_counts_handler'; 							   
begin
	dbms_output.enable(null);
	--!actualizar la tabla gl_position_counts con los datos del sorteo ganador
	upd_gl_position_counts(pn_drawing_id => pn_drawing_id
					     , pn_comb1 => pn_comb1
					     , pn_comb2 => pn_comb2
					     , pn_comb3 => pn_comb3
					     , pn_comb4 => pn_comb4
					     , pn_comb5 => pn_comb5
					     , pn_comb6 => pn_comb6);

	--!actualizar el calculo de la diferencia entre sorteos en la tabla gl_position_counts con los datos del sorteo ganador
	upd_gl_position_counts_dif;

	--!actualizar flags en base al promedio del valor de las columnas dif
	upd_gl_position_counts_flags (pf_percentile_ini => pf_percentile_ini
								, pf_percentile_end => pf_percentile_end); 
	
	--!calcula las probabilidades de la vista en base a un lambda general
	calcula_prob_lambda_general;
	
	--!calcula las probabilidades de la vista en base a un lambda por b_type
	calcula_prob_lambda_b_type;
		
	--!actualizacion de las banderas de las probabilidades
	upd_bayes_probabilidades;
	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_gl_pos_counts_handler;

--!insertar las probabilidades en la nueva tabla gl_automaticas_prob
procedure ins_gl_automaticas_prob(pv_nombre_modelo	varchar2
								, pn_list_id		number
								, pn_id				number
								, pn_sorteo_actual	number
								, pn_ia1			number
								, pn_ia2			number
								, pn_ia3			number
								, pn_ia4			number
								, pn_ia5			number
								, pn_ia6			number
								, pf_digito_prob1	float
								, pf_digito_prob2	float
								, pf_digito_prob3	float
								, pf_digito_prob4	float
								, pf_digito_prob5	float
								, pf_digito_prob6	float) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_gl_automaticas_prob';
begin
	--!insertando el nuevo registro de la prediccion
	insert into olap_sys.gl_automaticas_prob (gambling_type
											, nombre_modelo
											, list_id
											, id
											, seq_id
											, sorteo_actual
											, ia1
											, ia2
											, ia3
											, ia4
											, ia5
											, ia6
											, digito_prob1
											, digito_prob2
											, digito_prob3
											, digito_prob4
											, digito_prob5
											, digito_prob6
											, digito_sum_prob)
	values ('mrtr'
	      , pv_nombre_modelo
	      , pn_list_id
	      , pn_id
		  , (select nvl(max(seq_id),0)+1 from olap_sys.gl_automaticas_prob)
	      , pn_sorteo_actual
	      , pn_ia1
		  , pn_ia2
	      , pn_ia3
	      , pn_ia4
		  , pn_ia5
	      , pn_ia6
	      , pf_digito_prob1
		  , pf_digito_prob2
	      , pf_digito_prob3
	      , pf_digito_prob4
		  , pf_digito_prob5
	      , pf_digito_prob6
	      , pf_digito_prob1 + pf_digito_prob2 + pf_digito_prob3 + pf_digito_prob4 + pf_digito_prob5 + pf_digito_prob6);
	commit;		  
exception
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 	
end ins_gl_automaticas_prob;

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
								, pf_digito_prob6	float default 0.0) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'probabilidades_all_handler'; 
begin		
	--!insertar las probabilidades en la nueva tabla gl_automaticas_prob
	ins_gl_automaticas_prob(pv_nombre_modelo => pv_nombre_modelo
						  , pn_list_id		 => pn_list_id
						  , pn_id			 => pn_id
						  , pn_sorteo_actual => pn_sorteo_actual
						  , pn_ia1			 => pn_ia1
						  , pn_ia2			 => pn_ia2
						  , pn_ia3			 => pn_ia3
						  , pn_ia4			 => pn_ia4
						  , pn_ia5			 => pn_ia5
						  , pn_ia6			 => pn_ia6
						  , pf_digito_prob1	 => pf_digito_prob1
						  , pf_digito_prob2	 => pf_digito_prob2
						  , pf_digito_prob3	 => pf_digito_prob3
						  , pf_digito_prob4	 => pf_digito_prob4
						  , pf_digito_prob5	 => pf_digito_prob5
						  , pf_digito_prob6	 => pf_digito_prob6);		   
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end probabilidades_all_handler;


--!actualizar la bandera para el LT ganador en la tabla s_gl_mapas_fre_lt_cnt
procedure upd_lt_handler (pn_drawing_id number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_lt_handler'; 
begin
	olap_sys.w_common_pkg.g_index := 1;
	for p in c_main (pn_drawing_id =>  pn_drawing_id - 1) loop
		if p.winner = 'Y' then
			update olap_sys.s_gl_mapas_fre_lt_cnt
			   set winner_flag_end = 'Y'
			 where gl_type = 'LT'
			   and drawing_id_end = p.id
			   and b_type_end = p.b_type
			   and gl_color_end = p.lt;
		end if;	   
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_lt_handler;


--!registra los match por posicion para un patron especifico
procedure upd_conteo_patrones(pv_patron_type		varchar2) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_conteo_patrones'; 
	ln$default_no_match	 CONSTANT NUMBER := 100;
	
	cursor c_patrones (pv_patron_type		varchar2) is
	select sorteo_id
		 , b_type
		 , j_digit_type
		 , j_digit_count_str
		 , j_preferencia_flag
		 , r_digit_type
		 , r_preferencia_flag
	  from olap_sys.conteo_patrones
	 where patron_type = pv_patron_type 
	 order by b_type, sorteo_id; 	
begin
	for m in c_patrones (pv_patron_type => pv_patron_type) loop
		dbms_output.put_line(m.sorteo_id
		 ||' - '||m.b_type
		 ||' - '||m.j_digit_type
		 ||' - '||m.j_digit_count_str
		 ||' - '||m.j_preferencia_flag
		 ||' - '||m.r_digit_type
		 ||' - '||m.r_preferencia_flag);
		if m.b_type = 'B1' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match1 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match1 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match1 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;

		if m.b_type = 'B2' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match2 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match2 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match2 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;

		if m.b_type = 'B3' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match3 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match3 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match3 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;

		if m.b_type = 'B4' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match4 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match4 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match4 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;

		if m.b_type = 'B5' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match5 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match5 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match5 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;

		if m.b_type = 'B6' then
			if m.r_preferencia_flag in ('N','Y') then
				if instr(m.j_digit_type, m.r_digit_type) > 0 then
					update olap_sys.conteo_patrones
					   set match6 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match6 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			else
				update olap_sys.conteo_patrones
				   set no_match6 = ln$default_no_match	
				 where patron_type = pv_patron_type
				   and sorteo_id = m.sorteo_id
				   and b_type = m.b_type;			
			end if;
		end if;
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_conteo_patrones;


--!insertar registro en la tabla conteo_patrones
procedure ins_conteo_patrones(pv_patron_type		varchar2
							, pn_sorteo_id			number
							, pv_b_type				varchar2
							, pv_j_digit_type		varchar2
							, pv_j_digit_count_str	varchar2
							, pv_j_preferencia_flag	varchar2
							, pv_r_digit_type		varchar2
							, pv_r_preferencia_flag	varchar2
							, pn_r_drawing_id       number) is

	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_conteo_patrones'; 
	ln$ultimo_sorteo			  NUMBER := 0;
begin
	insert into olap_sys.conteo_patrones(patron_type
									   , sorteo_id
									   , b_type
									   , j_digit_type
									   , j_digit_count_str
									   , j_preferencia_flag
									   , r_digit_type
									   , r_preferencia_flag
									   , atributo_num2)
	values(pv_patron_type
		 , pn_sorteo_id
		 , pv_b_type
		 , pv_j_digit_type
		 , pv_j_digit_count_str
		 , pv_j_preferencia_flag
		 , pv_r_digit_type
		 , pv_r_preferencia_flag
		 , pn_r_drawing_id);									
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_conteo_patrones;


--!calcular el conteo de matches para patron digit_type
procedure patron_digit_type(pn_ultimo_sorteo	number
                          , pn_rango_historico	number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'patron_digit_type';
	CV$PATRON_NOMBRE	 CONSTANT VARCHAR2(30) := 'DIGIT_TYPE';
	ln$rango_historico			  NUMBER := 0;
	
	cursor c_conteo_patrones (pn_ultimo_sorteo		number
                            , pn_rango_historico	number) is
	with nivel_n_tbl as (
	select drawing_id
		 , b_type
		 , olap_sys.w_common_pkg.get_digit_type(digit) digit_type 
		 , decode(preferencia_flag,'X','Y','N') preferencia_flag
		 , count(1) cnt
	  from olap_sys.s_calculo_stats
	 where preferencia_flag is not null
	   and drawing_id = pn_ultimo_sorteo - pn_rango_historico
	 group by drawing_id
		 , b_type
		 , olap_sys.w_common_pkg.get_digit_type(digit)
		 , preferencia_flag		 
	) 
    , resultado_tbl as (
    select drawing_id + 1 r_drawing_id
         , b_type r_b_type
         , olap_sys.w_common_pkg.get_digit_type(digit) r_digit_type 
         , decode(preferencia_flag,'X','Y','N') r_preferencia_flag
      from olap_sys.s_calculo_stats
     where winner_flag is not null
       and drawing_id = pn_ultimo_sorteo - pn_rango_historico  
    )
	  select
		  drawing_id
		, b_type
		, listagg(digit_type, ',') within group (order by digit_type) as digit_type_str
		, listagg(cnt, ',') within group (order by digit_type) as count_str
		, preferencia_flag
        , nvl((select r_digit_type from resultado_tbl where r_drawing_id-1=drawing_id and r_b_type=b_type),0) r_digit_type
        , nvl((select r_preferencia_flag from resultado_tbl where r_drawing_id-1=drawing_id and r_b_type=b_type),'Z') r_preferencia_flag
		, nvl((select r_drawing_id from resultado_tbl where r_drawing_id-1=drawing_id and r_b_type=b_type),0) r_drawing_id
	from
		nivel_n_tbl
	group by drawing_id
	    , b_type
		, preferencia_flag;
begin
	olap_sys.w_common_pkg.g_rowcnt := 0;
	for h in reverse 1..pn_rango_historico loop
		dbms_output.put_line('sorteo_id: '||h);
		for p in c_conteo_patrones (pn_ultimo_sorteo => pn_ultimo_sorteo
                                  , pn_rango_historico => h) loop
			dbms_output.put_line(p.drawing_id||' - '||p.b_type||' - '||p.digit_type_str||' - '||p.count_str||' - '||p.preferencia_flag||' - '||p.r_drawing_id||' - '||p.r_digit_type||' - '||p.r_preferencia_flag);
			--!insertar registro en la tabla conteo_patrones
			ins_conteo_patrones(pv_patron_type => CV$PATRON_NOMBRE
							  , pn_sorteo_id   => p.drawing_id
							  , pv_b_type	   => p.b_type
							  , pv_j_digit_type  => p.digit_type_str
							  , pv_j_digit_count_str => p.count_str
							  , pv_j_preferencia_flag => p.preferencia_flag
							  , pv_r_digit_type => p.r_digit_type
							  , pv_r_preferencia_flag => p.r_preferencia_flag
							  , pn_r_drawing_id => p.r_drawing_id
							  ); 
		end loop;
		olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;	
	end loop;
	
	--!registra los match por posicion para un patron especifico
	upd_conteo_patrones(pv_patron_type => CV$PATRON_NOMBRE);
	
	dbms_output.put_line(olap_sys.w_common_pkg.g_rowcnt||' jugadas procesadas.');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end patron_digit_type;


--!registra los match por posicion para un patron especifico
procedure upd_conteo_patrones_ca(pv_patron_type		varchar2) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_conteo_patrones_ca'; 
	ln$default_no_match	 CONSTANT NUMBER := 100;
	
	cursor c_patrones (pv_patron_type		varchar2) is
	select sorteo_id
		 , b_type
		 , atributo_float1 avg_ca
		 , atributo_num1 r_ca
		 , atributo_num2 r_drawing_id
	  from olap_sys.conteo_patrones
	 where patron_type = pv_patron_type 
	 order by b_type, sorteo_id; 	
begin
	dbms_output.put_line('--------------------------------');
	for m in c_patrones (pv_patron_type => pv_patron_type) loop
		dbms_output.put_line(m.sorteo_id
		 ||' - '||m.b_type
		 ||' - '||m.avg_ca
		 ||' - '||m.r_drawing_id
		 ||' - '||m.r_ca);
		if m.b_type = 'B1' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match1 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match1 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

		if m.b_type = 'B2' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match2 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match2 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

		if m.b_type = 'B3' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match3 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match3 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

		if m.b_type = 'B4' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match4 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match4 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

		if m.b_type = 'B5' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match5 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match5 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

		if m.b_type = 'B6' then
			--!indica que el digito fue acertado
			if m.r_ca > 0 then
				if m.r_ca < m.avg_ca then
					update olap_sys.conteo_patrones
					   set match6 = 1	
					 where patron_type = pv_patron_type 
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				else
					update olap_sys.conteo_patrones
					   set no_match6 = 1	
					 where patron_type = pv_patron_type
					   and sorteo_id = m.sorteo_id
					   and b_type = m.b_type;
				end if;
			end if;
		end if;

	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_conteo_patrones_ca;


--!insertar registro en la tabla conteo_patrones
procedure ins_conteo_patrones_ca(pv_patron_type		varchar2
							   , pn_sorteo_id		number
							   , pv_b_type			varchar2
							   , pf_avg_ca			float
							   , pn_r_drawing_id    number
							   , pn_r_ca	        number) is

	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_conteo_patrones_ca'; 
	ln$ultimo_sorteo			  NUMBER := 0;
begin
	insert into olap_sys.conteo_patrones(patron_type
									   , sorteo_id
									   , b_type
									   , atributo_float1
									   , atributo_num1
									   , atributo_num2)
	values(pv_patron_type
		 , pn_sorteo_id
		 , pv_b_type
		 , pf_avg_ca
		 , pn_r_ca
		 , pn_r_drawing_id);									
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end ins_conteo_patrones_ca;


--!calcular el conteo de matches para patron ciclo de aparicion (CA)
procedure patron_ca(pn_ultimo_sorteo	number
                  , pn_rango_historico	number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'patron_ca';
	CV$PATRON_NOMBRE	 CONSTANT VARCHAR2(30) := 'CA';
	ln$rango_historico			  NUMBER := 0;
	
	cursor c_conteo_patrones (pn_ultimo_sorteo		number
                            , pn_rango_historico	number) is
	with nivel_n_tbl as (
	select drawing_id
		 , b_type
		 , round(avg(ciclo_aparicion),1) avg_ca
	  from olap_sys.s_calculo_stats
	 where drawing_id = pn_ultimo_sorteo - pn_rango_historico 
	 group by drawing_id
		 , b_type
	) 
    , resultado_tbl as (
    select drawing_id + 1 r_drawing_id
         , b_type r_b_type
         , ciclo_aparicion ca
      from olap_sys.s_calculo_stats
     where winner_flag is not null
       and drawing_id = pn_ultimo_sorteo - pn_rango_historico  
    )
	  select
		  drawing_id
		, b_type
		, avg_ca
        , nvl((select r_drawing_id from resultado_tbl where r_drawing_id-1=drawing_id and r_b_type=b_type),0) r_drawing_id
        , nvl((select ca from resultado_tbl where r_drawing_id-1=drawing_id and r_b_type=b_type),0) r_ca
	from
		nivel_n_tbl;
begin
	olap_sys.w_common_pkg.g_rowcnt := 0;
	for h in reverse 1..pn_rango_historico loop
		dbms_output.put_line('sorteo_id: '||h);
		for p in c_conteo_patrones (pn_ultimo_sorteo => pn_ultimo_sorteo
                                  , pn_rango_historico => h) loop
			dbms_output.put_line(p.drawing_id||' - '||p.b_type||' - '||p.avg_ca||' - '||p.r_drawing_id||' - '||p.r_ca);
			--!insertar registro en la tabla conteo_patrones
			ins_conteo_patrones_ca(pv_patron_type => CV$PATRON_NOMBRE
							     , pn_sorteo_id   => p.drawing_id
							     , pv_b_type	  => p.b_type
							     , pf_avg_ca	  => p.avg_ca
								 , pn_r_drawing_id => p.r_drawing_id
							     , pn_r_ca	      => p.r_ca); 
		end loop;	
		olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
	end loop;
	
	--!registra los match por posicion para un patron especifico
	upd_conteo_patrones_ca(pv_patron_type => CV$PATRON_NOMBRE);
	
	dbms_output.put_line(olap_sys.w_common_pkg.g_rowcnt||' jugadas procesadas.');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end patron_ca;


--!calcular el conteo de matches para un patron definido
procedure conteo_patrones_handler(pv_patron_type		varchar2 default 'DIGIT_TYPE'
								, pn_rango_historico	number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'conteo_patrones_handler'; 
	ln$ultimo_sorteo			  NUMBER := 0;
begin
	--!recuperando el ultimo ID del sorteo solo como referencia
	select max(drawing_id)
      into ln$ultimo_sorteo
      from olap_sys.s_calculo_stats
     where winner_flag is not null;
	
	--!limpiar la tabla
	delete olap_sys.conteo_patrones;
	
	--!calcular el conteo de matches para patron digit_type
	if upper(trim(pv_patron_type)) = 'DIGIT_TYPE' then
		patron_digit_type(pn_ultimo_sorteo => ln$ultimo_sorteo
						, pn_rango_historico => pn_rango_historico);
	end if;

	--!calcular el conteo de matches para patron ciclo de aparicion (CA)
	if upper(trim(pv_patron_type)) = 'CA' then
		patron_ca(pn_ultimo_sorteo => ln$ultimo_sorteo
				, pn_rango_historico => pn_rango_historico);
	end if;	
	
	commit;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end conteo_patrones_handler;


--!calcular y actualizar el promedio de ciclo de aparicion en base a b_type
--!para que se pueda utilizar como metrica para la seleccion de jjugadas
procedure upd_avg_gl_ca(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_avg_gl_ca';

	cursor c_sorteo_actual (pn_drawing_id		number) is
	select drawing_id
	     , b_type
		 , round(avg(ciclo_aparicion),2) avg_ca
	  from olap_sys.s_calculo_stats
	 where drawing_id = pn_drawing_id
	 group by drawing_id
	     , b_type; 	
begin
	for k in c_sorteo_actual (pn_drawing_id => pn_drawing_id) loop
		dbms_output.put_line(k.drawing_id||' - '||k.b_type||' - '||k.avg_ca);
		--!se calcula el promedio del ciclo de aparicion para que se pueda utilizar
		--!como metrica para la seleccion de jugadas 
		update olap_sys.s_calculo_stats
		   set avg_ciclo_aparicion = k.avg_ca
		     , b_avg_ciclo_aparicion = case when ciclo_aparicion < k.avg_ca then 1 else 0 end
			 , a_avg_ciclo_aparicion = case when ciclo_aparicion > k.avg_ca then 1 else 0 end
		 where drawing_id = k.drawing_id
		   and b_type = k.b_type;
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_avg_gl_ca;


--!en base al rango de historico identificar los patrones de las posicion 2-5
--!adicionalmente posicion 6
procedure get_patron_25_6(pn_digito			 number
						, pn_drawing_id		 number
						, pn_rango_historico number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'get_patron_25_6';
	lb$show_msg			 boolean := TRUE;
	
	cursor c_patron (pn_digito number
				   , pn_rango_historico number) is
	with resultados_tbl as (
	select gambling_id id
	     , comb1
		 , comb2
		 , comb3
		 , comb4
		 , comb5
		 , comb6
		 , additional
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id > pn_drawing_id - pn_rango_historico
	   and comb1 = pn_digito
	)
	, resultados_union_tbl as (
	select comb2 comb from resultados_tbl
	union all
	select comb3 from resultados_tbl
	union all
	select comb4 from resultados_tbl
	union all
	select comb5 from resultados_tbl
	)
	, resultados_group_tbl as (
	select '2-5' pos
		 , comb
		 , count(1) r_cnt
	  from resultados_union_tbl
	 group by comb
	union all
	select '6' pos
		 , comb6
		 , count(1) r_cnt
	  from resultados_tbl
	 group by comb6 
	union all
	select '1' pos
		 , comb1
		 , count(1) r_cnt
	  from resultados_tbl
	 group by comb1  
	 union all
	select 'adi' pos
		 , additional
		 , count(1) r_cnt
	  from resultados_tbl
	 group by additional  
	)
	select 
		   (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) sorteo_from
		 , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) sorteo_to
		 , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico)
		 - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) + 1 rango
		 , pos
		 , comb
		 , r_cnt
	  from resultados_group_tbl   
    where (pos = '2-5' and r_cnt > 1) 	 
       or pos = '6' 	  
	 order by pos, r_cnt desc, comb desc; 

	cursor c_jugadas (pn_ia1			 number) is
	select list_id
		 , id
		 , comb2||','||comb3||','||comb4||','||comb5 ia_25
		 , comb6 ia6
	  from olap_sys.gl_automaticas_detail
	 where comb1 = pn_ia1;

	cursor c_intersect (pv_posicion_25		varchar2
					  , pv_patron_25		varchar2) is
	select regexp_substr(pv_posicion_25,'[^,]+',1,level) digito_match
					   from dual 
					 connect by level <= length(pv_posicion_25)-length(replace(pv_posicion_25,',',''))+1
	intersect
	select pv_patron_25
	  from dual;	   
begin
	dbms_output.put_line('----------------------------------------');
	dbms_output.put_line('pn_digito: '||pn_digito);
	dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	dbms_output.put_line('pn_rango_historico: '||pn_rango_historico);

	--!obtiene el conteo de los digitos mas frecuentes entre la posicion 2 al 5 relacionados a los resultados
	for k in c_patron (pn_digito => pn_digito
				   , pn_rango_historico => pn_rango_historico) loop
		if lb$show_msg then
			dbms_output.put_line(k.SORTEO_FROM||' '||k.SORTEO_TO||' '||k.RANGO||' '||k.POS||' '||k.COMB||' '||k.R_CNT);	
		end if;	
		
		--!obtiene un string separado por comas de la posicion 2 al 5
		--!en una columna separada obtiene el valor del ID y de la posicion6
		for j in c_jugadas (pn_ia1 => pn_digito) loop
			if k.POS = '2-5' then				
				if lb$show_msg then
					dbms_output.put_line(j.id||' '||j.ia_25||' '||j.ia6);
				end if;	
				
				--!valida que el digito ganador se encuentre dentro de las posiciones 2 al 5 en las jugadas
				for m in c_intersect (pv_posicion_25	=> j.ia_25
									, pv_patron_25 => k.comb) loop
					if m.digito_match > 0 then
						if lb$show_msg then
							dbms_output.put_line(j.id||' '||k.R_CNT);
							lb$show_msg := false;
						end if;	
						
						--!cuando el digito existe se incrementan los contadores de la tabla
						update olap_sys.gl_automaticas_detail
						   set sorteo_from	= k.sorteo_from
							 , sorteo_to = k.sorteo_to
							 , patron_rango = k.rango
							 , patron_25_cnt = patron_25_cnt + 1
							 , patron_25_accum = patron_25_accum + k.r_cnt
							 , patron_25_history = patron_25_history||'|'||k.comb||'('||k.r_cnt||')'
						 where list_id = j.list_id
						   and id = j.id;	 
					end if;	
				end loop;					
			else
				update olap_sys.gl_automaticas_detail
				   set sorteo_from	= k.sorteo_from
					 , sorteo_to = k.sorteo_to
					 , patron_rango = k.rango
					 , patron_6_accum = k.r_cnt
				 where list_id = j.list_id
				   and id = j.id
				   and comb6 = k.comb;
			end if;					
		end loop;	
	end loop;			   
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end get_patron_25_6;


--!en base al rango de historico identificar los patrones de las posicion 2-5
--!adicionalmente posicion 6
procedure get_patron_handler(pn_drawing_id		number default 0
                           , pn_rango_historico number default 100) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'get_patron_handler';
 	ln$max_id	 		 NUMBER := 0;
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);
	
	if pn_drawing_id = 0 then
		select max(gambling_id)
		  into ln$max_id
		  from olap_sys.sl_gamblings;
	else
		ln$max_id := pn_drawing_id;
	end if;

	update olap_sys.gl_automaticas_detail
	   set sorteo_from = 0
	     , sorteo_to = 0
		 , patron_rango = 0
		 , patron_25_cnt = 0
		 , patron_25_accum = 0
		 , patron_25_history = null
		 , patron_6_accum = 0;
	--!en base al rango de historico identificar los patrones de las posicion 2-5
	--!adicionalmente posicion 6
	for ia1 in 1..9 loop
		get_patron_25_6(pn_digito => ia1
					  , pn_drawing_id => ln$max_id
				      , pn_rango_historico => pn_rango_historico);
	end loop;			  
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end get_patron_handler;


--!actualizar el historico del id que se pasa como parametro
procedure upd_pm_decenas_single(pn_drawing_id		number) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_pm_decenas_single';
   
    cursor c_resultados is 
    select gambling_id id
         , d1
         , d2
         , d3
         , d4
         , d5
         , d6
      from olap_sys.pm_mr_resultados_v2 
      where gambling_id = pn_drawing_id  
     order by gambling_id; 
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);
    
	--!actualizacion de un solo registro
    for r in c_resultados loop
        dbms_output.put_line(r.d1||' # '||r.d2||' # '||r.d3||' # '||r.d4||' # '||r.d5||' # '||r.d6);
		update OLAP_SYS.PM_DECENAS
           set MATCH_HISTORY = MATCH_HISTORY||'|'||to_char(r.id)
		     , MATCH_COUNT = MATCH_COUNT + 1
             , MAX_SORTEO_ID = pn_drawing_id
             , SORTEO_DIF = 0
			 , SORTEO_ID = pn_drawing_id
         where d1 = r.d1  
           and d2 = r.d2
           and d3 = r.d3
           and d4 = r.d4
           and d5 = r.d5
           and d6 = r.d6;
    end loop;  

	--!actualizacion de todo el historico excepto el registro que se acaba de actualizar
    update OLAP_SYS.PM_DECENAS
       set SORTEO_ID = pn_drawing_id
	     , SORTEO_DIF = SORTEO_ID - MAX_SORTEO_ID
 		 , UPDATED_DATE = SYSDATE
	 where SORTEO_DIF > 0;

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_pm_decenas_single;

--!actualizar el historico completo de las decenas
procedure upd_pm_decenas_all is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_pm_decenas_all';
    ln$sorteo_id         number := 0;
    cursor c_decenas is
    select d1
         , d2
         , d3
         , d4
         , d5
         , d6
         , count(1) d_cnt
         , min(gambling_id) min_id
         , max(gambling_id) max_id
      from olap_sys.pm_mr_resultados_v2 
     group by d1
         , d2
         , d3
         , d4
         , d5
         , d6
     order by d_cnt desc;
    
    cursor c_resultados is 
    select gambling_id id
         , d1
         , d2
         , d3
         , d4
         , d5
         , d6
      from olap_sys.pm_mr_resultados_v2 
     order by gambling_id; 
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);

    select max(gambling_id)
      into ln$sorteo_id 
      from olap_sys.pm_mr_resultados_v2;
      
    update OLAP_SYS.PM_DECENAS
       set MATCH_COUNT = 0
         , MATCH_HISTORY = NULL
         , SORTEO_ID = ln$sorteo_id
		 , UPDATED_DATE = SYSDATE;
           
    for k in c_decenas loop
        update OLAP_SYS.PM_DECENAS
           set MATCH_COUNT = k.d_cnt
             , MIN_SORTEO_ID = k.min_id
             , MAX_SORTEO_ID = k.max_id
             , SORTEO_DIF = ln$sorteo_id - k.max_id
         where d1 = k.d1  
           and d2 = k.d2
           and d3 = k.d3
           and d4 = k.d4
           and d5 = k.d5
           and d6 = k.d6;
    end loop;
    
    for r in c_resultados loop
        update OLAP_SYS.PM_DECENAS
           set MATCH_HISTORY = MATCH_HISTORY||'|'||to_char(r.id)
         where d1 = r.d1  
           and d2 = r.d2
           and d3 = r.d3
           and d4 = r.d4
           and d5 = r.d5
           and d6 = r.d6;
    end loop;    
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_pm_decenas_all;


--!actualizar las estaditicas almacenadas en la tabla pm_decenas
procedure upd_pm_decenas_handler(pn_drawing_id		number
							   , pv_execution_type  varchar2 default 'S') is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_pm_decenas_handler';
begin
	--!single
	if upper(pv_execution_type) = 'S' then
		--!actualizar el historico del id que se pasa como parametro
		upd_pm_decenas_single(pn_drawing_id	=> pn_drawing_id);
	--!all	
	elsif upper(pv_execution_type) = 'A' then
		--!actualizar el historico completo de las decenas
		upd_pm_decenas_all;
	end if;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_pm_decenas_handler;


--!se marcan las jugadas de la posicion 2 a las 5 que contengan los digitos mas frecuentes
--!se marcan las jugadas de la posicion 6 que contengan los digitos mas frecuentes
procedure upd_digito_25_handler(pn_rango_historico	number default 100) is
	LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'upd_digito_25_handler';
	ln$digito_found			number := 0;
	
	cursor c_digitos(pn_digito          number
				   , pn_rango_historico	number) is
	with siguiente_sorteo_tbl as (
	select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
	)
	, resultados_tbl as (
	select gambling_id id
		 , comb1
		 , comb2
		 , comb3
		 , comb4
		 , comb5
		 , comb6
		 , additional
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id > (select max_id from siguiente_sorteo_tbl) - nvl(pn_rango_historico,0)
	   and comb1 = pn_digito
	)
	, resultados_union_tbl as (
	select comb2 comb from resultados_tbl
	union all
	select comb3 from resultados_tbl
	union all
	select comb4 from resultados_tbl
	union all
	select comb5 from resultados_tbl
	)
	, resultados_group_tbl as (
	select '2-5' pos
		 , comb
		 , count(1) r_cnt
	  from resultados_union_tbl
	 group by comb
	union all
	select '6' pos
		 , comb6
		 , count(1) r_cnt
	  from resultados_tbl
	 group by comb6 
	union all
	select '1' pos
		 , comb1
		 , count(1) r_cnt
	  from resultados_tbl
	 group by comb1  
	 union all
	select 'adi' pos
		 , additional
		 , count(1) r_cnt
	  from resultados_tbl
	 group by additional  
	)
	select 
		   (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) sorteo_from
		 , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) sorteo_to
		 , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico)
		 - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - pn_rango_historico) + 1 rango
		 , pn_digito pos_1
		 , pos pos_n
		 , comb
		 , r_cnt
	  from resultados_group_tbl   
	 where pos in ('2-5','6');
--	 order by pos, r_cnt desc, comb desc; 	
	 
	cursor c_jugadas(pn_digito          number) is
	select id, comb1 ia1, comb2||','||comb3||','||comb4||','||comb5 ia25, comb6 ia6
	  from olap_sys.gl_automaticas_detail
	 where comb1 = pn_digito;

	cursor c_intersect(pv_ia25		varchar2
					 , pn_comb		number) is
	with intersect_tbl as (
	select to_number(regexp_substr(pv_ia25,'[^,]+',1,level)) digitos
	  from dual
	connect by level <= length(pv_ia25)-length(replace(pv_ia25,',',''))+1
	intersect
	select pn_comb
	  from dual
	)
	select case when digitos > 0 then 1 else 0 end digito_found
	  from intersect_tbl;	 
begin
	update olap_sys.gl_automaticas_detail
	   set digito_25_cnt = 0
		 , digito_25_accum = 0
		 , digito_25_history = NULL
		 , digito_6_cnt = 0;
		 
	for ia1 in 1..1 loop
		--dbms_output.put_line('ia1: '||ia1);
		for d in c_digitos(pn_digito => ia1
						 , pn_rango_historico	=> pn_rango_historico) loop
			
			--dbms_output.put_line('c_digitos -> comb: '||d.comb||' r_cnt: '||d.r_cnt);
			for j in c_jugadas(pn_digito => ia1) loop
				
				if d.pos_n = '2-5' then
					--dbms_output.put_line('     c_jugadas -> ia25: '||j.ia25);
					begin
						ln$digito_found := 0;
						
						--!cerrando cursor
						if c_intersect%isopen then
							close c_intersect;
						end if;	
						
						open c_intersect(pv_ia25 => j.ia25
									   , pn_comb => d.comb);
						fetch c_intersect into ln$digito_found;
						close c_intersect;	
					exception
						when no_data_found then
							ln$digito_found := 0;
					end;
					--dbms_output.put_line('     ln$digito_found: '||ln$digito_found);
					if ln$digito_found = 1 then
						--dbms_output.put_line('update1');
						update olap_sys.gl_automaticas_detail
						   set digito_25_cnt = digito_25_cnt + 1
						     , digito_25_accum = digito_25_accum + d.r_cnt
							 , digito_25_history = digito_25_history||'|'||d.comb||'('||d.r_cnt||')'
						 where id = j.id;	 
					end if;
				else
					--dbms_output.put_line('     ia6: '||j.ia6);
					if d.comb = j.ia6 then
						--dbms_output.put_line('update2');
						update olap_sys.gl_automaticas_detail
						   set digito_6_cnt = digito_6_cnt + 1
						 where id = j.id;						
					end if;
				end if;					
			end loop;
		end loop;				 	
	end loop;
exception
  when others then
    close c_intersect;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
--    raise; 
end upd_digito_25_handler;
	
--guarda en la tabla las jugadas seleccionadas por el master prompt
procedure save_claude_mp_predictions (
    pn_gambling_count  IN NUMBER DEFAULT 10,     -- Numero de jugadas seleccionadas por el master plan
	pn_gambling_id     IN NUMBER,                -- Sorteo al que corresponden las predicciones
    pv_plays           IN SYS.ODCIVARCHAR2LIST,  -- Array plano: 10 jugadas × 15 campos
    pv_result          OUT VARCHAR2              -- 'OK' o mensaje de error
) is
	v_emoji CONSTANT NVARCHAR2(1) := N'⭐';
    v_seq    NUMBER := 1;
    v_base   NUMBER;
BEGIN
    -- Cada jugada ocupa 16 posiciones en el array:
    -- [1]=COMB1, [2]=COMB2, [3]=COMB3, [4]=COMB4, [5]=COMB5, [6]=COMB6,
    -- [7]=SCORE, [8]=C1,    [9]=C2,    [10]=C3,   [11]=C4,   [12]=C5,
    -- [13]=C6,  [14]=CONFIDENCE_LABEL, [15]=GOLD_SIGNAL ('Y'/'N'),
    -- [16]=PRINCIPAL_REASON
    --
    -- EMOGI se calcula desde GOLD_SIGNAL (pos 15), NO desde CONFIDENCE_LABEL
    -- CONFIDENCE_LABEL es independiente: 'Alta'(≥90)·'Media'(70-89)·'Baja'(<70)

    FOR i IN 1..pn_gambling_count LOOP
        v_base := (i - 1) * 16;   -- ← 16 campos por jugada (era 15)
        INSERT INTO OLAP_SYS.MR_MP_PREDICTIONS (
            GAMBLING_ID, SEQ_ID,
            COMB1, COMB2, COMB3, COMB4, COMB5, COMB6,
            SCORE, C1, C2, C3, C4, C5, C6,
            CONFIDENCE_LABEL, EMOGI, PRINCIPAL_REASON, GL_GAMBLING_ID
        ) VALUES (
            pn_gambling_id, v_seq,
            TO_NUMBER(pv_plays(v_base + 1)),
            TO_NUMBER(pv_plays(v_base + 2)),
            TO_NUMBER(pv_plays(v_base + 3)),
            TO_NUMBER(pv_plays(v_base + 4)),
            TO_NUMBER(pv_plays(v_base + 5)),
            TO_NUMBER(pv_plays(v_base + 6)),
            TO_NUMBER(pv_plays(v_base + 7)),
            TO_NUMBER(pv_plays(v_base + 8)),
            TO_NUMBER(pv_plays(v_base + 9)),
            TO_NUMBER(pv_plays(v_base + 10)),
            TO_NUMBER(pv_plays(v_base + 11)),
            TO_NUMBER(pv_plays(v_base + 12)),
            TO_NUMBER(pv_plays(v_base + 13)),
            pv_plays(v_base + 14),           -- CONFIDENCE_LABEL (score-based)
            CASE WHEN pv_plays(v_base + 15) = 'Y'
                 THEN v_emoji ELSE NULL END,  -- EMOGI desde GOLD_SIGNAL pos 15
            pv_plays(v_base + 16),            -- PRINCIPAL_REASON (era pos 15)
            pn_gambling_id - 1                -- GL_GAMBLING_ID
        );
        v_seq := v_seq + 1;
    END LOOP;

    COMMIT;
    pv_result := 'OK - '||TO_CHAR(pn_gambling_count)||' jugadas guardadas para sorteo ' || pn_gambling_id;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        pv_result := 'ERROR: ' || SQLERRM;
END save_claude_mp_predictions;


--actualiza en la tabla las jugadas que fueron apostadas en el sorteo
PROCEDURE UPD_CLAUDE_BET_FLAG (
    pn_gambling_id     IN NUMBER,
    pv_plays           IN SYS.ODCIVARCHAR2LIST,
    pv_result          OUT VARCHAR2
) IS
    ------------------------------------------------------------
    -- CONFIGURACIÓN: 2 campos por jugada (SEQ_ID, COMB1)
    ------------------------------------------------------------
    c_FIELDS_PER_PLAY CONSTANT NUMBER := 2;
    c_SEQ_IDX         CONSTANT NUMBER := 1;
    c_COMB1_IDX       CONSTANT NUMBER := 2;

    ------------------------------------------------------------
    -- VARIABLES DE TRABAJO
    ------------------------------------------------------------
    v_play_count    NUMBER;         -- Jugadas calculadas automáticamente
    v_prev_comb1    VARCHAR2(10);
    v_curr_comb1    VARCHAR2(10);
    v_curr_seq      VARCHAR2(10);
    v_seq_list      VARCHAR2(4000);
    v_base          NUMBER;
    v_first_row     BOOLEAN := TRUE;
    v_sql           VARCHAR2(4000);
    v_total_rows    NUMBER := 0;
BEGIN

    ------------------------------------------------------------
    -- 1. VALIDACIÓN DEL ARRAY Y CÁLCULO AUTOMÁTICO DE JUGADAS
    ------------------------------------------------------------
    IF pv_plays IS NULL OR pv_plays.COUNT = 0 THEN
        pv_result := 'ERROR: El array pv_plays está vacío o es NULL';
        RETURN;
    END IF;

    -- Calcular cuántas jugadas completas vienen en el array
    v_play_count := pv_plays.COUNT / c_FIELDS_PER_PLAY;

    -- Verificar que el array tenga un número par de elementos
    IF MOD(pv_plays.COUNT, c_FIELDS_PER_PLAY) != 0 THEN
        pv_result := 'ERROR: Array incompleto. Se recibieron ' || pv_plays.COUNT || 
                     ' elementos, pero cada jugada requiere exactamente ' || 
                     c_FIELDS_PER_PLAY || ' campos (SEQ_ID, COMB1).';
        RETURN;
    END IF;

    ------------------------------------------------------------
    -- 2. RECORRER EL ARRAY USANDO v_play_count CALCULADO
    ------------------------------------------------------------
    FOR i IN 1..v_play_count LOOP
        
        v_base := (i - 1) * c_FIELDS_PER_PLAY;

        -- Leer en orden: SEQ_ID primero, COMB1 segundo
        v_curr_seq   := TRIM(pv_plays(v_base + c_SEQ_IDX));
        v_curr_comb1 := TRIM(pv_plays(v_base + c_COMB1_IDX));

        -- Validar numéricos
        IF NOT REGEXP_LIKE(v_curr_seq, '^\d+$') OR NOT REGEXP_LIKE(v_curr_comb1, '^\d+$') THEN
            pv_result := 'ERROR: Valor no numérico en jugada ' || i || 
                         '. SEQ=[' || v_curr_seq || '] COMB1=[' || v_curr_comb1 || ']';
            RETURN;
        END IF;

        ------------------------------------------------------------
        -- 3. CONTROL BREAK POR COMB1
        ------------------------------------------------------------
        IF v_first_row THEN
            v_prev_comb1 := v_curr_comb1;
            v_seq_list   := v_curr_seq;
            v_first_row  := FALSE;

        ELSIF v_curr_comb1 = v_prev_comb1 THEN
            v_seq_list := v_seq_list || ',' || v_curr_seq;

        ELSE

            -- Cerrar grupo anterior
            v_sql := 'UPDATE OLAP_SYS.MR_MP_PREDICTIONS
                         SET BET_FLAG = ''Y''
                       WHERE GAMBLING_ID = ' || TO_CHAR(pn_gambling_id) || '
                         AND COMB1 = ' || v_prev_comb1 || '
                         AND SEQ_ID IN (' || v_seq_list || ')';

            EXECUTE IMMEDIATE v_sql;
            v_total_rows := v_total_rows + SQL%ROWCOUNT;

            -- Iniciar nuevo grupo
            v_prev_comb1 := v_curr_comb1;
            v_seq_list   := v_curr_seq;
        END IF;

    END LOOP;

    ------------------------------------------------------------
    -- 4. PROCESAR ÚLTIMO GRUPO PENDIENTE
    ------------------------------------------------------------
    IF NOT v_first_row THEN
        v_sql := 'UPDATE OLAP_SYS.MR_MP_PREDICTIONS
                     SET BET_FLAG = ''Y''
                   WHERE GAMBLING_ID = ' || TO_CHAR(pn_gambling_id) || '
                     AND COMB1 = ' || v_prev_comb1 || '
                     AND SEQ_ID IN (' || v_seq_list || ')';				 

        EXECUTE IMMEDIATE v_sql;
        v_total_rows := v_total_rows + SQL%ROWCOUNT;
    END IF;

    ------------------------------------------------------------
    -- 5. CONFIRMAR Y RESPONDER
    ------------------------------------------------------------
    COMMIT;
    pv_result := 'OK - ' || TO_CHAR(v_total_rows) || ' filas actualizadas. ' ||
                 'Sorteo ' || TO_CHAR(pn_gambling_id) || '. ' ||
                 TO_CHAR(v_play_count) || ' jugadas procesadas.';

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        pv_result := 'ERROR: ' || SQLERRM;
END UPD_CLAUDE_BET_FLAG;

--inserta informacion en la tabla GL_HISTORY_PERCENTAGES
PROCEDURE INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE              OLAP_SYS.GL_HISTORY_PERCENTAGES.HISTORY_RANGE%TYPE
                                    , pv_GL_TYPE                    OLAP_SYS.GL_HISTORY_PERCENTAGES.GL_TYPE%TYPE
                                    , pv_B_TYPE                     OLAP_SYS.GL_HISTORY_PERCENTAGES.B_TYPE%TYPE
                                    , pn_HISTORY_LAST_ID            OLAP_SYS.GL_HISTORY_PERCENTAGES.HISTORY_LAST_ID%TYPE
                                    , pn_LT_REAL_CURRENT_COLOR      OLAP_SYS.GL_HISTORY_PERCENTAGES.LT_REAL_CURRENT_COLOR%TYPE
                                    , pn_LT_NEXT_COLOR              OLAP_SYS.GL_HISTORY_PERCENTAGES.LT_NEXT_COLOR%TYPE
                                    , pn_HISTORY_FREQUENCY          OLAP_SYS.GL_HISTORY_PERCENTAGES.HISTORY_FREQUENCY%TYPE
                                    , pn_TOTAL_TRANSITIONS          OLAP_SYS.GL_HISTORY_PERCENTAGES.TOTAL_TRANSITIONS%TYPE
                                    , pn_HISTORY_PCT                OLAP_SYS.GL_HISTORY_PERCENTAGES.HISTORY_PCT%TYPE
                                    , pn_NEXT_ID                    OLAP_SYS.GL_HISTORY_PERCENTAGES.NEXT_ID%TYPE
                                    , pn_AVAILABLE_DIGITS           OLAP_SYS.GL_HISTORY_PERCENTAGES.AVAILABLE_DIGITS%TYPE
                                    , pn_TOTAL_DIGIT_BY_POSITION    OLAP_SYS.GL_HISTORY_PERCENTAGES.TOTAL_DIGIT_BY_POSITION%TYPE
                                    , pn_PCT_AVAILABILITY           OLAP_SYS.GL_HISTORY_PERCENTAGES.PCT_AVAILABILITY%TYPE
                                    , pn_PCT_ADJUSTED               OLAP_SYS.GL_HISTORY_PERCENTAGES.PCT_ADJUSTED%TYPE 
                                    , pv_REPITE_FLAG                OLAP_SYS.GL_HISTORY_PERCENTAGES.REPITE_FLAG%TYPE
                                    , pn_GAMBLING_ID                OLAP_SYS.GL_HISTORY_PERCENTAGES.GAMBLING_ID%TYPE) IS    
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_gl_history_percentages';
BEGIN
    INSERT INTO OLAP_SYS.GL_HISTORY_PERCENTAGES(HISTORY_RANGE
                                              , GL_TYPE
                                              , B_TYPE
                                              , HISTORY_LAST_ID
                                              , LT_REAL_CURRENT_COLOR
                                              , LT_NEXT_COLOR
                                              , HISTORY_FREQUENCY
                                              , TOTAL_TRANSITIONS
                                              , HISTORY_PCT
                                              , NEXT_ID
                                              , AVAILABLE_DIGITS
                                              , TOTAL_DIGIT_BY_POSITION
                                              , PCT_AVAILABILITY
                                              , PCT_ADJUSTED
                                              , REPITE_FLAG
                                              , GAMBLING_ID)
   VALUES(pn_HISTORY_RANGE
        , pv_GL_TYPE
        , pv_B_TYPE
        , pn_HISTORY_LAST_ID
        , pn_LT_REAL_CURRENT_COLOR
        , pn_LT_NEXT_COLOR
        , pn_HISTORY_FREQUENCY
        , pn_TOTAL_TRANSITIONS
        , pn_HISTORY_PCT
        , pn_NEXT_ID
        , pn_AVAILABLE_DIGITS
        , pn_TOTAL_DIGIT_BY_POSITION
        , pn_PCT_AVAILABILITY
        , pn_PCT_ADJUSTED
        , pv_REPITE_FLAG
        , pn_GAMBLING_ID+1);
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;         
END INS_GL_HISTORY_PERCENTAGES;

--calcula probabilidades de repeticion de color por posicion en base a datos historicos
PROCEDURE GL_HISTORY_PERCENTAGES_HANDLER (pn_drawing_id		    number
                                        , pn_HISTORY_RANGE      OLAP_SYS.GL_HISTORY_PERCENTAGES.HISTORY_RANGE%TYPE DEFAULT 100
                                        , pv_GL_TYPE            OLAP_SYS.GL_HISTORY_PERCENTAGES.GL_TYPE%TYPE DEFAULT 'LT') IS
	LV$PROCEDURE_NAME       constant varchar2(30) := 'gl_history_percentages_handler';

    CURSOR C_POS1 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS       
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT1 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT1                                   AS COLOR_ACTUAL,
			   LEAD(CLT1) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B1'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B1' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt;



    CURSOR C_POS2 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS       
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT2 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT2                                   AS COLOR_ACTUAL,
			   LEAD(CLT2) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B2'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B2' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt;

	
    CURSOR C_POS3 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS       
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT3 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT3                                   AS COLOR_ACTUAL,
			   LEAD(CLT3) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B3'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B3' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt; 


    CURSOR C_POS4 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS       
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT4 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT4                                   AS COLOR_ACTUAL,
			   LEAD(CLT4) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B4'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B4' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt;
	

    CURSOR C_POS5 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS       
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT5 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT5                                   AS COLOR_ACTUAL,
			   LEAD(CLT5) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B5'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B5' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt;
	
	
    CURSOR C_POS6 (pn_drawing_id	 number
                 , pn_history_range  number
				 , pv_GL_TYPE        varchar2) IS    
	WITH CURRENT_COLOR_TBL AS (
		SELECT CLT6 AS COLOR_ACTUAL_CORRIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID = pn_drawing_id
	)
	, COLORES_REF AS (
		SELECT 0 AS COLOR FROM DUAL UNION ALL
		SELECT 1           FROM DUAL UNION ALL
		SELECT 2           FROM DUAL UNION ALL
		SELECT 3           FROM DUAL
	)
	, HISTORY_TBL AS (
		SELECT GAMBLING_ID,
			   CLT6                                   AS COLOR_ACTUAL,
			   LEAD(CLT6) OVER (ORDER BY GAMBLING_ID) AS COLOR_SIGUIENTE
		FROM OLAP_SYS.PM_MR_RESULTADOS_V3
		WHERE GAMBLING_ID >= pn_drawing_id - pn_history_range
	)
	, TRANSICIONES_TBL AS (
		SELECT COLOR_ACTUAL,
			   COLOR_SIGUIENTE,
			   COUNT(1) AS R_CNT
		FROM HISTORY_TBL
		WHERE COLOR_SIGUIENTE IS NOT NULL
		GROUP BY COLOR_ACTUAL, COLOR_SIGUIENTE
	)
	, TOTALES_TBL AS (
		SELECT COLOR_ACTUAL,
			   SUM(R_CNT) AS TOTAL_TRANSICIONES
		FROM TRANSICIONES_TBL
		GROUP BY COLOR_ACTUAL
	)
	, GL_CONTEOS_TBL AS (
		SELECT DISTINCT
			   GL_COLOR_END AS COLOR_SIGUIENTE,
			   GL_CNT_END   AS DIGITOS_DISPONIBLES
		FROM OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
		WHERE GL_TYPE        = 'LT'
		  AND B_TYPE_END     = 'B6'
		  AND DRAWING_ID_END = (SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)
	)
	, GL_TOTAL_TBL AS (
		SELECT NVL(SUM(DIGITOS_DISPONIBLES), 0) AS TOTAL_DIGITOS_POSICION
		FROM GL_CONTEOS_TBL
	)
	SELECT DISTINCT
		pn_history_range AS RANGO_HISTORICO,
		'B6' AS B_TYPE,
		(SELECT MAX(GAMBLING_ID) FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)                      AS ULTIMO_SORTEO,
		-- Color actual real del último sorteo
		cct.COLOR_ACTUAL_CORRIENTE               AS COLOR_ACTUAL_REAL,
		-- Posible color siguiente
		cr.COLOR                                 AS COLOR_SIGUIENTE,
		NVL(t.R_CNT, 0)                          AS FRECUENCIA_HISTORICA,
		NVL(tt.TOTAL_TRANSICIONES, 0)            AS TOTAL_TRANSICIONES,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			THEN ROUND(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES, 1)
			ELSE 0
		END                                      AS PCT_HISTORICA,
		(SELECT MAX(GAMBLING_ID)+1 FROM HISTORY_TBL WHERE COLOR_SIGUIENTE IS NOT NULL)  AS SIGUIENTE_SORTEO,    
		NVL(gl.DIGITOS_DISPONIBLES, 0)           AS DIGITOS_DISPONIBLES,
		glt.TOTAL_DIGITOS_POSICION,
		ROUND(
			NVL(gl.DIGITOS_DISPONIBLES, 0) * 100.0
			/ NULLIF(glt.TOTAL_DIGITOS_POSICION, 0)
		, 1)                                     AS PCT_DISPONIBILIDAD,
		CASE
			WHEN NVL(tt.TOTAL_TRANSICIONES, 0) > 0
			 AND glt.TOTAL_DIGITOS_POSICION > 0
			THEN ROUND(
					(NVL(t.R_CNT, 0) * 100.0 / tt.TOTAL_TRANSICIONES)
					* (NVL(gl.DIGITOS_DISPONIBLES, 0)
					   / glt.TOTAL_DIGITOS_POSICION)
				 , 1)
			ELSE 0
		END                                      AS PCT_AJUSTADA,
		CASE WHEN cr.COLOR = cct.COLOR_ACTUAL_CORRIENTE
			 THEN 'Y' ELSE 'N'
		END                                      AS SE_REPITE
	FROM COLORES_REF cr
	CROSS JOIN CURRENT_COLOR_TBL cct
	LEFT JOIN TRANSICIONES_TBL t
		ON  t.COLOR_ACTUAL    = cct.COLOR_ACTUAL_CORRIENTE
		AND t.COLOR_SIGUIENTE = cr.COLOR
	LEFT JOIN TOTALES_TBL tt
		ON  tt.COLOR_ACTUAL   = cct.COLOR_ACTUAL_CORRIENTE
	LEFT JOIN GL_CONTEOS_TBL gl
		ON  gl.COLOR_SIGUIENTE = cr.COLOR
	CROSS JOIN GL_TOTAL_TBL glt;                                    
BEGIN 

    FOR K IN C_POS1 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;

    FOR K IN C_POS2 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;


    FOR K IN C_POS3 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;


    FOR K IN C_POS4 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;
	
    FOR K IN C_POS5 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;

	
    FOR K IN C_POS6 (pn_drawing_id	=> pn_drawing_id
                   , pn_history_range => pn_history_range
				   , pv_GL_TYPE => pv_GL_TYPE) LOOP
        INS_GL_HISTORY_PERCENTAGES (pn_HISTORY_RANGE => k.RANGO_HISTORICO
                                    , pv_GL_TYPE => pv_GL_TYPE
                                    , pv_B_TYPE => k.B_TYPE
                                    , pn_HISTORY_LAST_ID => k.ULTIMO_SORTEO
                                    , pn_LT_REAL_CURRENT_COLOR => k.COLOR_ACTUAL_REAL
                                    , pn_LT_NEXT_COLOR => k.COLOR_SIGUIENTE
                                    , pn_HISTORY_FREQUENCY => k.FRECUENCIA_HISTORICA
                                    , pn_TOTAL_TRANSITIONS => k.TOTAL_TRANSICIONES
                                    , pn_HISTORY_PCT => k.PCT_HISTORICA
                                    , pn_NEXT_ID => k.SIGUIENTE_SORTEO
                                    , pn_AVAILABLE_DIGITS => k.DIGITOS_DISPONIBLES
                                    , pn_TOTAL_DIGIT_BY_POSITION => k.TOTAL_DIGITOS_POSICION
                                    , pn_PCT_AVAILABILITY => k.PCT_DISPONIBILIDAD
                                    , pn_PCT_ADJUSTED => k.PCT_AJUSTADA
                                    , pv_REPITE_FLAG => k.SE_REPITE
                                    , pn_GAMBLING_ID => pn_drawing_id);
    END LOOP;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;     
END GL_HISTORY_PERCENTAGES_HANDLER;
	
end w_gl_automaticas_pkg;
/
show errors;