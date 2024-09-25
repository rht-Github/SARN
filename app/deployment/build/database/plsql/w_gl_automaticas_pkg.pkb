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
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
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
												 , ia1
												 , ia2
												 , ia3
												 , ia4
												 , ia5
												 , ia6
												 , ia_rojo
												 , seq_id
												 , pn_cnt
												 , none_cnt
												 , par_cnt
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
			  );
	end loop;	  
	commit;
exception
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
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
    raise; 	
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
    raise; 	
end upd_gl_automaticas_header;


--!actualizar los valores de frecuencia, ley del tercio, ciclo de aparicion, pronos por ciclo y numeros preferidos
procedure upd_gl_automaticas_detail(pn_drawing_id		number
								  , pv_ca_comb_flag		varchar2
								  , pn_list_id          number) is 
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_gl_automaticas_detail';
	
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
		 , decode(ca1,-1,0,ca1)+decode(ca2,-1,0,ca2)+decode(ca3,-1,0,ca3)+decode(ca4,-1,0,ca4)+decode(ca5,-1,0,ca5)+decode(ca6,-1,0,ca6) ca_sum
		 , comb_sum
	  from olap_sys.gl_automaticas_detail
     where list_id = pn_list_id		  
	)
	select list_id
		 , id 
	  from automaticas_detail_tbl 
	 where (ca_sum,comb_sum) in (select sum_ca, comb_sum
								  from ca_comb_output_tbl);					  
begin	
	--!limpiar todos los campos
	update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
	   set FR1 = -1
		 , LT1 = -1
		 , CA1 = 0
		 , PXC1 = 0
		 , PF1 = 0
		 , CHG1 = '.'
		 , FR2 = -1
		 , LT2 = -1
		 , CA2 = 0
		 , PXC2 = 0
		 , PF2 = 0
		 , CHG2 = '.'
		 , FR3 = -1
		 , LT3 = -1
		 , CA3 = 0
		 , PXC3 = 0
		 , PF3 = 0
		 , CHG3 = '.'
	     , FR4 = -1
		 , LT4 = -1
		 , CA4 = 0
		 , PXC4 = 0
		 , PF4 = 0
		 , CHG4 = '.'
	     , FR5 = -1
		 , LT5 = -1
		 , CA5 = 0
		 , PXC5 = 0
		 , PF5 = 0
		 , CHG5 = '.'
		 , FR6 = -1
		 , LT6 = -1
		 , CA6 = 0
		 , PXC6 = 0
		 , PF6 = 0
		 , CHG6 = '.'
		 , SORTEO_ACTUAL = null
		 , INCIDENCIA = 0
		 , INCIDENCIA_CNT = 0
		 , JUGAR_FLAG = 'N'
	 WHERE LIST_ID = pn_list_id
	   AND EXISTS (SELECT 1
                     FROM OLAP_SYS.GL_AUTOMATICAS_HEADER AH
                    WHERE AH.LIST_ID = AD.LIST_ID
                      AND AH.SORTEO_FINAL IS NULL
					  AND AH.LIST_ID = pn_list_id)	 
		 ;
	
	for i in c_main (pn_drawing_id => pn_drawing_id) loop
		--!posicion1
		if i.b_type = 'B1' then
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
			   set FR1 = i.fre
			     , LT1 = i.lt
				 , CA1 = i.ca
				 , PXC1 = i.pxc
				 , PF1 = i.pref
				 , CHG1 = I.CHG
				 , SORTEO_ACTUAL = pn_drawing_id
		     where IA1 = i.digit
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
			   set FR2 = i.fre
			     , LT2 = i.lt
				 , CA2 = i.ca
				 , PXC2 = i.pxc
				 , PF2 = i.pref
				 , CHG2 = I.CHG
		     where IA2 = i.digit
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
			   set FR3 = i.fre
			     , LT3 = i.lt
				 , CA3 = i.ca
				 , PXC3 = i.pxc
				 , PF3 = i.pref
				 , CHG3 = I.CHG
		     where IA3 = i.digit
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
			   set FR4 = i.fre
			     , LT4 = i.lt
				 , CA4 = i.ca
				 , PXC4 = i.pxc
				 , PF4 = i.pref
				 , CHG4 = I.CHG
		     where IA4 = i.digit
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
			   set FR5 = i.fre
			     , LT5 = i.lt
				 , CA5 = i.ca
				 , PXC5 = i.pxc
				 , PF5 = i.pref
				 , CHG5 = I.CHG
		     where IA5 = i.digit
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
			   set FR6 = i.fre
			     , LT6 = i.lt
				 , CA6 = i.ca
				 , PXC6 = i.pxc
				 , PF6 = i.pref
				 , CHG6 = I.CHG
		     where IA6 = i.digit
			   and LIST_ID = pn_list_id
			   and exists (select 1
                             from olap_sys.gl_automaticas_header ah
                            where ah.list_id = ad.list_id
                              and ah.sorteo_final is null
							  and ah.list_id = pn_list_id);				 
		end if;	
	end loop;

    --!actualizar el ca_sum
    update OLAP_SYS.GL_AUTOMATICAS_DETAIL AD
	   set CA_SUM = CA1 + CA2 + CA3 + CA4 + CA5 + CA6
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

	--!actualizacion de los rangos de ca_sum y comb_sum mas ganadores
	if pv_ca_comb_flag = 'Y' then
		for v in c_ca_comb_validacion loop
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set JUGAR_FLAG = 'Y'
			 where list_id = v.list_id
			   and id = v.id
			   and list_id = pn_list_id;
		end loop;
	end if;
	commit;
exception
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end upd_gl_automaticas_detail;

	 
--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure contar_aciertos_repetidos(pn_drawing_id		number
								  , pn_list_id          number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'contar_aciertos_repetidos';
	ln$aciertos_sum					 number := 0;
	ln$row_cnt                       number := 0;

	--!cursor global de resultados del sorteo
	cursor c_resultados (pn_drawing_id		number) is
	with resultado_tbl as (
	select comb1||','||comb2||','||comb3||','||comb4||','||comb5||','||comb6 str
	     , gambling_id id
	  from olap_sys.sl_gamblings
	 where gambling_id = pn_drawing_id 
	) select regexp_substr((select str from resultado_tbl),'[^,]+',1,level) digit, (select id from resultado_tbl) id
					   from dual 
					 connect by level <= length((select str from resultado_tbl))-length(replace((select str from resultado_tbl),',',''))+1;	
	
	--!cursor para contar jugadas con aciertos	
	cursor c_aciertos (pn_list_id		number) is
	select ad.gambling_type, ad.list_id, ad.id, ad.aciertos_cnt
	  from olap_sys.gl_automaticas_header ah
	     , olap_sys.gl_automaticas_detail ad
	 where ah.gambling_type = ad.gambling_type 
	   and ah.list_id = ad.list_id
	   and ah.sorteo_final is null
	   and ad.aciertos_cnt > 0
	   and ah.list_id = pn_list_id; 

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
	--!verificando existencia de registros en la tabla detalle
	select count(1) cnt
	  into ln$row_cnt
	  from olap_sys.gl_automaticas_detail
	 where list_id = pn_list_id; 
	
	--!el proceso continua solo si hay registros en la tabla detalle
	--!de esta forma aunque solo exista el registro valido en la tabla maestra
	--!no se continua con el proceso
	if ln$row_cnt > 0 then
		--!limpiando el contador de aciertos y numeros repetidos
		update olap_sys.gl_automaticas_detail
		   set aciertos_cnt = 0
			 , repetidos_cnt = 0
		 where list_id = pn_list_id;
				   
		for i in c_main (pn_drawing_id => pn_drawing_id -1) loop
			if i.winner = CV$ACIERTO then
				dbms_output.put_line(i.id||' - '||i.b_type||' - '||i.digit||' - '||i.winner||' - '||pn_list_id);
			
				--!posicion1
				if i.b_type = 'B1' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1
					 where IA1 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;

				--!posicion2
				if i.b_type = 'B2' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1
					 where IA2 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;
				
				--!posicion3
				if i.b_type = 'B3' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1
					 where IA3 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;		

				--!posicion4
				if i.b_type = 'B4' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1			    
					 where IA4 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;	

				--!posicion5
				if i.b_type = 'B5' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1
					 where IA5 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;	

				--!posicion6
				if i.b_type = 'B6' then
					update OLAP_SYS.GL_AUTOMATICAS_DETAIL
					   set ACIERTOS_CNT = ACIERTOS_CNT + 1
					 where IA6 = i.digit
					   and LIST_ID = pn_list_id;		 		
				end if;
			end if;
		end loop;	

		--!actualizando el campo de aciertos history
		for a in c_aciertos (pn_list_id => pn_list_id) loop
            update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set ACIERTOS_HISTORY = ACIERTOS_HISTORY||'~'||pn_drawing_id||'('||a.ACIERTOS_CNT||')'
				 , ACIERTOS_ACCUM = ACIERTOS_ACCUM + a.ACIERTOS_CNT
                 , creation_date = sysdate
			 where gambling_type = a.gambling_type
			   and list_id = a.list_id
			   and id = a.id
			   and list_id = pn_list_id;		
		end loop;
		
		--!actualizando el campo de numeros repetidos
		--!en base a los resultados del sorteo anterior
		for r in c_resultados (pn_drawing_id	=> pn_drawing_id) loop
			dbms_output.put_line(r.id||' - '||r.digit);
			
			--!posicion1
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA1 = r.digit
			   and LIST_ID = pn_list_id; 
			
			dbms_output.put_line(sql%rowcount||' repetidos - B1');
			
			--!posicion2
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA2 = r.digit
			   and LIST_ID = pn_list_id; 

			dbms_output.put_line(sql%rowcount||' repetidos - B2');

			--!posicion3
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA3 = r.digit
			   and LIST_ID = pn_list_id; 

			dbms_output.put_line(sql%rowcount||' repetidos - B3');
			
			--!posicion4
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA4 = r.digit
			   and LIST_ID = pn_list_id; 
			
			dbms_output.put_line(sql%rowcount||' repetidos - B4');
			
			--!posicion5
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA5 = r.digit
			   and LIST_ID = pn_list_id; 		 

			dbms_output.put_line(sql%rowcount||' repetidos - B5');
			
			--!posicion6
			update OLAP_SYS.GL_AUTOMATICAS_DETAIL
			   set REPETIDOS_CNT = nvl(REPETIDOS_CNT,0) + 1
			 where IA6 = r.digit
			   and LIST_ID = pn_list_id; 

			dbms_output.put_line(sql%rowcount||' repetidos - B6');	
		end loop;
		
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
	end if;	
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end contar_aciertos_repetidos;


--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
procedure aciertos_repetidos_handler(pn_drawing_id		number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'aciertos_repetidos_handler';
begin
	for k in c_automaticas_header loop
		--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
		contar_aciertos_repetidos(pn_drawing_id	=> pn_drawing_id
							    , pn_list_id    => k.list_id);
	end loop;
end aciertos_repetidos_handler; 	


--!handler para actualizar jugadas en la tabla GL_AUTOMATICAS_DETAIL
--!en base a la info de gigaloterias
procedure upd_gl_automaticas_handler(pn_drawing_id     number
								   , pv_ca_comb_flag    varchar2 default 'Y') is
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
								
		--!actualizar bandera de jugadas a Y para las jugadas mas ganadoras
		--!cuyo percentile sea mayour a 55
		update olap_sys.gl_automaticas_detail 
		   set jugar_flag='Y'
			 , updated_date = sysdate
		 where jugar_flag='N' 
		   and aciertos_accum > (   
		with group_tbl as (   
		select aciertos_accum
			 , count(1) cnt
		  from olap_sys.gl_automaticas_detail 
		 where jugar_flag='N' 
		 group by aciertos_accum
		)
		select percentile_disc(0.55) within group (order by aciertos_accum) perc_aciertos
		  from group_tbl);								
	end loop;						
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
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
		 , case when olap_sys.w_common_pkg.is_prime_number(comb1) = 1 then 1 else 0 end primo1
		 , case when olap_sys.w_common_pkg.is_prime_number(comb2) = 1 then 1 else 0 end primo2       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb3) = 1 then 1 else 0 end primo3
		 , case when olap_sys.w_common_pkg.is_prime_number(comb4) = 1 then 1 else 0 end primo4 
		 , case when olap_sys.w_common_pkg.is_prime_number(comb5) = 1 then 1 else 0 end primo5       
		 , case when olap_sys.w_common_pkg.is_prime_number(comb6) = 1 then 1 else 0 end primo6
		 , case when mod(comb1,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb1) = 0 then 1 else 0 end impar1
		 , case when mod(comb2,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb2) = 0 then 1 else 0 end impar2       
		 , case when mod(comb3,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb3) = 0 then 1 else 0 end impar3
		 , case when mod(comb4,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb4) = 0 then 1 else 0 end impar4 
		 , case when mod(comb5,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb5) = 0 then 1 else 0 end impar5       
		 , case when mod(comb6,2) > 0 and olap_sys.w_common_pkg.is_prime_number(comb6) = 0 then 1 else 0 end impar6   
		 , case when mod(comb1,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb1) = 0 then 1 else 0 end par1
		 , case when mod(comb2,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb2) = 0 then 1 else 0 end par2       
		 , case when mod(comb3,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb3) = 0 then 1 else 0 end par3
		 , case when mod(comb4,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb4) = 0 then 1 else 0 end par4 
		 , case when mod(comb5,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb5) = 0 then 1 else 0 end par5       
		 , case when mod(comb6,2) = 0 and olap_sys.w_common_pkg.is_prime_number(comb6) = 0 then 1 else 0 end par6             
         , case when chng_pos1 is null then 0 else 1 end chng1 
         , case when chng_pos2 is null then 0 else 1 end chng2 
         , case when chng_pos3 is null then 0 else 1 end chng3 
         , case when chng_pos4 is null then 0 else 1 end chng4 
         , case when chng_pos5 is null then 0 else 1 end chng5 
         , case when chng_pos6 is null then 0 else 1 end chng6 
         , case when pre1 is null then 0 else 1 end pref1
         , case when pre2 is null then 0 else 1 end pref2
         , case when pre3 is null then 0 else 1 end pref3
         , case when pre4 is null then 0 else 1 end pref4
         , case when pre5 is null then 0 else 1 end pref5
         , case when pre6 is null then 0 else 1 end pref6
         , case when pxc1 is null then 0 else 1 end pxc1
         , case when pxc2 is null then 0 else 1 end pxc2
         , case when pxc3 is null then 0 else 1 end pxc3
         , case when pxc4 is null then 0 else 1 end pxc4
         , case when pxc5 is null then 0 else 1 end pxc5
         , case when pxc6 is null then 0 else 1 end pxc6		 
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
           pxc1, pxc2, pxc3, pxc4, pxc5, pxc6
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
		   and prediccion_tipo = 'LT';

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
		   and prediccion_tipo = 'FR';
		
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
		   and prediccion_tipo = 'PRIMO';

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
		   and prediccion_tipo = 'IMPAR';			   

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
		   and prediccion_tipo = 'PAR';			   

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
		   and prediccion_tipo = 'CHNG';	

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
		   and prediccion_tipo = 'DIGIT';	

		dbms_output.put_line('favorables');
		dbms_output.put_line(k.pref1||' - '||k.pref2||' - '||k.pref3||' - '||k.pref4||' - '||k.pref5||' - '||k.pref6);
		--digits
		update olap_sys.predicciones_all
		   set res1 = k.pref1
		     , res2 = k.pref2
			 , res3 = k.pref3
			 , res4 = k.pref4
			 , res5 = k.pref5
			 , res6 = k.pref6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and prediccion_tipo = 'PREF';

		dbms_output.put_line('PXC');
		dbms_output.put_line(k.pxc1||' - '||k.pxc2||' - '||k.pxc3||' - '||k.pxc4||' - '||k.pxc5||' - '||k.pxc6);
		--digits
		update olap_sys.predicciones_all
		   set res1 = k.pxc1
		     , res2 = k.pxc2
			 , res3 = k.pxc3
			 , res4 = k.pxc4
			 , res5 = k.pxc5
			 , res6 = k.pxc6
		 where prediccion_sorteo = ln$sorteo_anterior
		   and prediccion_tipo = 'PXC';	
		   
		/*estos son los valores validos para esta actualizacion
		cuando pxcN tiene valor se transforma en 1
		cuando preN tiene valor se transforma en 2
		pxcN	preN	posN
		0		0		0
		0		2		3
		1		0		4
		1		2		5
		*/
		/*
		update olap_sys.predicciones_all
		   set res1 = k.pre1
		     , res2 = k.pre2
			 , res3 = k.pre3
			 , res4 = k.pre4
			 , res5 = k.pre5
			 , res6 = k.pre6
		 where prediccion_sorteo = pn_drawing_id
		   and prediccion_tipo = 'Preferente';
		*/   
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
       and prediccion_tipo in ('IMPAR','PAR','PRIMO');

    --actualizando las columnas match para las predicciones relacionadas a CHNG
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = res3 then 1 else 0 end
		 , match4 = case when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = res6 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and prediccion_tipo in ('CHNG','DIGIT');
	   
    --actualizando las columnas match para las predicciones relacionadas a LT y FR
	update olap_sys.predicciones_all
	   set match1 = case when pred1 = '#' and res1 = '#' then 0 when pred1 = res1 then 1 else 0 end
		 , match2 = case when pred2 = '#' and res2 = '#' then 0 when pred2 = res2 then 1 else 0 end
		 , match3 = case when pred3 = '#' and res3 = '#' then 0 when pred3 = res3 then 1 else 0 end
		 , match4 = case when pred4 = '#' and res4 = '#' then 0 when pred4 = res4 then 1 else 0 end
		 , match5 = case when pred5 = '#' and res5 = '#' then 0 when pred5 = res5 then 1 else 0 end
		 , match6 = case when pred6 = '#' and res6 = '#' then 0 when pred6 = res6 then 1 else 0 end
	 where prediccion_sorteo = ln$sorteo_anterior
       and prediccion_tipo in ('LT','FR');

    --actualizando el contador de match
	update olap_sys.predicciones_all
	   set match_cnt = match1 + match2 + match3 + match4 + match5 + match6
	     , prediccion_fecha = (select to_date(gambling_date,'DD-MM-YYYY') from olap_sys.pm_mr_resultados_v2 where gambling_id = ln$sorteo_anterior) 
	 where prediccion_sorteo = ln$sorteo_anterior;	
	 
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
         , case when olap_sys.w_common_pkg.is_prime_number(ia1) = 1 then 'PR' else 
		   case when mod(ia1,2) = 0 then 'PA' else 
		   case when mod(ia1,2) > 0 then 'IN' end end end c1
		 , case when olap_sys.w_common_pkg.is_prime_number(ia3) = 1 then 'PR' else 
		   case when mod(ia3,2) = 0 then 'PA' else 
		   case when mod(ia3,2) > 0 then 'IN' end end end c3
		 , case when olap_sys.w_common_pkg.is_prime_number(ia4) = 1 then 'PR' else 
		   case when mod(ia4,2) = 0 then 'PA' else 
		   case when mod(ia4,2) > 0 then 'IN' end end end c4       
		 , case when olap_sys.w_common_pkg.is_prime_number(ia6) = 1 then 'PR' else 
		   case when mod(ia6,2) = 0 then 'PA' else 
		   case when mod(ia6,2) > 0 then 'IN' end end end c6 
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
    raise; 
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
    raise; 
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
    raise; 
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
    rollback;
	--!habilitando el trigger de la tabla
	EXECUTE IMMEDIATE 'ALTER TRIGGER OLAP_SYS.AIU_W_COMBINATION_RESPONSES_FS ENABLEN';
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
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
    raise; 
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
    raise; 
end filtrar_jugadas_handler;	

--!insertar las predicciones en la nueva tabla predicciones
procedure ins_predicciones_all(pv_nombre				varchar2
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
							 , pf_pres6					float) is
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
										   pres6)
	values ((select nvl(max(prediccion_id),0)+1 from olap_sys.predicciones_all)
	      , pv_nombre
	      , pn_sorteo
	      , pv_tipo
		  , pn_sig_sorteo1
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred1,'1','R','2','G','3','B') else pv_pred1 end
	      , pf_pres1
		  , pn_sig_sorteo2
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred2,'1','R','2','G','3','B') else pv_pred2 end
	      , pf_pres2
		  , pn_sig_sorteo3
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred3,'1','R','2','G','3','B') else pv_pred3 end
	      , pf_pres3
		  , pn_sig_sorteo4
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred4,'1','R','2','G','3','B') else pv_pred4 end
	      , pf_pres4
		  , pn_sig_sorteo5
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred5,'1','R','2','G','3','B') else pv_pred5 end
	      , pf_pres5
		  , pn_sig_sorteo6
	      , case when instr(pv_tipo,'LT') > 0 or instr(pv_tipo,'FR') > 0 then decode(pv_pred6,'1','R','2','G','3','B') else pv_pred6 end
	      , pf_pres6);
	commit;		  
exception
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end ins_predicciones_all;

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
								 , pf_pres6					float) is
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
					   , pf_pres6  => pf_pres6);			   
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end predicciones_all_handler;								 

end w_gl_automaticas_pkg;
/
show errors;