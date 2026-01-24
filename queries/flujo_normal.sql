--C:\temp\sarnMR.xlsx
--!Tab Name: MELATE RETRO
select *
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 10
 order by gambling_id;


##### Ingesta de datos en tabla olap_sys.gl_lt_counts
--!Tab Name: COMPARATIVO LT
 SET SERVEROUTPUT ON
SET ECHO OFF
SET VERIFY OFF
--SET FEEDBACK OFF
DECLARE
    ln$drawing_id       number := 0;
    --!Y Insertara un nuevo registro
    --!N Actualizara la info del sorteo actual
    --!P Imprimira info del sorteo actual
    ln$insert_pattern   varchar2(1) := 'Y';
    ln$_err_code        number := -1;
BEGIN
    select max(drawing_id) into ln$drawing_id from olap_sys.s_calculo_stats;
--    dbms_output.put_line('max ln$drawing_id: '||ln$drawing_id);
    
    --!proceso para generar conteos de lt types para los dos ultimos sorteos
    olap_sys.w_NEW_pick_panorama_pkg.generar_lt_counts_handler (pn_drawing_id     => ln$drawing_id
                                                          , pv_resultado_type => 'CURR'
                                                          , pv_insert_allowed_flag => 'Y'
                                                          , x_err_code        => ln$_err_code);     
END;
/

--!regresa los conteos de LT agrupados por b_type
--!Tab Name: COMPARATIVO LT (actual)
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
select distinct gl_type
     , drawing_id_ini id_ini
     , b_type_ini
     , decode(gl_color_ini,1,'R',2,'G',3,'B') lt_ini
     , gl_cnt_ini
     , '.' predicted_manual
     , '.' predicted_flag
     , '.' winner_flag_ini
     , drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') gl_color_end
     , gl_cnt_end
     --, gl_cnt_end_rank     
     , gl_output
     , case when gl_cnt_end <= 1 and gl_output = '<' then 'war1' 
            when gl_cnt_end <= 1 and gl_output != '<' then 'war2' 
            when gl_cnt_end <= 2 and gl_output = '<' then 'war3' 
            when gl_cnt_end <= 2 and gl_output != '<' then 'war4' else '.' end flag     
     , '.' predicted_manual_end
     , nvl(predicted_flag_end,'.') predicted_flag_end
     , nvl(winner_flag_end,'.') winner_flag_end
     , gl_color_end borrar
  from OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
 where gl_type = 'LT'
--   and drawing_id_ini = (select max_id from resultado_tbl)
   and drawing_id_end = (select max_id from resultado_tbl)
--   and winner_flag_end is not null
 order by 10, 18  
;

--!regresa los conteos de LT agrupados por b_type del sorteo anterior
--!Tab Name: COMPARATIVO LT (previo)
with resultado_tbl as (
select max(gambling_id)-1 prev_id from olap_sys.sl_gamblings
) 
select distinct gl_type
     , drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') gl_color_end
     , gl_cnt_end
     , nvl(predicted_manual_end,'.') predicted_manual_end
     , nvl(predicted_flag_end,'.') predicted_flag_end
     , nvl(winner_flag_end,'.') winner_flag_end
     , gl_color_end borrar
  from OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
 where gl_type = 'LT'
--   and drawing_id_ini = (select max_id from resultado_tbl)
   and drawing_id_end = (select prev_id from resultado_tbl)
--   and winner_flag_end is not null
 order by 3,9 
;

--!regresa los conteos de LT agrupados por b_type basados en numeros impares, pares y primos
--!Tab Name: COMPARATIVO LT DTL
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id id, b_type, digit, decode(color_ubicacion,1,'R',2,'G',3,'B') cfr, ubicacion vfr, decode(color_ley_tercio,1,'R',2,'G',3,'B') clt, ciclo_aparicion ca, pronos_ciclo pxc
     , preferencia_flag pre, case when chng_posicion is null then '.' else 'C' end chg, rango_ley_tercio rlt, preferencia_num pre_num
     , case when prime_number_flag = 1 then 'PRIMO' else
       case when prime_number_flag = 0 and inpar_number_flag = 1 then 'IMPAR' else
       case when prime_number_flag = 0 and inpar_number_flag = 0 then 'PAR' else 'ERROR'
       end end end tipo_num
     , case when winner_flag is null then '.' else winner_flag end resultado  
     , avg_ciclo_aparicion avg_ca
     , case when ciclo_aparicion < avg_ciclo_aparicion then 1 else 0 end b_ca_flag
     , case when ciclo_aparicion > avg_ciclo_aparicion then 1 else 0 end a_ca_flag
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl)
) 
, comparativo_lt_tbl as (
select distinct gl_type
     , drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') gl_color_end
     , gl_cnt_end
     --, gl_cnt_end_rank     
     , gl_output
     , case when gl_cnt_end <= 1 and gl_output = '<' then 'war1' 
            when gl_cnt_end <= 1 and gl_output != '<' then 'war2' 
            when gl_cnt_end <= 2 and gl_output = '<' then 'war3' 
            when gl_cnt_end <= 2 and gl_output != '<' then 'war4' else '.' end flag     
     , '.' predicted_manual_end
     , nvl(predicted_flag_end,'.') predicted_flag_end
     , nvl(winner_flag_end,'.') winner_flag_end
     , gl_color_end borrar
  from OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
 where gl_type = 'LT'
   and drawing_id_end = (select max_id from resultado_tbl)
)
, gl_count_tbl as (
select 'LT' o_label
     , g.id o_id
     , g.b_type o_b_type
     , g.clt o_clt
     , g.tipo_num o_tipo_num
     , g.chg o_chg
     , count(1) j_cnt
     , '.' predicted 
     , '.' prediccion
     , '.' resultado
     , sum(b_ca_flag) b_ca_flag
     , sum(a_ca_flag) a_ca_flag
  from gl_tbl g
 group by g.id
     , g.b_type
     , g.clt
     , g.tipo_num
     , g.chg
) --select * from gl_count_tbl;
, output_tbl as (
select ct.o_label
     , ct.o_id
     , ct.o_b_type
     , ct.o_clt
     , ct.o_tipo_num
     , ct.o_chg
     , ct.j_cnt
     , ct.predicted
     , cl.predicted_flag_end prediccion
     , nvl((select resultado from gl_tbl gt where gt.id=ct.o_id and gt.b_type=ct.o_b_type and gt.clt=ct.o_clt and gt.tipo_num=ct.o_tipo_num and gt.chg=ct.o_chg and gt.resultado='Y'),'.') resultado
     , ct.b_ca_flag
     , ct.a_ca_flag
  from gl_count_tbl ct
     , comparativo_lt_tbl cl
 where ct.o_id = cl.id_end
   and ct.o_b_type = cl.b_type_end
   and ct.o_clt = cl.gl_color_end
)
select *
  from output_tbl
 order by o_b_type, o_clt desc, o_tipo_num, j_cnt desc;


--!Tab Name: COMPARATIVO_LT_CNT
with resultado_tbl as (
select max(gambling_id)-1 prev_id from olap_sys.sl_gamblings
) 
, output_tbl as (
select distinct gl_type
     , drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') gl_color_end
     , gl_cnt_end
--     , nvl(predicted_manual_end,'.') predicted_manual_end
     , case when length(predicted_flag_end) = 1 then 'P1' else nvl(predicted_flag_end,'.') end predicted_flag_end
     , nvl(winner_flag_end,'.') winner_flag_end
     , gl_color_end borrar
  from OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
 where gl_type = 'LT'
--   and drawing_id_ini = (select max_id from resultado_tbl)
--   and drawing_id_end = (select prev_id from resultado_tbl)
   and winner_flag_end is not null
)
, group_tbl as (
select b_type_end
     , predicted_flag_end
     , winner_flag_end
     , count(1) r_cnt
  from output_tbl
-- where b_type_end = 'B6' 
 group by b_type_end
     , predicted_flag_end
     , winner_flag_end
)
select (select min(id_end) from output_tbl) id_min 
     , (select max(id_end) from output_tbl) id_max
     , (select max(id_end) from output_tbl) - (select min(id_end) from output_tbl) rango
     , b_type_end
     , predicted_flag_end
     , winner_flag_end
     , r_cnt
  from group_tbl
order by b_type_end, r_cnt desc    
;  


--!mostrar las predicciones para cada b_type para el ultimo sorteo
--!solo debe ejecutarse en caso de que el query anterior no muestre las predicciones
set serveroutput on
declare
    ln$drawing_id       number := 0;
begin
    select max(drawing_id) into ln$drawing_id from olap_sys.s_calculo_stats;

    olap_sys.w_gl_automaticas_pkg.comparativo_lt_handler;
    olap_sys.w_gl_automaticas_pkg.upd_lt_handler(pn_drawing_id => ln$drawing_id);
    commit;
end;
/

--!en base a las predicciones de las terminaciones se actualizan las jugadas en gl_automaticas_detail
--!es necesario que la ejecucion de las predicciones en python esten generadas antes de ejecutar el bloque siguiente
--!Ejecucion manual de la actualizacion de la tabla gl_automaticas_detail
set serveroutput off
declare
    ln$drawing_id  number := 0;
begin
    select max(drawing_id) into ln$drawing_id from olap_sys.s_calculo_stats;
    --olap_sys.w_gl_automaticas_pkg.upd_terminacion_cnt_handler(pn_drawing_id	=> ln$drawing_id);
    --olap_sys.w_gl_automaticas_pkg.upd_gl_automaticas_handler(pn_drawing_id => ln$drawing_id
    --                                              , pv_ca_comb_flag => 'Y');
    -- 0: execute all, 1: execute contar_repetidos, 2: execute contar_aciertos
    --olap_sys.w_gl_automaticas_pkg.aciertos_repetidos_handler(pn_drawing_id => ln$drawing_id, pv_execution_type=>1);  
    --olap_sys.w_gl_automaticas_pkg.upd_avg_gl_ca(pn_drawing_id => ln$drawing_id);  
    olap_sys.w_gl_automaticas_pkg.get_patron_handler;
    --olap_sys.w_new_pick_panorama_pkg.patron_primo_impar_par_handler;
    --commit;                                                  
end;
/


--<<<conteo de posiciones>>>
--!Tab Name: CONTEO
--Nota: el proceso que hace estos conteos debe estar basado en la vista gl_automaticas_detail_v. reevisar este proceso
select *      
  from olap_sys.gl_position_counts_v
 order by seq 
;


--<<<conteo de posiciones_log>>>
--!Tab Name: CONTEO_STATS
--!informacion sumarizada
select *
  from olap_sys.gl_position_counts_log_v
;

--<<<conteo de posiciones_log>>>
--!Tab Name: CONTEO_STATS
--!informacion detallada
with sorteo_list_tbl as (
select distinct sorteo sorteo_list
  from olap_sys.gl_position_counts_log
)
, sorteo_actual_tbl as (
select sorteo_list curr_sorteo
     , nvl((select curr.pos1 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos1 > 0 and curr.dif1=0),0) pos1
     , nvl((select curr.pos2 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos2 > 0 and curr.dif2=0),0) pos2
     , nvl((select curr.pos3 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos3 > 0 and curr.dif3=0),0) pos3
     , nvl((select curr.pos4 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos4 > 0 and curr.dif4=0),0) pos4
     , nvl((select curr.pos5 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos5 > 0 and curr.dif5=0),0) pos5
     , nvl((select curr.pos6 from olap_sys.gl_position_counts_log curr where curr.sorteo = sorteo_list and curr.pos6 > 0 and curr.dif6=0),0) pos6
  from sorteo_list_tbl
) --select * from sorteo_actual_tbl order by curr_sorteo;
, sorteo_previo_tbl as (
select sorteo_list curr_sorteo
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif1= 0),0) P1
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif2= 0),0) P2
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif3= 0),0) P3
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif4= 0),0) P4
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif5= 0),0) P5
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif6= 0),0) P6
     , nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif1= 0),0)
     + nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif2= 0),0)
     + nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif3= 0),0)
     + nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif4= 0),0)
     + nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif5= 0),0)
     + nvl((select 1 from olap_sys.gl_position_counts_log curr2 where curr2.sorteo=sorteo_list and curr2.dif6= 0),0) SUM_P
     --!pos1
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq1
     , nvl((select prev.pos1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos1 
     , nvl((select prev.dif1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif1
     , nvl((select prev.g_prob1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob1
     , nvl((select prev.lt1||'+'||prev.g_flag1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag1
     , nvl((select prev.type_prob1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob1
     , nvl((select prev.lt1||'+'||prev.type_flag1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag1
     --!pos2
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq2
     , nvl((select prev.pos2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos2
     , nvl((select prev.pxc2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pxc2
     , nvl((select prev.dif2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif2
     , nvl((select prev.g_prob2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob2
     , nvl((select prev.lt2||'+'||prev.g_flag2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag2
     , nvl((select prev.type_prob2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob2
     , nvl((select prev.lt2||'+'||prev.type_flag2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag2
     --!pos3
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq3
     , nvl((select prev.pos3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos3
     , nvl((select prev.pxc3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pxc3
     , nvl((select prev.dif3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif3
     , nvl((select prev.g_prob3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob3
     , nvl((select prev.lt3||'+'||prev.g_flag3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag3
     , nvl((select prev.type_prob3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob3
     , nvl((select prev.lt3||'+'||prev.type_flag3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag3
     --!pos4
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq4
     , nvl((select prev.pos4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos4
     , nvl((select prev.pxc4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pxc4
     , nvl((select prev.dif4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif4
     , nvl((select prev.g_prob4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob4
     , nvl((select prev.lt4||'+'||prev.g_flag4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag4
     , nvl((select prev.type_prob4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob4
     , nvl((select prev.lt4||'+'||prev.type_flag4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag4
     --!pos5
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq5
     , nvl((select prev.pos5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos5
     , nvl((select prev.pxc5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pxc5
     , nvl((select prev.dif5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif5
     , nvl((select prev.g_prob5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob5
     , nvl((select prev.lt5||'+'||prev.g_flag5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag5
     , nvl((select prev.type_prob5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob5
     , nvl((select prev.lt5||'+'||prev.type_flag5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag5
     --!pos6
     , nvl((select prev.seq from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) seq6
     , nvl((select prev.pos6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pos6
     , nvl((select prev.pxc6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) pxc6
     , nvl((select prev.dif6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) dif6
     , nvl((select prev.g_prob6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) g_prob6
     , nvl((select prev.lt6||'+'||prev.g_flag6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') g_flag6
     , nvl((select prev.type_prob6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),0) type_prob6
     , nvl((select prev.lt6||'+'||prev.type_flag6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') type_flag6
     --!avg_ca
     , nvl((select prev.avg_ca_flag1 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos1 > 0 and prev.pos1=(select pos1 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag1
     , nvl((select prev.avg_ca_flag2 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos2 > 0 and prev.pos2=(select pos2 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag2
     , nvl((select prev.avg_ca_flag3 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos3 > 0 and prev.pos3=(select pos3 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag3
     , nvl((select prev.avg_ca_flag4 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos4 > 0 and prev.pos4=(select pos4 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag4
     , nvl((select prev.avg_ca_flag5 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos5 > 0 and prev.pos5=(select pos5 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag5
     , nvl((select prev.avg_ca_flag6 from olap_sys.gl_position_counts_log prev where prev.sorteo = sorteo_list -1 and prev.pos6 > 0 and prev.pos6=(select pos6 from sorteo_actual_tbl where curr_sorteo=sorteo_list)),'#') avg_ca_flag6     
  from sorteo_list_tbl
)
select curr_sorteo
     , p1, p2, p3, p4, p5, p6, sum_p
     , seq1, case when seq1 = 0 then 0 when seq1 between 1 and 3 then 1 when seq1 between 4 and 6 then 2 when seq1 > 6 then 3 end seq1_rank, pos1, dif1, g_prob1, g_flag1, type_prob1, type_flag1
     , seq2, case when seq2 = 0 then 0 when seq2 between 1 and 6 then 1 when seq2 between 7 and 12 then 2 when seq2 > 12 then 3 end seq2_rank, pos2, dif2, g_prob2, g_flag2, type_prob2, type_flag2
     , seq3, case when seq3 = 0 then 0 when seq3 between 1 and 6 then 1 when seq3 between 7 and 12 then 2 when seq3 > 12 then 3 end seq3_rank, pos3, dif3, g_prob3, g_flag3, type_prob3, type_flag3
     , seq4, case when seq4 = 0 then 0 when seq4 between 1 and 6 then 1 when seq4 between 7 and 12 then 2 when seq4 > 12 then 3 end seq4_rank, pos4, dif4, g_prob4, g_flag4, type_prob4, type_flag4
     , seq5, case when seq5 = 0 then 0 when seq5 between 1 and 6 then 1 when seq5 between 7 and 12 then 2 when seq5 > 12 then 3 end seq5_rank, pos5, dif5, g_prob5, g_flag5, type_prob5, type_flag5
     , seq6, case when seq6 = 0 then 0 when seq6 between 1 and 3 then 1 when seq6 between 4 and 6 then 2 when seq6 > 6 then 3 end seq6_rank, pos6, dif6, g_prob6, g_flag6, type_prob6, type_flag6
     , avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6
  from sorteo_previo_tbl
-- where (g_flag5='N' and g_flag2='N') or (g_flag5='N' and g_flag6='Y') 
 order by curr_sorteo
;

##### Dado un dígito dado y su posición, imprimir que otro dígito se ha aparecido mas veces

--!C:\temp\sarnNextDigit.xlsx
--!Tab Name: NXT_DIGIT
select * 
  from olap_sys.next_digit_v
; 


--!recupera las predicciones para el sorteo a jugar != '15.GL'
--!Tab Name: PREDICCIONES
with siguiente_sorteo_tbl as (
select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
)
, output_tbl as (
select distinct prediccion_nombre, 0 muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6, match7, match8, match9, match0,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num
  FROM olap_sys.predicciones_all
 WHERE prediccion_tipo != '15.GL'
   and prediccion_sorteo = (select max_id from siguiente_sorteo_tbl) - nvl(:pn_historico,0)
/*union
select prediccion_nombre, 0 muestra, prediccion_sorteo, replace(prediccion_tipo,'_2',null) prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6,
       res1, res2, res3, res4, res5, res6, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num
  from olap_sys.predicciones_all
 where prediccion_sorteo = (select max_id from siguiente_sorteo_tbl)-1 */
 order by prediccion_sorteo desc, prediccion_tipo_num, prediccion_nombre
)
select prediccion_nombre, muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0,
       match_cnt, match_pct, match1, match2, match3, match4, match5, match6, match7, match8, match9, match0
  from output_tbl
;

--!setear el rango a los digitos de las predicciones
clear screen
set serveroutput on
declare
    cursor c_reultados is
    select max(drawing_id) max_id 
      from olap_sys.s_calculo_stats;

    cursor c_predicciones (pn_sorteo    number) is
    with predicciones_tbl as (    
    select *
      from (
    select prediccion_nombre, siguiente_sorteo1 sig_sorteo, to_number(pred1) pred1, to_number(pres1) pres1, to_number(res1) res1, to_number(match1) match1, b_type
         , max(to_number(pres1)) over (partition by b_type, pred1) max_pres1
    from olap_sys.predicciones_all
     where instr(prediccion_tipo,'15.') > 0 
       and prediccion_sorteo = pn_sorteo
    ) where pres1 = max_pres1 
    ) 
    , percentile_predicciones_tbl as (
    select distinct sig_sorteo
         , b_type
         , percentile_cont(0.4) within group (order by pres1) over (partition by sig_sorteo, b_type) as low_perc_pres1
         , percentile_cont(0.9) within group (order by pres1) over (partition by sig_sorteo, b_type) as high_perc_pres1
      from predicciones_tbl
    ) 
    select distinct pp.b_type, pp.low_perc_pres1, pp.high_perc_pres1
      from percentile_predicciones_tbl pp;

    cursor c_next_digit (pn_sorteo    number) is
    with posicion_tbl as (
    select hh.drawing_id, hh.b_type, hh.digit digit_hdr
         , hd.id, hd.history_digit, hd.match_cnt, hd.drawing_cnt, hd.drawing_list, hd.next_drawing_id, hd.winner_flag
      from olap_sys.position_digit_history_header hh
         , olap_sys.position_digit_history_dtl hd
     where hh.drawing_id = pn_sorteo
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
     where drawing_id >=  pn_sorteo - 100
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
         , nvl(c.preferencia_num,-1) pre_num 
         , case when id between (select min_id from rango_resultados_tbl where rango_b_type=p.b_type) and (select avg_id from rango_resultados_tbl where rango_b_type=p.b_type) then 'Y' else 'N' end jugar_flag
         , p.drawing_list
      from posicion_tbl p
         , olap_sys.s_calculo_stats c
     where c.drawing_id(+) = p.drawing_id
       and c.b_type(+) = p.b_type
       and c.digit(+) = p.history_digit
    )
    select *
      from output_tbl
     where 1=1
       and lt != '#'
       and chng = '.'
       and jugar_flag = 'Y';      
begin
    for r in c_reultados loop
        update olap_sys.predicciones_all
           set pred2 = 'N'
         where prediccion_sorteo = r.max_id
           and instr(prediccion_tipo,'15.') > 0;

        for p in c_predicciones (pn_sorteo => r.max_id) loop
            dbms_output.put_line(r.max_id||'-'||p.b_type||'-'||p.low_perc_pres1||'-'||p.high_perc_pres1);
            update olap_sys.predicciones_all
               set pred2 = 'Z'
             where prediccion_sorteo = r.max_id
               and instr(prediccion_tipo,'15.') > 0
               and pred2 = 'N'
               and b_type = p.b_type
               and pres1 between p.low_perc_pres1 and p.high_perc_pres1;
        end loop;

        for n in c_next_digit (pn_sorteo => r.max_id) loop
            dbms_output.put_line('test :'||r.max_id||'-'||n.b_type||'-'||n.history_digit);
            if n.b_type in ('B1','B2','B3','B4','B5','B6') then
                dbms_output.put_line('test2');
                update olap_sys.predicciones_all
                   set pred2 = 'Y'
                 where prediccion_sorteo = r.max_id
                   and instr(prediccion_tipo,'15.') > 0
                   and pred2 = 'Z'
                   and b_type = n.b_type
                   and pred1 = to_char(n.history_digit);       
            end if;   
        end loop;
    end loop;
    commit;
end;
/

--!Tab Name: PREDICCIONES_DIGITO
--!ordenamiento de predicciones por probabilidad
with siguiente_sorteo_tbl as (
select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
)
select distinct prediccion_sorteo, siguiente_sorteo1 sig_sorteo, b_type, pred1, olap_sys.w_common_pkg.get_digit_type(pred1) primo_par_impar, match1 match, pred2
  from olap_sys.predicciones_all
 where instr(prediccion_tipo,'15.') > 0
   and prediccion_sorteo = (select max_id from siguiente_sorteo_tbl) - nvl(:pn_historico,0)
   and pred2 in ('Z','Y')
 order by b_type, to_number(pred1), pred2  
;







--!proceso que identifica los numeros mas repetidos para las posiciones p2 a p5
--!no es necesario ejecutar este proceso en cada sorteo
begin
    olap_sys.w_new_pick_panorama_pkg.patron_primo_impar_par_handler;
end;
/

/*
Ejecucion manual de la actualizacion de la tabla gl_automaticas_detail
declare
    ln$drawing_id  number := 0;
begin
    select max(drawing_id) into ln$drawing_id from olap_sys.s_calculo_stats;
    w_gl_automaticas_pkg.upd_gl_automaticas_handler(pn_drawing_id => ln$drawing_id
                                                  , pv_ca_comb_flag => 'Y');
    commit;                                                  
end;
/

Revisar la salida del proceso

select *--LIST_ID, ID, IA1, IA2, IA3, IA4, IA5, IA6, PN1, PN2, PN3, PN4, PN5, PN6, NONE1, NONE2, NONE3, NONE4, NONE5, NONE6, PAR1, PAR2, PAR3, PAR4, PAR5, PAR6
  from olap_sys.gl_automaticas_detail
 where list_id = 1
 order by id 
; 
*/


     
--<<<QUERIES PARA SELECCIONAR JUGADAS>>>

set pagesize 50
clear screen

--!IMPAR
prompt IMPAR
with resultados_tbl as (
select gambling_id
     , none_cnt
     , nvl(lead(none_cnt) over (order by gambling_id),-1) next_none_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select none_cnt
     , next_none_cnt
     , count(1) r_cnt
  from resultados_tbl
 where none_cnt = (select none_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by none_cnt
     , next_none_cnt
 order by r_cnt desc, next_none_cnt desc
; 



--PAR
prompt PAR
with resultados_tbl as (
select gambling_id
     , par_cnt
     , nvl(lead(par_cnt) over (order by gambling_id),-1) next_par_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select par_cnt
     , next_par_cnt
     , count(1) r_cnt
  from resultados_tbl
 where par_cnt = (select par_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by par_cnt
     , next_par_cnt
 order by r_cnt desc, next_par_cnt desc
; 

--M3
prompt M3
with resultados_tbl as (
select gambling_id
     , m3_cnt
     , nvl(lead(m3_cnt) over (order by gambling_id),-1) next_m3_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select m3_cnt
     , next_m3_cnt
     , count(1) r_cnt
  from resultados_tbl
 where m3_cnt = (select m3_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by m3_cnt
     , next_m3_cnt
 order by r_cnt desc, next_m3_cnt desc
; 

--M4
prompt M4
with resultados_tbl as (
select gambling_id
     , m4_cnt
     , nvl(lead(m4_cnt) over (order by gambling_id),-1) next_m4_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select m4_cnt
     , next_m4_cnt
     , count(1) r_cnt
  from resultados_tbl
 where m4_cnt = (select m4_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by m4_cnt
     , next_m4_cnt
 order by r_cnt desc, next_m4_cnt desc
; 

--M5
prompt M5
with resultados_tbl as (
select gambling_id
     , m5_cnt
     , nvl(lead(m5_cnt) over (order by gambling_id),-1) next_m5_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select m5_cnt
     , next_m5_cnt
     , count(1) r_cnt
  from resultados_tbl
 where m5_cnt = (select m5_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by m5_cnt
     , next_m5_cnt
 order by r_cnt desc, next_m5_cnt desc
; 

--M7
prompt M7
with resultados_tbl as (
select gambling_id
     , m7_cnt
     , nvl(lead(m7_cnt) over (order by gambling_id),-1) next_m7_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select m7_cnt
     , next_m7_cnt
     , count(1) r_cnt
  from resultados_tbl
 where m7_cnt = (select m7_cnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by m7_cnt
     , next_m7_cnt
 order by r_cnt desc, next_m7_cnt desc
; 

--FAVORABLES
prompt FAVORABLES
with resultados_tbl as (
select gambling_id
     , precnt
     , nvl(lead(precnt) over (order by gambling_id),-1) next_precnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
)
select precnt
     , next_precnt
     , count(1) r_cnt
  from resultados_tbl
 where precnt = (select precnt from olap_sys.pm_mr_resultados_v2 where gambling_id = (select max(gambling_id) from olap_sys.sl_gamblings))
 group by precnt
     , next_precnt
 order by r_cnt desc, next_precnt desc
; 


--!PATRON N+R
with resultados_tbl as (
select gambling_id id
     , comb3
     , comb4
     , comb5
     , comb6
     , top_cnt
     , top_history
  from olap_sys.pm_mr_resultados_v2
 where 1=1
   and gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 100
 order by gambling_id
) --select top_cnt, count(1) r_cnt from resultados_tbl group by top_cnt order by r_cnt desc;
, c3_tbl as (
select 'B3' b_type
     , top_cnt
     , comb3 comb
     , count(1) r_cnt
  from resultados_tbl
 where comb3 in (11,13,24,27,28,29,38)
   and top_cnt in (1,2,3)
 group by top_cnt
     , comb3
)
, c4_tbl as (
select 'B4' b_type
     , top_cnt
     , comb4 comb
     , count(1) r_cnt
  from resultados_tbl
 where comb4 in (11,13,24,27,28,29,38)
   and top_cnt in (1,2,3)
 group by top_cnt
     , comb4
)
, c5_tbl as (
select 'B5' b_type
     , top_cnt
     , comb5 comb
     , count(1) r_cnt
  from resultados_tbl
 where comb5 in (11,13,24,27,28,29,38)
   and top_cnt in (1,2,3)
 group by top_cnt
     , comb5
)
, c6_tbl as (
select 'B6' b_type
     , top_cnt
     , comb6 comb
     , count(1) r_cnt
  from resultados_tbl
 where comb6 in (11,13,24,27,28,29,38)
   and top_cnt in (1,2,3)
 group by top_cnt
     , comb6
)
, union_tbl as ( 
select b_type, top_cnt, comb, r_cnt from c3_tbl
union
select b_type, top_cnt, comb, r_cnt from c4_tbl
union
select b_type, top_cnt, comb, r_cnt from c5_tbl
union
select b_type, top_cnt, comb, r_cnt from c6_tbl
)
select (select min(id) from resultados_tbl) min_id
     , (select max(id) from resultados_tbl) max_id
     , (select max(id) from resultados_tbl) - (select min(id) from resultados_tbl) + 1 rango
     , b_type
     , top_cnt
     , comb
     , r_cnt
  from union_tbl     
 order by b_type, top_cnt, r_cnt desc, comb
;  

--!C3
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
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- nvl(:pn_rango_historico,100)
   and top_cnt in (1,2,3)
   and comb3 in (11,13,24,27,28,29,38)
) --select * from resultados_tbl;
, resultados_union_tbl as (
select comb3, comb2 comb from resultados_tbl
union all
select comb3, comb4 from resultados_tbl
union all
select comb3, comb5 from resultados_tbl
) --select * from resultados_union_tbl order by comb;
, resultados_group_tbl as (
select '1' pos
     , comb3
     , comb1 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb1, comb3 
union all
select '3' pos
     , comb3 
     , 0 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb3 
union all
select '2-5' pos
     , comb3
     , comb
     , count(1) r_cnt
  from resultados_union_tbl
 group by comb, comb3
union all
select '6' pos
     , comb3
     , comb6
     , count(1) r_cnt
  from resultados_tbl
 group by comb6, comb3 
) --select * from resultados_group_tbl where comb3 = 13 and comb>0 order by pos, r_cnt desc;
select 
       (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_from
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_to
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100))
     - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) + 1 rango
     , comb3 pos_3
     , pos pos_n
     , comb
     , r_cnt
  from resultados_group_tbl   
 where (((pos = '2-5' and r_cnt > 1) or (pos = '1')) or pos = '6')
   and comb > 0
 order by comb3, pos, r_cnt desc, comb desc;  
 
 
 --!C4
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
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- nvl(:pn_rango_historico,100)
   and top_cnt in (1,2,3)
   and comb4 in (11,13,24,27,28,29,38)
) --select * from resultados_tbl;
, resultados_union_tbl as (
select comb4, comb2 comb from resultados_tbl
union all
select comb4, comb3 from resultados_tbl
union all
select comb4, comb5 from resultados_tbl
) --select * from resultados_union_tbl order by comb;
, resultados_group_tbl as (
select '1' pos
     , comb4
     , comb1 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb1, comb4 
union all
select '4' pos
     , comb4 
     , 0 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb4 
union all
select '2-5' pos
     , comb4
     , comb
     , count(1) r_cnt
  from resultados_union_tbl
 group by comb, comb4
union all
select '6' pos
     , comb4
     , comb6
     , count(1) r_cnt
  from resultados_tbl
 group by comb6, comb4 
) --select * from resultados_group_tbl where comb4 = 11 and comb>0 order by pos, r_cnt desc;
select 
       (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_from
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_to
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100))
     - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) + 1 rango
     , comb4 pos_4
     , pos pos_n
     , comb
     , r_cnt
  from resultados_group_tbl   
 where(((pos = '2-5' and r_cnt > 1) or (pos = '1')) or pos = '6')
   and comb > 0
 order by comb4, pos, r_cnt desc, comb desc;  
 ;
 


 --!C5
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
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- nvl(:pn_rango_historico,100)
   and top_cnt in (1,2,3)
   and comb5 in (11,13,24,27,28,29,38)
) --select * from resultados_tbl;
, resultados_union_tbl as (
select comb5, comb2 comb from resultados_tbl
union all
select comb5, comb3 from resultados_tbl
union all
select comb5, comb4 from resultados_tbl
) --select * from resultados_union_tbl order by comb;
, resultados_group_tbl as (
select '1' pos
     , comb5
     , comb1 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb1, comb5 
union all
select '4' pos
     , comb5 
     , 0 comb
     , count(1) r_cnt
  from resultados_tbl
 group by comb5 
union all
select '2-5' pos
     , comb5
     , comb
     , count(1) r_cnt
  from resultados_union_tbl
 group by comb, comb5
union all
select '6' pos
     , comb5
     , comb6
     , count(1) r_cnt
  from resultados_tbl
 group by comb6, comb5 
) --select * from resultados_group_tbl where comb4 = 11 and comb>0 order by pos, r_cnt desc;
select 
       (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_from
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) sorteo_to
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100))
     - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - nvl(trim(:pn_rango_historico),100)) + 1 rango
     , comb5 pos_5
     , pos pos_n
     , comb
     , r_cnt
  from resultados_group_tbl   
 where(((pos = '2-5' and r_cnt > 1) or (pos = '1')) or pos = '6')
   and comb > 0
 order by comb5, pos, r_cnt desc, comb desc
; 


--!en base a un digito especificado, regresa el conteo de la lista de digistos de la posicion de 2-5
--!y un conteo por separado de la lista de numeros de la posicion 6
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
 where gambling_id > (select max_id from siguiente_sorteo_tbl) - nvl(:pn_rango_historico,0)
   and comb1 = :pn_digito
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
       (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - :pn_rango_historico) sorteo_from
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - :pn_rango_historico) sorteo_to
     , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - :pn_rango_historico)
     - (select min(gambling_id) from olap_sys.pm_mr_resultados_v2 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - :pn_rango_historico) + 1 rango
     , :pn_digito pos_1
     , pos pos_n
     , comb
     , r_cnt
  from resultados_group_tbl   
 where (pos = '2-5' and r_cnt > 1) 	 
    or pos = '6'
--   and comb in (21,22,23,25,27,29)
 order by pos, r_cnt desc, comb desc;  



--<<OPCION A>>
--C:\temp\jugadas_a_jugar.xlsx
--![[ultimo resultado}}
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)  
--![[informacion de gigaloterias]]
, gl_tbl as (
select drawing_id id, b_type, digit, color_ubicacion cfr, ubicacion vfr, color_ley_tercio clt, ciclo_aparicion ca, case when pronos_ciclo is null then 'N' else 'Y' end pxc
     , case when preferencia_flag is null then 'N' else 'Y' end pre, case when CHNG_POSICION is null then '.' else 'X' end chg, rango_ley_tercio rlt, preferencia_num pre_num
  from olap_sys.s_calculo_stats
 where preferencia_flag is not null 
   and drawing_id = (select max_id from resultado_tbl) 
) --select * from gl_tbl;
, e1_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B1'
--   and rlt between 1 and 6
) --select * from e1_b1_tbl;;
, e1_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B2'
--   and rlt between 1 and 9
) --select * from e1_b2_tbl;
, e1_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B3'
--   and rlt between 1 and 10
) --select * from e1_b3_tbl;
, e1_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B4'
--   and rlt between 1 and 10 
) --select * from e1_b4_tbl;
, e1_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B5'
--   and rlt between 1 and 9 
) --select * from e1_b5_tbl;
, e1_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc, pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B6'
--   and rlt between 1 and 6
) --select * from e1_b6_tbl;
--![[informacion de patrones de LT]]
, lt_details_tbl as (
select lt1, lt2, lt3, lt4, lt5, lt6
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = 1583--(select max_id from resultado_tbl)
   and null_cnt in(0,1) 
   and match_cnt = 0 
   --and null_cnt in (0,1) and match_cnt in (0,1)
   and red_cnt < 3
) --select *  from lt_details_tbl; 
--![[resumen agrupado del LT]]
, lt_summary_tbl as (
select distinct drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') lt
     , nvl(predicted_flag_end,'.') predicted_flag_end
  from olap_sys.s_gl_mapas_fre_lt_cnt
 where gl_type = 'LT'
   and drawing_id_end = (select max_id from resultado_tbl)
   and predicted_flag_end is not null
) --select * from lt_summary_tbl order by b_type_end;
, aciertos_tbl as (
select aciertos_accum
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 where sorteo_actual =  (select max_id from resultado_tbl)
 group by aciertos_accum
) --select * from aciertos_tbl order by r_cnt desc; 
, avg_aciertos_tbl as (
select round(avg(r_cnt),2) avg_r_cnt
     , percentile_cont(0.45) within group (order by r_cnt) as low_perc_r_cnt
  from aciertos_tbl
) --select * from avg_aciertos_tbl;
, distinct_aciertos_tbl as (
select a.aciertos_accum
  from aciertos_tbl a
 where 1=1
   --!si el query recupera muchos registros con la condicion basada en el percentile entonces hacer el cambio  la condicion basada en el promedio
--#   and a.r_cnt >= (select low_perc_r_cnt from avg_aciertos_tbl)  
   and a.r_cnt >= (select avg_r_cnt from avg_aciertos_tbl) 
 order by a.aciertos_accum
) --select aciertos_accum from distinct_aciertos_tbl;
--!bayes?_general_flag = 'N'
--!bayes?_b_type_flag = 'Y'
, b1_merge_tbl as (
select 'B1' b_type, pos1 pos, g_flag1 g_flag, type_flag1 t_flag from olap_sys.gl_position_counts_v where pos1 > 0 /*and pref1 = 'X'*/ and sorteo = (select max_id from resultado_tbl) and seq between 2 and 7 and g_prob1 between 0.003 and 0.2155 
--union
--select b_type, digit, '', '' from e1_b1_tbl
) --select * from b1_merge_tbl;
, b2_merge_tbl as (
select 'B2' b_type, pos2 pos, g_flag2 g_flag, type_flag2 t_flag from olap_sys.gl_position_counts_v where pos2 > 0 and sorteo = (select max_id from resultado_tbl) and g_prob2 between 0.0035 and 0.085 --and g_flag2 = 'N'
--union
--select b_type, digit, '', '' from e1_b2_tbl
) --select * from b2_merge_tbl;
, b3_merge_tbl as (
select 'B3' b_type, pos3 pos, g_flag3 g_flag, type_flag3 t_flag from olap_sys.gl_position_counts_v where pos3 > 0 /*and bayes3_b_type_prob > 0 and bayes3_b_type_flag = 'Y'*/ and sorteo = (select max_id from resultado_tbl) and g_prob3 between 0 and 0.157 and seq between 5 and 16
--union
--select b_type, digit, '', '' from e1_b3_tbl
) --select * from b3_merge_tbl;
, b4_merge_tbl as (
select 'B4' b_type, pos4 pos, g_flag4 g_flag, type_flag4 t_flag from olap_sys.gl_position_counts_v where pos4 > 0 /*and bayes4_b_type_prob > 0 and bayes4_b_type_flag = 'Y'*/ and sorteo = (select max_id from resultado_tbl) and g_prob4 between 0 and 0.1415 and seq between 1 and 13
--union
--select b_type, digit, '', '' from e1_b4_tbl
) --select * from b4_merge_tbl;
, b5_merge_tbl as (
select 'B5' b_type, pos5 pos, g_flag5 g_flag, type_flag5 t_flag from olap_sys.gl_position_counts_v where pos5 > 0 and pref5 = 'X' and sorteo = (select max_id from resultado_tbl) --and g_prob5 between 0 and 0.079 --and seq between 1 and 13 
--union
--select b_type, digit, '', '' from e1_b5_tbl
) --select * from b5_merge_tbl;
, b6_merge_tbl as (
select 'B6' b_type, pos6 pos, g_flag6 g_flag, type_flag6 t_flag from olap_sys.gl_position_counts_v where pos6 > 0 and sorteo = (select max_id from resultado_tbl) and g_prob6 between 0 and 0.208 and seq between 1 and 9  
--union
--select b_type, digit from e1_b6_tbl
) --select * from b6_merge_tbl;
, lt_contadores_tbl as (
select red_cnt, green_cnt, blue_cnt
     , count(1) r_cnt
  from olap_sys.pm_mr_resultados_v2
 where 1=1
   and gambling_id > 594
--   and gambling_id > (select max_id from resultado_tbl) - 100
   and null_cnt = 0
 group by red_cnt, green_cnt, blue_cnt
)
, lt_contadores_avg_tbl as ( 
select round(avg(r_cnt),2) avg_cnt
  from lt_contadores_tbl
)
--select * from lt_contadores_tbl where r_cnt > (select avg_cnt from lt_contadores_avg_tbl) order by r_cnt desc;
--![[capa de transformaciones]]
, output_tbl as (
select ia1, ia2, ia3, ia4, ia5, ia6
     --!frecuencia
--     , decode(fr1,-1,'#',1,'R',2,'G',3,'B') fr1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') fr2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') fr3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') fr4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') fr5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') fr6
     --!ley del tercio
     , decode(lt1,-1,'#',1,'R',2,'G',3,'B') lt1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') lt2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') lt3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') lt4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') lt5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') lt6
     --!ciclo de aparicion
     , avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6
     --!decenas
     , d1, d2, d3, d4, d5, d6
     --!numeros sin cambios
     , chg1, chg2, chg3, chg4, chg5, chg6
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg
     --!primos
     , pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt
     --!impares
     , none1, none2, none3, none4, none5, none6, none_cnt
     --!pares
     , par1, par2, par3, par4, par5, par6, par_cnt
     --!multiplo de 3
     , m3_1, m3_2, m3_3, m3_4, m3_5, m3_6, m3_cnt
     --!multiplo de 4
     , m4_1, m4_2, m4_3, m4_4, m4_5, m4_6, m4_cnt
     --!multiplo de 5
     , m5_1, m5_2, m5_3, m5_4, m5_5, m5_6, m5_cnt
     --!multiplo de 7
     , m7_1, m7_2, m7_3, m7_4, m7_5, m7_6, m7_cnt	
     --!numeros favorables
     , pf1, pf2, pf3, pf4, pf5, pf6, pf1 + pf2 + pf3 + pf4 + pf5 + pf6 pf_sum
     , sorteo_actual
     , caso_id
     , repetidos_cnt
     , t1_cnt, t2_cnt
     , patron_6_accum,  patron_25_cnt, patron_25_accum
     , aciertos_accum, digito_cnt, 0 ia6_cnt, ca_sum, comb_sum
     , 'N' jugar_flag
     , patron_25_history
     , digito_25_history 
     , red_cnt
     , green_cnt
     , blue_cnt
     , nvl((select g_flag1 from olap_sys.gl_position_counts_v where pos1=ia1),'#') g1_flag
     , nvl((select g_flag2 from olap_sys.gl_position_counts_v where pos2=ia2),'#') g2_flag
     , nvl((select g_flag3 from olap_sys.gl_position_counts_v where pos3=ia3),'#') g3_flag
     , nvl((select g_flag4 from olap_sys.gl_position_counts_v where pos4=ia4),'#') g4_flag
     , nvl((select g_flag5 from olap_sys.gl_position_counts_v where pos5=ia5),'#') g5_flag
     , nvl((select g_flag6 from olap_sys.gl_position_counts_v where pos6=ia6),'#') g6_flag
 /*    , nvl((select type_flag1 from olap_sys.gl_position_counts_v where pos1=ia1),'#') t1_flag
     , nvl((select type_flag2 from olap_sys.gl_position_counts_v where pos2=ia2),'#') t2_flag
     , nvl((select type_flag3 from olap_sys.gl_position_counts_v where pos3=ia3),'#') t3_flag
     , nvl((select type_flag4 from olap_sys.gl_position_counts_v where pos4=ia4),'#') t4_flag
     , nvl((select type_flag5 from olap_sys.gl_position_counts_v where pos5=ia5),'#') t5_flag
     , nvl((select type_flag6 from olap_sys.gl_position_counts_v where pos6=ia6),'#') t6_flag */
     , nvl((select case when seq = 0 then 0 when seq between 1 and 3 then 1 when seq between 4 and 6 then 2 when seq > 6 then 3 end from olap_sys.gl_position_counts_v where pos1=ia1),0) seq1_rank 
     , nvl((select case when seq = 0 then 0 when seq between 1 and 6 then 1 when seq between 7 and 12 then 2 when seq > 12 then 3 end from olap_sys.gl_position_counts_v where pos2=ia2),0) seq2_rank 
     , nvl((select case when seq = 0 then 0 when seq between 1 and 6 then 1 when seq between 7 and 12 then 2 when seq > 12 then 3 end from olap_sys.gl_position_counts_v where pos3=ia3),0) seq3_rank 
     , nvl((select case when seq = 0 then 0 when seq between 1 and 6 then 1 when seq between 7 and 12 then 2 when seq > 12 then 3 end from olap_sys.gl_position_counts_v where pos4=ia4),0) seq4_rank 
     , nvl((select case when seq = 0 then 0 when seq between 1 and 6 then 1 when seq between 7 and 12 then 2 when seq > 12 then 3 end from olap_sys.gl_position_counts_v where pos5=ia5),0) seq5_rank 
     , nvl((select case when seq = 0 then 0 when seq between 1 and 3 then 1 when seq between 4 and 6 then 2 when seq > 6 then 3 end from olap_sys.gl_position_counts_v where pos6=ia6),0) seq6_rank 
     , top_digit_cnt top_cnt
     , nvl(top_digit_history,'NA') top_history
  from olap_sys.gl_automaticas_detail
 where 1=1
   --!contadores de LT
--   and (red_cnt,green_cnt,blue_cnt) in (select red_cnt, green_cnt, blue_cnt from lt_contadores_tbl where r_cnt > (select avg_cnt from lt_contadores_avg_tbl))
   --!numero favorable no marcado
--   and pf1 = 1
--   and pf4 = 0
   --!ley del tercio en G y B
--#   and lt2 in (2,3)
   --![[rango de aciertos mas amplio]]
--#   and aciertos_accum in (select aciertos_accum from distinct_aciertos_tbl)
   --!condicion temporal mientras se incrementa el aciertos_accum. una vez que sea lo suficientemente grande el conjunto se comentara esta condicion y se descomentara la condicion de arriba
   and aciertos_accum between 21 and 36  
   and patron_25_cnt > 0
   --![[terminaciones]]
--#   and (t1_cnt, t2_cnt) in ((4,2),(6,0))
   --![[gl_position_counts & next_digit_v & informacion de gigaloterias]]
   and IA1 in (select pos from b1_merge_tbl)
--#   and IA2 in (select pos from b2_merge_tbl)
--#   and IA3 in (select pos from b3_merge_tbl)
--#   and IA4 in (select pos from b4_merge_tbl)
--#   and IA5 in (select pos from b5_merge_tbl)
--#   and IA6 in (select pos from b6_merge_tbl)
   and (IA2 in (select pos from b2_merge_tbl) or IA5 in (select pos from b5_merge_tbl))
   --<<lista de predicciones por decena>>
   and (D1,D2,D3,D4,D5,D6) in (select pred1, pred2, pred3, pred4, pred5, pred6 from olap_sys.predicciones_all where instr(prediccion_tipo,'DECENA') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))        
--   and (D1,D3,D4,D6) in (select distinct pred1, pred3, pred4, pred6 from olap_sys.predicciones_all where instr(prediccion_tipo, 'DECENA') > 0  and prediccion_sorteo = (select max_id from resultado_tbl))     
--#   and patron_25_cnt > 1
--#   and patron_25_accum > 5  
) /*select ia1, ia2, ia3, ia4, ia5, ia6
     , lt1, lt2, lt3, lt4, lt5, lt6
     , red_cnt, green_cnt, blue_cnt
     , d1, d2, d3, d4, d5, d6
     , pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt
     , none1, none2, none3, none4, none5, none6, none_cnt
     , par1, par2, par3, par4, par5, par6, par_cnt
     , m3_1, m3_2, m3_3, m3_4, m3_5, m3_6, m3_cnt
     , m4_1, m4_2, m4_3, m4_4, m4_5, m4_6, m4_cnt
     , m5_1, m5_2, m5_3, m5_4, m5_5, m5_6, m5_cnt
     , m7_1, m7_2, m7_3, m7_4, m7_5, m7_6, m7_cnt
     , pf1, pf2, pf3, pf4, pf5, pf6, pf_sum
     , chg1, chg2, chg3, chg4, chg5, chg6, sum_chg
     , avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6
     , g1_flag, g2_flag, g3_flag, g4_flag, g5_flag, g6_flag
     , seq1_rank, seq2_rank, seq3_rank, seq4_rank, seq5_rank, seq6_rank
     , sorteo_actual, caso_id, repetidos_cnt, t1_cnt, t2_cnt, patron_6_accum, patron_25_cnt, patron_25_accum, aciertos_accum, ca_sum, comb_sum, jugar_flag, patron_25_history 
     , top_cnt, top_history
     from output_tbl 
     where 1=1 
     --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
     --and sum_chg in (0,1,2)
     order by ia1, ia2, ia3, ia4, ia5, ia6;*/
select ia1, ia2, ia3, ia4, ia5, ia6
     , lt1, lt2, lt3, lt4, lt5, lt6
     , red_cnt, green_cnt, blue_cnt
     , d1, d2, d3, d4, d5, d6
     , pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt
     , none1, none2, none3, none4, none5, none6, none_cnt
     , par1, par2, par3, par4, par5, par6, par_cnt
     , m3_1, m3_2, m3_3, m3_4, m3_5, m3_6, m3_cnt
     , m4_1, m4_2, m4_3, m4_4, m4_5, m4_6, m4_cnt
     , m5_1, m5_2, m5_3, m5_4, m5_5, m5_6, m5_cnt
     , m7_1, m7_2, m7_3, m7_4, m7_5, m7_6, m7_cnt
     , pf1, pf2, pf3, pf4, pf5, pf6, pf_sum
     , chg1, chg2, chg3, chg4, chg5, chg6, sum_chg
     , avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6
     , g1_flag, g2_flag, g3_flag, g4_flag, g5_flag, g6_flag
     , seq1_rank, seq2_rank, seq3_rank, seq4_rank, seq5_rank, seq6_rank
     , sorteo_actual, caso_id, repetidos_cnt, t1_cnt, t2_cnt, patron_6_accum, patron_25_cnt, patron_25_accum, aciertos_accum, ca_sum, comb_sum, jugar_flag, patron_25_history 
     , top_cnt, top_history
  from output_tbl
 where 1=1 
   --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
   and sum_chg in (0,1,2)
   --!numeros favorables
   and (pf_sum = 0
     or (pf_sum = 2 and pf1=1 and (pf2=1 or pf6=1))
     or ((pf_sum = 3 and (pf1=1 and pf2=1 and (pf3=1 or pf6=1))) or (pf_sum = 3 and (pf1=1 and pf3=1 and (pf2=1 or pf5=1))) or (pf_sum = 3 and (pf1=1 and pf5=1 and (pf2=1 or pf3=1))))
     or (pf_sum = 4 and pf1=1 and pf2=1 and pf4=0)
     or ((pf_sum = 5 and pf2=1 and pf5=1 and pf6=1 and (pf3=1 or pf4=1)))
     or pf_sum = 6
       ) 
   and (pn_cnt,none_cnt,par_cnt,m3_cnt) in ((2,1,3,1),
											(2,1,3,1),
											(2,1,3,2),
											(2,1,3,3),
											(2,2,2,2),
											(2,2,2,3),
											--
											(2,1,3,1),
											(2,1,3,2),
											(2,2,2,2),
											(2,2,2,3),
											--
											(2,1,3,1),
											(2,1,3,2),
											(2,2,2,2),
											(2,2,2,3))                                                        
   --!solo jugadas que esten el patron de ley del tercio
   --!esta condicion es sustituida por lt_summary de arriba
   --and (lt1, lt2, lt3, lt4, lt5, lt6) in (select lt1, lt2, lt3, lt4, lt5, lt6 from lt_details_tbl) 
   --!COMPARATIVO LT
 /*  and lt1 in ('R','G','B')
   and lt2 in ('G','B')
   and lt3 in ('G','B')
--   and lt4 in ('G','B')
   and lt5 in ('R','G','B')
--   and lt6 in ('R','G','B')*/
   and ia3 in (20,16,12,13,14,17,15,18,24,21,23)
   and ia4 in (24,22,21,28,29,19,30,25,27,33,23,26)
   and ia5 in (27,32,31,29,28,34,36,25,30,24,26)
    order by ia1, ia2, ia3, ia4, ia5, ia6
;



##### Dado un dígito de la posición 1, imprimir de forma descendente que números se repiten mas

clear screen
set serveroutput on
declare
    ln$max_id       number:= 0 ;
begin 
	select max(gambling_id) max_id 
	  into ln$max_id
	  from olap_sys.sl_gamblings;
    olap_sys.w_new_pick_panorama_pkg.listado_numeros_handler(pn_drawing_id => ln$max_id
                                                           --, pn_comb1 => 1
                                                           , pn_historico => ln$max_id); 
end;
/

select SORTEO_ID
     , ATRIBUTO_STR1 HISTORICO
     , ATRIBUTO_STR2 COMB1
     , ATRIBUTO_NUM1 COMB6
     , ATRIBUTO_NUM2 MATCH_CNT
     , ATRIBUTO_NUM3 ULTIMO_SORTEO_ID
     , ATRIBUTO_NUM4 DIF_SORTEO_COMB1_COMB6
  from OLAP_SYS.CONTEO_PATRONES
 where patron_type = 'LISTADO_NUMEROS' 
   and ATRIBUTO_NUM4 > 0
 order by ATRIBUTO_NUM2 desc, ATRIBUTO_NUM1 
;  

--!obtener patrones de numeros de posiciones intermedias
with output_tbl as (
select sorteo_id sorteo
     , atributo_str1 historico
     , atributo_str2 parametro
     , atributo_num1 digito
     , atributo_num2 match_cnt
     , atributo_num3 ult_sorteo
     , atributo_num4 diff_comb6
  from olap_sys.conteo_patrones
 where patron_type = 'LISTADO_NUMEROS' 
   and atributo_num4 > 0
)
, resultados_intermedios_tbl as (
select gambling_id id, comb1, comb2, comb3, comb4, comb5, comb6
  from olap_sys.sl_gamblings
 where gambling_id > (select distinct sorteo from output_tbl) - (select distinct historico from output_tbl)
   and (comb1,comb6) in (select parametro, digito from output_tbl)
) --select * from resultados_intermedios_tbl;
, union_tbl as (
select comb1, comb2 comb, comb6
  from resultados_intermedios_tbl 
union all
select comb1, comb3 comb, comb6
  from resultados_intermedios_tbl 
union all
select comb1, comb4 comb, comb6
  from resultados_intermedios_tbl 
union all
select comb1, comb5 comb, comb6
  from resultados_intermedios_tbl 
) --select * from  union_tbl;
, group_tbl as (
select comb1
     , comb
     , comb6
     , count(1) rcnt
    from union_tbl
   group by comb1
     , comb
     , comb6
) --select * from group_tbl order by comb1, comb6, comb;
select distinct o.sorteo, o.historico, o.parametro, o.digito, o.match_cnt, o.ult_sorteo, o.diff_comb6, g.comb, g.rcnt
  from output_tbl o
     , group_tbl g
 where o.parametro = g.comb1
   and o.digito = g.comb6
 order by o.digito, comb  
; 

--!contador de aciertos
select jugar_flag
     , sorteo_actual
     , aciertos_cnt
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 group by jugar_flag
     , sorteo_actual
     , aciertos_cnt
 order by jugar_flag, r_cnt desc
; 

--<<<QUERIES PARA RECUPERAR RESULTADOS GANADORES>>>

with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, output_tbl as (
select ia1, ia2, ia3, ia4, ia5, ia6
     --!frecuencia
     , decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6
     --!ley del tercio
     , decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6
     --!ciclo de aparicion
     , ca1, ca2, ca3, ca4, ca5, ca6
     --!decenas
     , d1, d2, d3, d4, d5, d6
     ,0 pxc_cnt
     --!numeros favorables
     , pf1, pf2, pf3, pf4, pf5, pf6
     --!contador de primos, impares, pares
     , pn_cnt, none_cnt, par_cnt
     , 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena
     , sorteo_actual, olap_sys.w_common_pkg.get_c1_c5_c6_rank(ia1, ia5, ia6) b1_b5_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos
     --!numeros sin cambios
     , chg1, chg2, chg3, chg4, chg5, chg6
     , aciertos_accum, aciertos_cnt, incidencia_cnt, jugar_flag, ca_sum, comb_sum
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg
     , list_id, t2_cnt, terminacion_cnt
  from olap_sys.gl_automaticas_detail 
 where aciertos_cnt >= 4
) 
select distinct ia1, ia2, ia3, ia4, ia5, ia6, 
                 f1, f2, f3, f4, f5, f6, 
                 t1, t2, t3, t4, t5, t6, 
                 ca1, ca2, ca3, ca4, ca5, ca6, 
                 d1, d2, d3, d4, d5, d6, pxc_cnt,
                 pf1, pf2, pf3, pf4, pf5, pf6, 
                 pn_cnt, none_cnt, par_cnt, consecutivos_cnt, terminacion_str, 
                 decena, sorteo_actual, b1_b5_b6_flag, repetidos_cnt, 
                 c1_c6_flag, mapa_primos, 
                 chg1, chg2, chg3, chg4, chg5, chg6, 
                 aciertos_accum, aciertos_cnt, incidencia_cnt, jugar_flag, ca_sum, comb_sum, terminacion_cnt t_cnt
  from output_tbl
 where 1=1
 order by jugar_flag desc, aciertos_cnt desc, ia1, ia2, ia3, ia4, ia5, ia6; 



--==============================================================================================================================
--<<recupera el conteo de aciertos, preferidos y no preferidos
--<<p_historico: indica el numero de sorteos que se va a usar como historico
with resultado_tbl as (
select max(gambling_id) - :pn_historico max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id
     , b_type
     , digit
     , preferencia_flag pref
     , winner_flag
     , case when olap_sys.w_common_pkg.get_digit_type(digit) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 3 then 'IMPAR' end end end digit_type 
  from olap_sys.s_calculo_stats
 where 1=1
   and drawing_id >= (select max_id from resultado_tbl) 
) --select * from gl_tbl order by drawing_id, b_type, digit_type;
, gl_group_tbl as (
select drawing_id
     , b_type
     , digit_type
     , count(1) j_cnt  
  from gl_tbl
 where pref is not null 
 group by drawing_id
     , b_type
     , digit_type  
) --select * from gl_group_tbl order by drawing_id, b_type, digit_type;
, gl_count_tbl as (
select drawing_id
     , b_type
     , count(1) j_cnt
  from gl_group_tbl   
 group by drawing_id
     , b_type
) --select * from gl_count_tbl order by drawing_id, b_type;
--select j_cnt from gl_count_tbl where drawing_id+1= 1500 and b_type='B1';
, resultados_tbl as (
select gambling_id
     , comb1 r1
     , comb2 r2
     , comb3 r3
     , comb4 r4
     , comb5 r5
     , comb6 r6
     , case when olap_sys.w_common_pkg.get_digit_type(comb1) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb1) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb1) = 3 then 'IMPAR' end end end digit_type_r1
     , case when olap_sys.w_common_pkg.get_digit_type(comb2) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb2) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb2) = 3 then 'IMPAR' end end end digit_type_r2
     , case when olap_sys.w_common_pkg.get_digit_type(comb3) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb3) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb3) = 3 then 'IMPAR' end end end digit_type_r3
     , case when olap_sys.w_common_pkg.get_digit_type(comb4) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb4) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb4) = 3 then 'IMPAR' end end end digit_type_r4
     , case when olap_sys.w_common_pkg.get_digit_type(comb5) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb5) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb5) = 3 then 'IMPAR' end end end digit_type_r5
     , case when olap_sys.w_common_pkg.get_digit_type(comb6) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(comb6) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(comb6) = 3 then 'IMPAR' end end end digit_type_r6
  from olap_sys.pm_mr_resultados_v2
 where gambling_id >= (select max_id from resultado_tbl) 
) --select * from resultados_tbl order by gambling_id;
, merge_tbl as (
select gambling_id, r1, r2, r3, r4, r5, r6, digit_type_r1, digit_type_r2, digit_type_r3, digit_type_r4, digit_type_r5, digit_type_r6
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B1'),-1) cnt_j1
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B1' and digit=r1),0) res_j1 
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B1' and digit=r1),0) val_j1
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B2'),-1) cnt_j2
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B2' and digit=r2),0) res_j2 
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B2' and digit=r2),0) val_j2     
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B3'),-1) cnt_j3
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B3' and digit=r3),0) res_j3 
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B3' and digit=r3),0) val_j3      
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B4'),-1) cnt_j4
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B4' and digit=r4),0) res_j4 
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B4' and digit=r4),0) val_j4     
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B5'),-1) cnt_j5
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B5' and digit=r5),0) res_j5
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B5' and digit=r5),0) val_j5   
     , nvl((select j_cnt from gl_count_tbl where drawing_id= gambling_id-1 and b_type='B6'),-1) cnt_j6
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B6' and digit=r6),0) res_j6
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B6' and digit=r6),0) val_j6 
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B1' and digit=r1),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B2' and digit=r2),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B3' and digit=r3),0) 
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B4' and digit=r4),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B5' and digit=r5),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and pref is not null and b_type='B6' and digit=r6),0) pref_sum
     , nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B1' and digit=r1),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B2' and digit=r2),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B3' and digit=r3),0) 
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B4' and digit=r4),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B5' and digit=r5),0)
     + nvl((select 1 from gl_tbl where drawing_id= gambling_id-1 and winner_flag is not null and b_type='B6' and digit=r6),0) res_sum
  from resultados_tbl r   
) --select * from merge_tbl order by gambling_id;
, summary_tbl as (
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B1' b_type
     , sum(val_j1) pref
     , sum(res_j1) aciertos
  from merge_tbl 
union
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B2' b_type
     , sum(val_j2)
     , sum(res_j2)
  from merge_tbl
union
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B3' b_type
     , sum(val_j3)
     , sum(res_j3)
  from merge_tbl
union
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B4' b_type
     , sum(val_j4)
     , sum(res_j4)
  from merge_tbl
union
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B5' b_type
     , sum(val_j5)
     , sum(res_j5)
  from merge_tbl  
union
select min(gambling_id) min_id
     , max(gambling_id) max_id
     , 'B6' b_type
     , sum(val_j6)
     , sum(res_j6)
  from merge_tbl  
) --select * from summary_tbl;
select min_id
     , max_id
     , (max_id-min_id) range_id
     , b_type
     , aciertos
     , to_char(round((aciertos/((max_id-min_id)+1)),2))*100||'%' aciertos_pct
     , (max_id-min_id) - aciertos no_#
     , to_char((1 - round((aciertos/((max_id-min_id))),2))*100)||'%' no_#_pct
     , pref preferido
     , to_char(round((pref/(max_id-min_id)),2)*100)||'%' preferido_pct     
     , (aciertos-pref) no_preferido
     , to_char(round((((aciertos-pref))/(aciertos)),2)*100)||'%' no_preferido_pct     
  from summary_tbl
 order by b_type 
;  


select drawing_id
     , b_type
     , digit
     --, preferencia_flag pre
     , case when olap_sys.w_common_pkg.get_digit_type(digit) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 3 then 'IMPAR' end end end digit_type 
  from olap_sys.s_calculo_stats
 where preferencia_flag is not null
   and drawing_id = 1487
 order by b_type
     , digit  ; 


select drawing_id
     , b_type
     , digit
     --, preferencia_flag pre
     , case when olap_sys.w_common_pkg.get_digit_type(digit) = 1 then 'PRIMO' else 
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 2 then 'PAR' else
       case when olap_sys.w_common_pkg.get_digit_type(digit) = 3 then 'IMPAR' end end end digit_type 
  from olap_sys.s_calculo_stats
 where preferencia_flag is not null
   and drawing_id = 1487
 order by b_type
     , digit  ; 