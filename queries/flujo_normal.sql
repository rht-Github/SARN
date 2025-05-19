--C:\temp\sarnMR.xlsx
--!Tab Name: MELATE RETRO
select *
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 10
 order by gambling_id;


--!recupera las predicciones para el sorteo a jugar 
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
 WHERE prediccion_sorteo = (select max_id from siguiente_sorteo_tbl)
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



###### --!recupera historico de las predicciones con mas de 2 aciertos
with siguiente_sorteo_tbl as (
select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
)
, output_tbl as (
select prediccion_nombre,  0 muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6, match7, match8, match9, match0,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num 
  from olap_sys.predicciones_all 
 where 1=1
--   and prediccion_sorteo < (select max_id from siguiente_sorteo_tbl) 
--   and prediccion_sorteo > (select max_id from siguiente_sorteo_tbl) - 20
   and match_cnt > 3
   and prediccion_tipo!= '6.CHNG'
union all
select prediccion_nombre,  0 muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6, match7, match8, match9, match0,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num 
  from olap_sys.predicciones_all 
 where 1=1
   and match_cnt > 0
   and (instr(prediccion_tipo,'14.') > 0 or instr(prediccion_tipo,'13.') > 0)
 order by match_cnt desc, prediccion_tipo_num, prediccion_sorteo desc 
)
select prediccion_nombre, muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0,
       match_cnt, match_pct, match1, match2, match3, match4, match5, match6, match7, match8, match9, match0
  from output_tbl
;  


##### Ingesta de datos en tabla olap_sys.gl_lt_counts
--!Tab Name: COMPARATIVO LT
CLEAR SCREEN
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

with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
select distinct gl_type
     , drawing_id_ini id_ini
     , b_type_ini
     , decode(gl_color_ini,1,'R',2,'G',3,'B') lt_ini
     , gl_cnt_ini
     , nvl(predicted_manual_ini,'.') predicted_manual
     , nvl(predicted_flag_ini,'.') predicted_flag
     , nvl(winner_flag_ini,'.') winner_flag_ini
     , drawing_id_end id_end
     , b_type_end
     , decode(gl_color_end,1,'R',2,'G',3,'B') gl_color_end
     , gl_cnt_end
     --, gl_cnt_end_rank     
     , gl_output
     , case when gl_cnt_end <= 1 and gl_output = '<' then 'war1' 
            when gl_cnt_end <= 1 and gl_output != '<' then 'war2' 
            when gl_cnt_end <= 2 and gl_output = '<' then 'war3' 
            when gl_cnt_end <= 2 and gl_output != '<' then 'war4' end flag     
     , '.' predicted_manual_end
     , '.' predicted_flag_end
     , nvl(winner_flag_end,'.') winner_flag_end
     , gl_color_end borrar
  from OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT
 where gl_type = 'LT'
--   and drawing_id_ini = (select max_id from resultado_tbl)
   and drawing_id_end = (select max_id from resultado_tbl)
--   and winner_flag_end is not null
 order by 10, 18  
;

--!nuevo query con el detalle
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id id, b_type, digit, decode(color_ubicacion,1,'R',2,'G',3,'B') cfr, ubicacion vfr, decode(color_ley_tercio,1,'R',2,'G',3,'B') clt, ciclo_aparicion ca, pronos_ciclo pxc
     , preferencia_flag pre, case when chng_posicion is null then '.' else 'X' end chg, rango_ley_tercio rlt, preferencia_num pre_num
     , case when prime_number_flag = 1 then 'PRIMO' else
       case when prime_number_flag = 0 and inpar_number_flag = 1 then 'IMPAR' else
       case when prime_number_flag = 0 and inpar_number_flag = 0 then 'PAR' else 'ERROR'
       end end end tipo_num
     , case when winner_flag is null then '.' else winner_flag end resultado  
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl)
) 
, output_tbl as (
select 'LT' o_label
     , id o_id
     , b_type o_b_type
     , clt o_clt
     , tipo_num o_tipo_num
     , chg o_chg
     , count(1) j_cnt
     , '.' predicted 
     , '.' prediccion
     , '.' resultado
  from gl_tbl
 group by id
     , b_type
     , clt
     , tipo_num
     , chg
)
select o_label
     , o_id
     , o_b_type
     , o_clt
     , o_tipo_num
     , o_chg
     , j_cnt
     , predicted
     , prediccion
     , nvl((select resultado from gl_tbl gt where gt.id=o_id and gt.b_type=o_b_type and gt.clt=o_clt and gt.tipo_num=o_tipo_num and gt.chg=o_chg and gt.resultado='Y'),'.') resultado
  from output_tbl
 order by o_b_type, o_clt desc, o_tipo_num, j_cnt desc;

--!mostrar las predicciones para cada b_type para el ultimo sorteo
set serveroutput on
clear screen
begin
    olap_sys.w_gl_automaticas_pkg.comparativo_lt_handler;
end;
/

--!en base a las predicciones de las terminaciones se actualizan las jugadas en gl_automaticas_detail
--!es necesario que la ejecucion de las predicciones en python esten generadas antes de ejecutar el bloque siguiente
set serveroutput on
clear screen
declare
    ln$drawing_id  number := 0;
begin
    select max(drawing_id) into ln$drawing_id from olap_sys.s_calculo_stats;
    olap_sys.w_gl_automaticas_pkg.upd_terminacion_cnt_handler(pn_drawing_id	=> ln$drawing_id);
    olap_sys.w_gl_automaticas_pkg.aciertos_repetidos_handler(pn_drawing_id => ln$drawing_id);                                                  
    commit;                                                  
end;
/

--!contador de terminacionessi
select jugar_flag
     , sorteo_actual
     , terminacion_cnt
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail 
 group by jugar_flag
     , sorteo_actual
     , terminacion_cnt
 order by jugar_flag, r_cnt desc
;  


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

select *
  from olap_sys.gl_automaticas_detail 
 where list_id = 1
 order by id 
; 

--<<<conteo de posiciones>>>
select seq
      , sorteo
      , d1
      , pos1
      , j_cnt1
      , r_cnt1
      , sorteo1_id
      , dif1
      , d2
      , pos2
      , j_cnt2
      , r_cnt2
      , sorteo2_id
      , dif2
      , d3
      , pos3
      , j_cnt3
      , r_cnt3
      , sorteo3_id
      , dif3
      , d4
      , pos4
      , j_cnt4
      , r_cnt4
      , sorteo4_id
      , dif4
      , d5
      , pos5
      , j_cnt5
      , r_cnt5
      , sorteo5_id
      , dif5
      , d6
      , pos6
      , j_cnt6
      , r_cnt6
      , sorteo6_id
      , dif6
  from olap_sys.gl_position_counts
 order by seq 
;

select pos1 from olap_sys.gl_position_counts where r_cnt1_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B1') > 0 and prediccion_sorteo = 1503;  
select pos2 from olap_sys.gl_position_counts where r_cnt2_flag = 1 and dif2_flag = 1 or (pos2 is not null and r_cnt2 > 0 and dif2 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B2') > 0 and prediccion_sorteo = 1503;  
select pos3 from olap_sys.gl_position_counts where r_cnt3_flag = 1 and dif3_flag = 1 or (pos3 is not null and r_cnt3 > 0 and dif3 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B3') > 0 and prediccion_sorteo = 1503;  
select pos4 from olap_sys.gl_position_counts where r_cnt4_flag = 1 and dif4_flag = 1 or (pos4 is not null and r_cnt4 > 0 and dif4 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B4') > 0 and prediccion_sorteo = 1503;  
select pos5 from olap_sys.gl_position_counts where r_cnt5_flag = 1 and dif5_flag = 1 or (pos5 is not null and r_cnt5 > 0 and dif5 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B5') > 0 and prediccion_sorteo = 1503;    
select pos6 from olap_sys.gl_position_counts where r_cnt6_flag = 1 and dif6_flag = 1 or (pos6 is not null and r_cnt6 > 0 and dif6 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B6') > 0 and prediccion_sorteo = 1503;   

select ID, B_TYPE, DECENA, POS, J_CNT, J_CNT_FLAG, R_CNT, R_CNT_FLAG, SORTEO_ID, DIF, DIF_FLAG, DIGIT_TYPE, COLOR_FR, COLOR_LT, CHNG
  from gl_position_counts_v
 where DIF_FLAG = 1 
   and b_type = 'B2'
 order by B_TYPE, J_CNT desc
; 

select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B6') > 0 and prediccion_sorteo = 1503;

Ejecutar los siguientes queries para recuperar las jugadas que se jugaran en el sorteo

## Queries para seleccionar jugadas

--!ejecutar este query antes del query para seleccionar jugadas
--!identifica los patrones del historico de los aciertos 
--!query 2
--!la salida de este query se utiliza pen el query de seleccion de jugadas
with main_tbl as (
select b_type
     , id
     , count(1) r_cnt
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
 group by b_type, id
 order by b_type
) --select * from main_tbl;
, percentile_tbl as (
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B1'
  group by b_type 
union
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B2'
  group by b_type 
union
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B3'
  group by b_type   
union
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B4'
  group by b_type 
union
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B5'
  group by b_type 
union
select b_type
     , percentile_disc(0.2) within group (order by r_cnt) perc_ini
  from main_tbl
 where b_type = 'B6'
  group by b_type     
) --select * from percentile_tbl order by b_type;
, listaggtbl as (
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B1'
   and r_cnt >= (select perc_ini from percentile_tbl where b_type = 'B1')
 group by b_type
union
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B2'
   and r_cnt >= (select perc_ini from percentile_tbl where b_type = 'B2')
 group by b_type
union
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B3'
   --!si el valor del percentile es 1 entonces el operador es solamente mayor que
   and r_cnt > (select perc_ini from percentile_tbl where b_type = 'B3')
 group by b_type
union
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B4'
   --!si el valor del percentile es 1 entonces el operador es solamente mayor que
   and r_cnt > (select perc_ini from percentile_tbl where b_type = 'B4')
 group by b_type  
union
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B5'
   --!si el valor del percentile es 1 entonces el operador es solamente mayor que
   and r_cnt > (select perc_ini from percentile_tbl where b_type = 'B5')
 group by b_type   
union
select b_type
     , listagg(id, ',') within group (order by r_cnt) list_ids
  from main_tbl
 where b_type = 'B6'
   and r_cnt >= (select perc_ini from percentile_tbl where b_type = 'B6')
 group by b_type  
)
select *
  from listaggtbl
;

--<<CONTEO DE POSICIONES CON CAMBIO>>>
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id id, b_type, digit, color_ubicacion cfr, ubicacion vfr, color_ley_tercio clt, ciclo_aparicion ca, pronos_ciclo pxc
     , preferencia_flag pre, case when CHNG_POSICION is null then '.' else 'X' end chg, rango_ley_tercio rlt, preferencia_num pre_num
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl) 
)
select id
     , b_type
     , chg
     , count(1) j_cnt
  from gl_tbl
 where chg = 'X' 
 group by id
     , b_type
     , chg
 order by b_type, chg    
;     

       
--<<<QUERIES PARA SELECCIONAR JUGADAS>>>

with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id id, b_type, digit, color_ubicacion cfr, ubicacion vfr, color_ley_tercio clt, ciclo_aparicion ca, pronos_ciclo pxc
     , preferencia_flag pre, case when CHNG_POSICION is null then '.' else 'X' end chg, rango_ley_tercio rlt, preferencia_num pre_num
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl) 
)
, lt_details_tbl as (
select red_cnt, green_cnt,blue_cnt, count(1) lt_cnt
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = (select max_id from resultado_tbl)
   and null_cnt  = 0 
   --!patrones con mas repeticiones
   and red_cnt in (0,1,2)
   and match_cnt in (0)
 group by red_cnt, green_cnt,blue_cnt
) --SELECT * FROM lt_details_tbl;
, lt_percentile_tbl as (
select percentile_disc(0.35) within group (order by lt_cnt) perc_lt_ini
from lt_details_tbl
) --SELECT * FROM lt_percentile_tbl;
, lt_filter_tbl as (
select red_cnt, green_cnt, blue_cnt
  from lt_details_tbl
 where lt_cnt >= (select perc_lt_ini from lt_percentile_tbl) 
) --SELECT * FROM lt_filter_tbl;
, ley_tercio_pattern_tbl as (
select lt1, lt2, lt3, lt4, lt5, lt6
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = (select max_id from resultado_tbl)
   and null_cnt  = 0 
   --!patrones con mas repeticiones
   and red_cnt in (0,1,2)
   and match_cnt in (0)
   and (red_cnt, green_cnt, blue_cnt) in (select red_cnt, green_cnt, blue_cnt from lt_filter_tbl)
   --!filtros basados en lt  
) --SELECT * FROM ley_tercio_pattern_tbl;
/*, e1_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B1'
   --and rlt between 1 and 8
   --and ca between 3 and 16
)
, e1_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B2'
   --and rlt between 3 and 13
)
, e1_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B3'
   --and rlt between 3 and 15
)
, e1_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B4'
   --and rlt between 3 and 15
)
, e1_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B5'
   --and rlt between 2 and 12
)
, e1_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num
  from gl_tbl
 where b_type = 'B6'
   --and rlt between 1 and 8
   --and ca between 3 and 15
) */
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
     , aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca_sum, comb_sum
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg
     , list_id, t2_cnt, terminacion_cnt
  from olap_sys.gl_automaticas_detail 
 where 1=1
   --and IA1 in (select digit from e1_b1_tbl)
   --and IA2 in (select digit from e1_b2_tbl)
   --and IA3 in (select digit from e1_b3_tbl)
   --and IA4 in (select digit from e1_b4_tbl)
   --and IA5 in (select digit from e1_b5_tbl)
   --and IA6 in (select digit from e1_b6_tbl)
   --!bandera producto del un proceso que revisa las parejas de 
   --!comb_sum y ca mas ganadoras
 --  and jugar_flag = 'Y'
   and list_id = 1
   --<<conteo de posiciones>>
   --and IA1 in (select pos1 from olap_sys.gl_position_counts where r_cnt1_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B1') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))  
   --and IA2 in (select pos1 from olap_sys.gl_position_counts where r_cnt2_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B2') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))
   --and IA3 in (select pos1 from olap_sys.gl_position_counts where r_cnt3_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B3') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))
   and IA4 in (select pos1 from olap_sys.gl_position_counts where r_cnt4_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B4') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))
   and IA5 in (select pos1 from olap_sys.gl_position_counts where r_cnt5_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B5') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))
   and IA6 in (select pos1 from olap_sys.gl_position_counts where r_cnt6_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select to_number(pred1) from olap_sys.predicciones_all where instr(prediccion_tipo,'B6') > 0 and prediccion_sorteo = (select max_id from resultado_tbl))
   --<<lista de apariciones previas>>
--   and IA1 in (select history_digit from olap_sys.history_digit_info where b_type = 'B1' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
--   and IA2 in (select history_digit from olap_sys.history_digit_info where b_type = 'B2' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
--   and IA3 in (select history_digit from olap_sys.history_digit_info where b_type = 'B3' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
--   and IA4 in (select history_digit from olap_sys.history_digit_info where b_type = 'B4' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
--   and IA5 in (select history_digit from olap_sys.history_digit_info where b_type = 'B5' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
--   and IA6 in (select history_digit from olap_sys.history_digit_info where b_type = 'B6' and chng = '.' and lt != '#' and drawing_id >=  (select max_id from resultado_tbl))
) 
, sum_ca_cnt_tbl as ( 
select sum_ca
     , count(1) r_cnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > 594
 group by sum_ca
)
, sum_ca_percentile_tbl as (
select percentile_disc(0.3) within group (order by r_cnt) perc_sum_ca_ini
  from sum_ca_cnt_tbl
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
                 aciertos_accum, incidencia, incidencia_cnt, jugar_flag, ca_sum, comb_sum, terminacion_cnt t_cnt
  from output_tbl
 where 1=1
   --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
   and sum_chg in (0,1,2)
   --!solo jugadas con 1 o 2 numeros repetidos
   and repetidos_cnt in (1,2)
   --!solo jugadas que esten el patron de ley del tercio
   and (t1, t2, t3, t4, t5, t6) in (select lt1, lt2, lt3, lt4, lt5, lt6 from ley_tercio_pattern_tbl)
   --!solo ca que esten arriba del percentil
   and ca_sum in (select sum_ca from sum_ca_cnt_tbl where r_cnt >= (select perc_sum_ca_ini from sum_ca_percentile_tbl))
   --!jugadas que solo tengran menos de 2 valores en los ciclos de aparicion
   --and pxc_cnt in (0,1,2,3)
   and terminacion_cnt in (0,1)
   --!patron de combinacion entre c1, c5, c6
--   and b1_b5_b6_flag in (1,2)
 order by repetidos_cnt, ia1, ia2, ia3, ia4, ia5, ia6; 

##### Dado un dígito dado y su posición, imprimir que otro dígito se ha aparecido mas veces

--!C:\temp\sarnNextDigit.xlsx
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)
, posicion_tbl as (
select hh.drawing_id, hh.b_type, hh.digit digit_hdr
     , hd.id, hd.history_digit, hd.match_cnt, hd.drawing_cnt, hd.drawing_list, hd.next_drawing_id, hd.winner_flag
  from olap_sys.position_digit_history_header hh
     , olap_sys.position_digit_history_dtl hd
 where hh.drawing_id = (select max_id from resultado_tbl) 
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
 where drawing_id >=  (select max(drawing_id) from resultados_tbl) -100
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
   and jugar_flag = 'Y'
--   and B_TYPE = 'B6'
 order by drawing_id, b_type, match_cnt desc, history_digit 
; 

--!C:\temp\sarnNextDigit.xlsx
--!query equivalente
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)
select drawing_id, b_type, digit_hdr, history_digit, match_cnt, drawing_cnt, next_drawing_id, id, fr, lt, ca, pxc, preferencia, chng, pxc_pref
  from olap_sys.history_digit_info
 where drawing_id >=  (select max_id from resultado_tbl) 
   and chng = '.'
   and lt != '#'
 order by drawing_id, b_type, match_cnt desc, history_digit  
;



##### Dado un dígito de la posición 1, imprimir de forma descendente que números se repiten mas

clear screen
set serveroutput on
begin olap_sys.w_new_pick_panorama_pkg.listado_numeros_handler(pn_comb1 => 2); end;
/

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

--!conteo de aciertos vs terminaciones
select sorteo_actual
     , aciertos_cnt
     , terminacion_cnt
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail 
 group by sorteo_actual
     , aciertos_cnt
     , terminacion_cnt
 order by aciertos_cnt desc, r_cnt desc
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

--!conteo de apariciones de C1, C5 y C6 para jugadas, resultados y automaticas
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
select ia1, ia2, ia3, ia4, ia5, ia6
     , olap_sys.w_common_pkg.get_c1_c5_c6_rank(ia1, ia5, ia6) a_c1_c5_c6_rank
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
   and a_cnt > 0 
 order by r_cnt desc, a_cnt desc 
;
