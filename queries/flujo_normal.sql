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
select distinct prediccion_nombre, 0 muestra, prediccion_sorteo, replace(prediccion_tipo,'_2',null) prediccion_tipo, 
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
   and match_cnt > 2
   and prediccion_tipo!= '6.CHNG'
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


--!mostrar las predicciones para cada b_type para el ultimo sorteo
set serveroutput on
clear screen
begin
    olap_sys.w_gl_automaticas_pkg.comparativo_lt_handler;
end;
/

--!en base a las predicciones de las terminaciones se actualizan las jugadas en gl_automaticas_detail
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
/*
este proceso es reemplazado por la ejecucion del bloque anonimo superior
--!Tab Name: GL TEMPLATE PATRONES
--!(resumen) calculo del porcentaje de apariciones de b_type en base a rank_cnt
--!pn_history puede ser cualquie valor entero positivo;
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)
, lt_history_tbl as (
select *
  from olap_sys.ley_tercio_history_dtl
 where winner_flag is not null
   and next_drawing_id >= (select max_id - :pn_history from resultado_tbl)
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
) --SELECT * FROM lt_history_cnt_tbl;
, output_tbl as (
select b_type
     , lt
     , rank_cnt
     , cnt
     , (select min(next_drawing_id) from lt_history_tbl) min_id
     , (select max(next_drawing_id) from lt_history_tbl) max_id
     ,  :pn_history history_cnt 
  from lt_history_cnt_tbl
) 
, output_sum_tbl as (
select b_type b_typex
     , rank_cnt rank_cntx
     , sum(cnt) sum_cnt
  from output_tbl
 group by b_type
     , rank_cnt 
) select DISTINCT b_type
     , lt
     , rank_cnt
     , cnt
     , min_id
     , max_id
     , history_cnt
     , (select sum_cnt from output_sum_tbl where b_typex=b_type and rank_cntx=rank_cnt) sum_cnt
     , (select sum(sum_cnt) from output_sum_tbl where b_typex=b_type) grand_total_cnt
     , round(((select sum_cnt from output_sum_tbl where b_typex=b_type and rank_cntx=rank_cnt)/(select sum(sum_cnt) from output_sum_tbl where b_typex=b_type))*100) pct_cnt
  from output_tbl   
 order by b_type
     , pct_cnt desc
     , rank_cnt
;

--!(detalle) deglose de la columna rank_cnt agrupado por ID, B_TYPE y LT
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
) --select * from rank_all_tbl;
select 1 seqno
     , d.drawing_id
     , h.lt1
     , h.lt2
     , h.lt3
     , h.lt4
     , h.lt5
     , h.lt6   
     , d.b_type
     , d.id
     , d.lt
     , d.lt_cnt
     , d.drawing_id_ini
     , d.drawing_id_end
     , d.last_drawing_id_cnt
     , d.last_drawing_id 
     , d.next_drawing_id
     , nvl((select rank_cnt from rank_tbl where drawing_idx=h.drawing_id and b_typex=d.b_type and idx=d.id and ltx=d.lt),0) rank_cnt
     , nvl(d.winner_flag,'.') winner_flag
  from olap_sys.ley_tercio_history_header h
     , olap_sys.ley_tercio_history_dtl d
 where h.drawing_id = d.drawing_id
   and d.drawing_id = (select max_id from resultado_tbl)
 order by seqno, drawing_id desc, b_type, id;
*/

Abrir el archivo llamado **==jugadas.xlsm==**, copiar la salida de este proceso en el tab llamado **==GL TEMPLATE PATRONES==**. Posteriormente, copiar la columna D para cada **==B_TYPE (R,G,B)==** a su correspondiente B_TYPE en el tab **==LT HISTORICO==** para el sorteo que se va a jugar.

En ela app de GigaLoterias identificar el siguiente color de la Ley del Tercio para cada posicion. Actualizar el tab **==LT HISTORICO==** para cada posicion de B_TYPE del sorteo que se va a jugar.


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

Ejecutar los siguientes queries para recuperar las jugadas que se jugaran en el sorteo

## Queries para seleccionar jugadas

--!recuperar las combinaciones con mas repeticiones para la posicion B5,B6
select --comb1,
       comb5,
       comb6,
       count(1) rcnt
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings) - 200
   and comb1 < 10
   and comb5 > 29
   and comb6 > 29
--   and comb5 in (24,28,29,34)
   and comb6 in (35,39)
 group by --comb1,
       comb5,
       comb6
having count(1) > 1      
 order by rcnt desc,
       comb5,
       comb6;



--!ejecutar este query antes del query para seleccionar jugadas
--!identifica los patrones del historico de los aciertos 
--!query 1
with main_tbl as (
select b_type main_b_type
     , min(ID) min_id
     , round(avg(ID)) avg_id
     , max(ID) max_id
     , count(1) r_cnt
     , min(drawing_id) min_drawing_id
     , max(drawing_id) max_drawing_id
     , percentile_disc(0.1) within group (order by id) per_id_ini
     , percentile_disc(0.85) within group (order by id) per_id_end
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
 group by b_type
) --select * from main_tbl;
, under_avg_tbl as (
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B1'
   and id <= (select avg_id from main_tbl where main_b_type = 'B1')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B2'
   and id <= (select avg_id from main_tbl where main_b_type = 'B2')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B3'
   and id <= (select avg_id from main_tbl where main_b_type = 'B3')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B4'
   and id <= (select avg_id from main_tbl where main_b_type = 'B4')
  group by b_type  
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B5'
   and id <= (select avg_id from main_tbl where main_b_type = 'B5')
  group by b_type   
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B6'
   and id <= (select avg_id from main_tbl where main_b_type = 'B6')
  group by b_type    
)
, above_avg_tbl as (
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B1'
   and id > (select avg_id from main_tbl where main_b_type = 'B1')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B2'
   and id > (select avg_id from main_tbl where main_b_type = 'B2')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B3'
   and id > (select avg_id from main_tbl where main_b_type = 'B3')
  group by b_type 
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B4'
   and id > (select avg_id from main_tbl where main_b_type = 'B4')
  group by b_type  
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B5'
   and id > (select avg_id from main_tbl where main_b_type = 'B5')
  group by b_type   
union
select b_type
     , count(1) r_cnt 
  from olap_sys.history_digit_info
 where winner_flag = 'Y'
   and  b_type = 'B6'
   and id > (select avg_id from main_tbl where main_b_type = 'B6')
  group by b_type    
) 
select main_b_type, min_id, avg_id, max_id, r_cnt, min_drawing_id, max_drawing_id
     , (select r_cnt from under_avg_tbl where b_type = main_b_type) under_avg
     , (select r_cnt from above_avg_tbl where b_type = main_b_type) above_avg
     , to_char(round(((select r_cnt from under_avg_tbl where b_type = main_b_type)/r_cnt)*100))||'%' low_equal_avg_pct
     , to_char(round(((select r_cnt from above_avg_tbl where b_type = main_b_type)/r_cnt)*100))||'%' high_avg_pct   
     , (select count(distinct drawing_id) from olap_sys.history_digit_info where winner_flag = 'Y') total_rows
     , round(avg_id*(avg_id-min_id)/(max_id-min_id)) scaled
  from main_tbl
 order by main_b_type; 


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


       
Queries para seleccionar jugadas

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
)
, lt_percentile_tbl as (
select percentile_disc(0.35) within group (order by lt_cnt) perc_lt_ini
from lt_details_tbl
) 
, lt_filter_tbl as (
select red_cnt, green_cnt, blue_cnt
  from lt_details_tbl
 where lt_cnt >= (select perc_lt_ini from lt_percentile_tbl) 
)
, ley_tercio_pattern_tbl as (
select lt1, lt2, lt3, lt4, lt5, lt6
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = (select max_id from resultado_tbl)
   and null_cnt  = 0 
   --!patrones con mas repeticiones
   and red_cnt in (0,1,2)
   and match_cnt in (0)
   and (red_cnt, green_cnt, blue_cnt) in (select red_cnt, green_cnt, blue_cnt from lt_filter_tbl)
) 
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
, aciertos_max_tbl as (
select max(aciertos_accum)/10 max_aciertos_accum from (
select aciertos_accum
     , count(1) cnt
  from olap_sys.gl_automaticas_detail
 group by aciertos_accum)
) --select * from aciertos_max_tbl;
, aciertos_cnt_tbl as (
select aciertos_cnt
     , aciertos_accum
     , count(1) j_cnt 
  from olap_sys.gl_automaticas_detail
 where aciertos_accum > (select max_aciertos_accum from aciertos_max_tbl) 
 group by aciertos_cnt
     , aciertos_accum 
having count(1) > 1  
) --select * from aciertos_cnt_tbl order by aciertos_accum;
, aciertos_percentile_tbl as (
select percentile_disc(0.1) within group (order by aciertos_accum) per_acierto_accum_ini
  from aciertos_cnt_tbl
) --select * from aciertos_percentile_tbl;
, output_tbl as (
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, 
       decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, lt5, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, 
       ca1, ca2, ca3, ca4, ca5, ca6, 
       d1, d2, d3, d4, d5, d6,
       0 pxc_cnt,
       pf1, pf2, pf3, pf4, pf5, pf6, 
       pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, 
       sorteo_actual, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, 
       chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca_sum, comb_sum,
       case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt,
       terminacion_cnt
  from olap_sys.gl_automaticas_detail 
 where 1=1
   --and IA1 in (select digit from e1_b1_tbl)
   --and IA2 in (select digit from e1_b2_tbl)
   --and IA3 in (select digit from e1_b3_tbl)
   --and IA4 in (select digit from e1_b4_tbl)
   --and IA5 in (select digit from e1_b5_tbl)
   --and IA6 in (select digit from e1_b6_tbl)
   --!enfocar el query en el rango donde hay mas aciertos aprox el 85%
   and aciertos_accum > (select per_acierto_accum_ini from aciertos_percentile_tbl) 
   --and aciertos_accum > 3 
   --!bandera producto del un proceso que revisa las parejas de 
   --!comb_sum y ca mas ganadoras
   and jugar_flag = 'Y'
   --and list_id = 1
   --!revisando estas condiciones
   --!se le esta sumando +1 al avg del id para
   --and IA1 in (select history_digit from olap_sys.history_digit_info where b_type = 'B1' and id in (7,4,3,8,2) and drawing_id = sorteo_actual)
   --and IA2 in (select history_digit from olap_sys.history_digit_info where b_type = 'B2' and chng = '.' and id in (6,4,5,10,2,7,8) and drawing_id = sorteo_actual)
   --and IA3 in (select history_digit from olap_sys.history_digit_info where b_type = 'B3' and chng = '.' and id in (3,5,6,9) and drawing_id = sorteo_actual)
   --and IA4 in (select history_digit from olap_sys.history_digit_info where b_type = 'B4' and chng = '.' and id in (9,11,8,6,3,5) and drawing_id = sorteo_actual)
   --and IA5 in (select history_digit from olap_sys.history_digit_info where b_type = 'B5' and chng = '.' and id in (9,6,4,1,3) and drawing_id = sorteo_actual)
   --and IA6 in (select history_digit from olap_sys.history_digit_info where b_type = 'B6' and chng = '.' and id in (10,5,4,6,8,1,3,2) and drawing_id = sorteo_actual)   
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
                 decena, sorteo_actual, b1_b4_b6_flag, repetidos_cnt, 
                 c1_c6_flag, mapa_primos, 
                 chg1, chg2, chg3, chg4, chg5, chg6, 
                 aciertos_accum, incidencia, incidencia_cnt, jugar_flag, ca_sum, comb_sum, terminacion_cnt t_cnt
  from output_tbl
 where 1=1
   --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
   and sum_chg in (0)
   --!solo jugadas con 1 o 2 numeros repetidos
--   and repetidos_cnt in (1,2)
   --!solo jugadas que esten el patron de ley del tercio
   and (t1, t2, t3, t4, t5, t6) in (select lt1, lt2, lt3, lt4, lt5, lt6 from ley_tercio_pattern_tbl)
   --!solo ca que esten arriba del percentil
   and ca_sum in (select sum_ca from sum_ca_cnt_tbl where r_cnt >= (select perc_sum_ca_ini from sum_ca_percentile_tbl))
   --!jugadas que solo tengran menos de 2 valores en los ciclos de aparicion
   --and pxc_cnt in (0,1,2,3)
   and terminacion_cnt in (2,3)
 order by repetidos_cnt, aciertos_accum desc, ia1, ia2, ia3, ia4, ia5, ia6; 

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
 order by p.drawing_id, p.b_type, p.match_cnt desc, p.history_digit 
; 

--!C:\temp\sarnNextDigit.xlsx
--!query equivalente
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)
select drawing_id, b_type, digit_hdr, history_digit, match_cnt, drawing_cnt, next_drawing_id, id, fr, lt, ca, pxc, preferencia, chng, pxc_pref
  from olap_sys.history_digit_info
 where drawing_id >=  (select max_id from resultado_tbl) 
 order by drawing_id, b_type, match_cnt desc, history_digit  
;


##### Dado un dígito de la posición 1, imprimir de forma descendente que números se repiten mas

clear screen
set serveroutput on
begin olap_sys.w_new_pick_panorama_pkg.listado_numeros_handler(pn_comb1 => 3); end;
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
