--C:\temp\sarnMR.xlsx
--!Tab Name: MELATE RETRO
select *
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > (select max(gambling_id) from olap_sys.sl_gamblings)- 10
 order by gambling_id;


--!recupera las predicciones para el sorteo a jugar != '15.GL'
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
   and prediccion_sorteo = (select max_id from siguiente_sorteo_tbl)-1
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
declare
    cursor c_predicciones is
    with siguiente_sorteo_tbl as (
    select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
    )
    , predicciones_tbl as (
    select prediccion_sorteo, prediccion_tipo||'-'||b_type prediccion_tipo, b_type, 
           to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num, to_number(PRES1) pres1, to_number(pred1) pred1
      FROM olap_sys.predicciones_all
     WHERE prediccion_tipo = '15.GL'
       and prediccion_sorteo = (select max_id from siguiente_sorteo_tbl)
    -- order by b_type, rank_pres--, pres1 desc, pred1_num
    ) --select * from predicciones_tbl;
    , sum_prediccion_tbl as (
    select prediccion_sorteo, prediccion_tipo, b_type,
           pred1, sum(pres1) sum_pres 
      from predicciones_tbl 
     group by prediccion_sorteo, prediccion_tipo, b_type,
           pred1
     order by b_type, sum_pres desc       
    ) --select * from sum_prediccion_tbl;
    , rank_prediccion_tbl as (
    select prediccion_sorteo, prediccion_tipo, b_type, pred1, round(sum_pres,2) sum_pres, rank() over (partition by b_type order by sum_pres desc) rank_pres
      from sum_prediccion_tbl 
    ) select * 
       from rank_prediccion_tbl 
      order by b_type, rank_pres;
begin
        update olap_sys.predicciones_all
           set pred2 = '0.0'
             , pred3 = '0'
         where prediccion_sorteo = (select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null)
           and prediccion_tipo = '15.GL';

    for p in c_predicciones loop
        update olap_sys.predicciones_all
           set pred2 = to_char(p.sum_pres)
             , pred3 = to_char(p.rank_pres)
         where prediccion_sorteo = p.prediccion_sorteo
           and prediccion_tipo = '15.GL'
           and b_type = p.b_type
           and pred1 = to_char(p.pred1);
    end loop;
    commit;
end;
/

SELECT *
  FROM olap_sys.predicciones_all
 WHERE prediccion_tipo = '15.GL'
   and prediccion_sorteo = 1514
--   and b_type = 'B5'
 order by b_type, to_number(pred3)
;

--!ordenamiento de las predicciones por rank y probabilidad
with siguiente_sorteo_tbl as (
select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
)
, output_tbl as (
select distinct prediccion_nombre, 0 muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, to_number(pred3) rank_pred, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6, match7, match8, match9, match0,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num
  FROM olap_sys.predicciones_all
 WHERE prediccion_tipo = '15.GL'
   and prediccion_sorteo = (select max_id from siguiente_sorteo_tbl)
) 
select prediccion_nombre, muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, rank_pred, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0,
       match_cnt, match_pct, match1, match2, match3, match4, match5, match6, match7, match8, match9, match0
  from output_tbl
-- where to_number(rank_pred) < 9 
 order by rank_pred
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
   and prediccion_tipo not in ('6.CHNG','12.TERMINACIONES')
   and prediccion_sorteo >= (select max(prediccion_sorteo) from olap_sys.predicciones_all) - 1
/*union all
select prediccion_nombre,  0 muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0, match_cnt, 0 match_pct,
       match1, match2, match3, match4, match5, match6, match7, match8, match9, match0,
       to_number(substr(prediccion_tipo, 1,instr(prediccion_tipo,'.')-1)) prediccion_tipo_num 
  from olap_sys.predicciones_all 
 where 1=1
   and match_cnt > 0
   and (instr(prediccion_tipo,'14.') > 0 or instr(prediccion_tipo,'13.') > 0)
   and prediccion_sorteo >= (select max(prediccion_sorteo) from olap_sys.predicciones_all) - 10
 order by match_cnt desc, prediccion_tipo_num, prediccion_sorteo desc */
)
select prediccion_nombre, muestra, prediccion_sorteo, prediccion_tipo, 
       pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8, pred9, pred0,
       res1, res2, res3, res4, res5, res6, res7, res8, res9, res0,
       match_cnt, match_pct, match1, match2, match3, match4, match5, match6, match7, match8, match9, match0
  from output_tbl
;  


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
    olap_sys.w_gl_automaticas_pkg.upd_terminacion_cnt_handler(pn_drawing_id	=> ln$drawing_id);
    olap_sys.w_gl_automaticas_pkg.upd_gl_automaticas_handler(pn_drawing_id => ln$drawing_id
                                                  , pv_ca_comb_flag => 'Y');
    olap_sys.w_gl_automaticas_pkg.aciertos_repetidos_handler(pn_drawing_id => ln$drawing_id);  
    olap_sys.w_gl_automaticas_pkg.upd_avg_gl_ca(pn_drawing_id => ln$drawing_id);  
    olap_sys.w_gl_automaticas_pkg.get_patron_handler;
    commit;                                                  
end;
/

--!contador de aciertos
select sorteo_actual
     , aciertos_cnt
     , digito_cnt
     , ia6_cnt
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 where aciertos_cnt > 0
   and digito_cnt > 0
 group by sorteo_actual
     , aciertos_cnt
     , digito_cnt
     , ia6_cnt
 order by aciertos_cnt desc, r_cnt
;  

--!aciertos en funcion de numeros repetidos
select sorteo_actual
     , aciertos_cnt
     , repetidos_cnt
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 where aciertos_cnt > 0
   and digito_cnt > 0
 group by sorteo_actual
     , aciertos_cnt
     , repetidos_cnt
 order by aciertos_cnt desc, r_cnt desc
;  

--!aciertos en funcion de acumulado de aciertos
select sorteo_actual
     , case when aciertos_accum between 1 and 5 then '1-5' else case when aciertos_accum between 6 and 10 then '6-10' else case when aciertos_accum between 11 and 15 then '11-15' else case when aciertos_accum between 16 and 20 then '16-20' else '>21' end end end end aciertos_accum
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 where aciertos_cnt > 0
 group by sorteo_actual
     , case when aciertos_accum between 1 and 5 then '1-5' else case when aciertos_accum between 6 and 10 then '6-10' else case when aciertos_accum between 11 and 15 then '11-15' else case when aciertos_accum between 16 and 20 then '16-20' else '>21' end end end end
 order by r_cnt desc
;  


--!aciertos en funcion de patron de la posicion 2-5
select sorteo_actual
     , PATRON_25_CNT
     , count(1) r_cnt
  from olap_sys.gl_automaticas_detail
 where aciertos_cnt > 0
 group by sorteo_actual
     , PATRON_25_CNT
 order by r_cnt desc
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

--<<<conteo de posiciones>>>
--Nota: el proceso que hace estos conteos debe estar basado en la vista gl_automaticas_detail_v. reevisar este proceso
select seq
      , sorteo
      , d1
      , pos1
      , j_cnt1
      , r_cnt1
      , sorteo1_id
      , dif1
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') fr1
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') lt1
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') pxc1
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') pref1
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') chng1
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B1' and pos1 = digit),'#') avg_ca_flag1
      , d2
      , pos2
      , j_cnt2
      , r_cnt2
      , sorteo2_id
      , dif2
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') fr2
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') lt2
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') pxc2
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') pref2
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') chng2
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B2' and pos2 = digit),'#') avg_ca_flag2
      , d3
      , pos3
      , j_cnt3
      , r_cnt3
      , sorteo3_id
      , dif3
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') fr3
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') lt3
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') pxc3
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') pref3
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') chng3
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B3' and pos3 = digit),'#') avg_ca_flag3
      , d4
      , pos4
      , j_cnt4
      , r_cnt4
      , sorteo4_id
      , dif4
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') fr4
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') lt4
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') pxc4
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') pref4
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') chng4
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B4' and pos4 = digit),'#') avg_ca_flag4
      , d5
      , pos5
      , j_cnt5
      , r_cnt5
      , sorteo5_id
      , dif5
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') fr5
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') lt5
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') pxc5
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') pref5
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') chng5
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B5' and pos5 = digit),'#') avg_ca_flag5
      , d6
      , pos6
      , j_cnt6
      , r_cnt6
      , sorteo6_id
      , dif6
      , nvl((select decode(color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') fr6
      , nvl((select decode(color_ley_tercio,1,'R',2,'G',3,'B')  from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') lt6
      , nvl((select decode(pronos_ciclo,null,'.',to_char(pronos_ciclo)) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') pxc6
      , nvl((select decode(preferencia_flag,null,'.',preferencia_flag) from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') pref6
      , nvl((select decode(chng_posicion,null,'.','C') from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') chng6
      , nvl((select case when b_avg_ciclo_aparicion = 1 and a_avg_ciclo_aparicion = 0 then 'B' else 'A' end from olap_sys.s_calculo_stats where sorteo = drawing_id and b_type = 'B6' and pos6 = digit),'#') avg_ca_flag6
  from olap_sys.gl_position_counts
 order by seq 
;

--actualizar el numero del sorteo en los siguientes queries antes de ejecutarlos
select pos1 from olap_sys.gl_position_counts where r_cnt1_flag = 1 and dif1_flag = 1 or (pos1 is not null and r_cnt1 > 0 and dif1 = 0) union select history_digit history_digit_pos1 from olap_sys.history_digit_info where b_type = 'B1' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;  
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B1' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;
select pos2 from olap_sys.gl_position_counts where r_cnt2_flag = 1 and dif2_flag = 1 or (pos2 is not null and r_cnt2 > 0 and dif2 = 0) union select history_digit history_digit_pos2 from olap_sys.history_digit_info where b_type = 'B2' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;  
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B2' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;
select pos3 from olap_sys.gl_position_counts where r_cnt3_flag = 1 and dif3_flag = 1 or (pos3 is not null and r_cnt3 > 0 and dif3 = 0) union select history_digit history_digit_pos3 from olap_sys.history_digit_info where b_type = 'B3' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;  
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B3' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;
select pos4 from olap_sys.gl_position_counts where r_cnt4_flag = 1 and dif4_flag = 1 or (pos4 is not null and r_cnt4 > 0 and dif4 = 0) union select history_digit history_digit_pos4 from olap_sys.history_digit_info where b_type = 'B4' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;  
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B4' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;
select pos5 from olap_sys.gl_position_counts where r_cnt5_flag = 1 and dif5_flag = 1 or (pos5 is not null and r_cnt5 > 0 and dif5 = 0) union select history_digit history_digit_pos5 from olap_sys.history_digit_info where b_type = 'B5' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;    
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B5' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;
select pos6 from olap_sys.gl_position_counts where r_cnt6_flag = 1 and dif6_flag = 1 or (pos6 is not null and r_cnt6 > 0 and dif6 = 0) union select history_digit history_digit_pos6 from olap_sys.history_digit_info where b_type = 'B6' and chng = '.' and lt != '#' and drawing_id >=  1517 order by 1;   
select distinct to_number(pred1) prediccion from olap_sys.predicciones_all where b_type = 'B6' and prediccion_sorteo = 1517 and to_number(pred3) < 9 order by 1;

--predicciones
with resultado_tbl as (
select max(drawing_id) max_id from olap_sys.s_calculo_stats where winner_flag is null
)
, output_tbl as (
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B1' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B1' and id = (select max_id from resultado_tbl)
union
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B2' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B2' and id = (select max_id from resultado_tbl)
union
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B3' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B3' and id = (select max_id from resultado_tbl)
union
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B4' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B4' and id = (select max_id from resultado_tbl)
union
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B5' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B5' and id = (select max_id from resultado_tbl)
union
select distinct b_type, to_number(pred1) p_b, to_number(pred3) rank_pred from olap_sys.predicciones_all where prediccion_tipo = '15.GL' and b_type = 'B6' and prediccion_sorteo = (select max_id from resultado_tbl) union select b_type, digit, 99 from olap_sys.gl_jugada_actual_v where repetido = 1 and b_type = 'B6' and id = (select max_id from resultado_tbl)
)
select *
  from output_tbl
 order by b_type, rank_pred, p_b  
;

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

     
--<<<QUERIES PARA SELECCIONAR JUGADAS>>>


--<<OPCION A>>
--C:\temp\jugadas_a_jugar.xlsx
--![[ultimo resultado}}
with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
)  
--![[patron de conteo basado en decenas, digitos y sorteos]]
, pc_tbl as (
select * 
  from olap_sys.gl_position_counts
 where sorteo = (select max_id from resultado_tbl)  
) --select * from pc_tbl;
--B1
, pos1_all_tbl as (
select d1
     , pos1
     , j_cnt1
     , r_cnt1
     , dif1
 from  pc_tbl   
 where d1 is not null    
)
, pos1_stats_tbl as (
select percentile_disc(0.25) within group (order by dif1) perc_dif1_ini
     , percentile_disc(0.75) within group (order by dif1) perc_dif1_end
     , round(avg(r_cnt1)) avg_r_cnt1 
 from pos1_all_tbl
)
, b1_tbl as (
select 'B1' b_type
     , pos1 pos
  from pos1_all_tbl
 where dif1 >= (select perc_dif1_ini from pos1_stats_tbl) 
   and dif1 <= (select perc_dif1_end from pos1_stats_tbl) 
   --and r_cnt1 >= (select avg_r_cnt1 from pos1_stats_tbl)
union
select 'B1' b_type
     , pos1
  from pos1_all_tbl
 where dif1 in (0,1)
) --select * from b1_tbl;
--B2
, pos2_all_tbl as (
select d2
     , pos2
     , j_cnt2
     , r_cnt2
     , dif2
 from  pc_tbl   
 where d2 is not null    
)
, pos2_stats_tbl as (
select percentile_disc(0.25) within group (order by dif2) perc_dif2_ini
     , percentile_disc(0.75) within group (order by dif2) perc_dif2_end
     , round(avg(r_cnt2)) avg_r_cnt2 
 from pos2_all_tbl
)
, b2_tbl as (
select 'B2' b_type
     , pos2 pos
  from pos2_all_tbl
 where dif2 >= (select perc_dif2_ini from pos2_stats_tbl) 
   and dif2 <= (select perc_dif2_end from pos2_stats_tbl) 
   and r_cnt2 >= (select avg_r_cnt2 from pos2_stats_tbl)
union
select 'B2' b_type
     , pos2
  from pos2_all_tbl
 where dif2 in (0,1)
) --select * from b2_tbl;
--B3
, pos3_all_tbl as (
select d3
     , pos3
     , j_cnt3
     , r_cnt3
     , dif3
 from  pc_tbl   
 where d3 is not null    
)
, pos3_stats_tbl as (
select percentile_disc(0.25) within group (order by dif3) perc_dif3_ini
     , percentile_disc(0.75) within group (order by dif3) perc_dif3_end
     , round(avg(r_cnt3)) avg_r_cnt3 
 from pos3_all_tbl
)
, b3_tbl as (
select 'B3' b_type
     , pos3 pos
  from pos3_all_tbl
 where dif3 >= (select perc_dif3_ini from pos3_stats_tbl) 
   and dif3 <= (select perc_dif3_end from pos3_stats_tbl) 
   and r_cnt3 >= (select avg_r_cnt3 from pos3_stats_tbl)
union
select 'B3' b_type
     , pos3
  from pos3_all_tbl
 where dif3 in (0,1)
) --select * from b3_tbl;
--B4
, pos4_all_tbl as (
select d4
     , pos4
     , j_cnt4
     , r_cnt4
     , dif4
 from  pc_tbl   
 where d4 is not null    
)
, pos4_stats_tbl as (
select percentile_disc(0.25) within group (order by dif4) perc_dif4_ini
     , percentile_disc(0.75) within group (order by dif4) perc_dif4_end
     , round(avg(r_cnt4)) avg_r_cnt4 
 from pos4_all_tbl
)
, b4_tbl as (
select 'B4' b_type
     , pos4 pos
  from pos4_all_tbl
 where dif4 >= (select perc_dif4_ini from pos4_stats_tbl) 
   and dif4 <= (select perc_dif4_end from pos4_stats_tbl) 
   and r_cnt4 >= (select avg_r_cnt4 from pos4_stats_tbl)
union
select 'B4' b_type
     , pos4
  from pos4_all_tbl
 where dif4 in (0,1)
) --select * from b4_tbl;
--B5
, pos5_all_tbl as (
select d5
     , pos5
     , j_cnt5
     , r_cnt5
     , dif5
 from  pc_tbl   
 where d5 is not null    
)
, pos5_stats_tbl as (
select percentile_disc(0.25) within group (order by dif5) perc_dif5_ini
     , percentile_disc(0.75) within group (order by dif5) perc_dif5_end
     , round(avg(r_cnt5)) avg_r_cnt5 
 from pos5_all_tbl
)
, b5_tbl as (
select 'B5' b_type
     , pos5 pos
  from pos5_all_tbl
 where dif5 >= (select perc_dif5_ini from pos5_stats_tbl) 
   and dif5 <= (select perc_dif5_end from pos5_stats_tbl) 
   and r_cnt5 >= (select avg_r_cnt5 from pos5_stats_tbl)
union
select 'B5' b_type
     , pos5
  from pos5_all_tbl
 where dif5 in (0,1)
) --select * from b5_tbl;
--B6
, pos6_all_tbl as (
select d6
     , pos6
     , j_cnt6
     , r_cnt6
     , dif6
 from  pc_tbl   
 where d6 is not null    
)
, pos6_stats_tbl as (
select percentile_disc(0.25) within group (order by dif6) perc_dif6_ini
     , percentile_disc(0.75) within group (order by dif6) perc_dif6_end
     , round(avg(r_cnt6)) avg_r_cnt6 
 from pos6_all_tbl
)
, b6_tbl as (
select 'B6' b_type
     , pos6 pos
  from pos6_all_tbl
 where dif6 >= (select perc_dif6_ini from pos6_stats_tbl) 
   and dif6 <= (select perc_dif6_end from pos6_stats_tbl) 
union
select 'B6' b_type
     , pos6
  from pos6_all_tbl
 where dif6 in (0,1)
) --select * from b6_tbl;
--![[informacion de gigaloterias]]
, gl_tbl as (
select drawing_id id, b_type, digit, color_ubicacion cfr, ubicacion vfr, color_ley_tercio clt, ciclo_aparicion ca, pronos_ciclo pxc
     , preferencia_flag pre, case when CHNG_POSICION is null then '.' else 'X' end chg, rango_ley_tercio rlt, preferencia_num pre_num
     --, avg_ciclo_aparicion avg_ca
     --!se le agregan 2 unidades al avg para extender el rango de seleccion de jugadas
     , case when b_type in ('B1','B2','B5','B6') then avg_ciclo_aparicion + 2 else avg_ciclo_aparicion end avg_ca
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl) 
)
, e1_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B1'
) --select * from e1_b1_tbl;
, e1_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B2'
)
, e1_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B3'
)
, e1_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B4'
)
, e1_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B5'
)
, e1_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, decode(pxc,null,0,1) pxc, decode(pre,null,0,2) pre, rlt, pre_num, avg_ca
  from gl_tbl
 where b_type = 'B6'
) --SELECT * FROM e1_b6_tbl;
--![[informacion de patrones de LT]]
, lt_details_tbl as (
select lt1, lt2, lt3, lt4, lt5, lt6
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = (select max_id from resultado_tbl)
   and null_cnt  = 0 
   and match_cnt = 0
)
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
--![[patron de resultados por posicion]]
, patron_posicion_tbl as (
select hh.drawing_id, hh.b_type, hh.digit digit_hdr
     , hd.id, hd.history_digit, hd.match_cnt, hd.drawing_cnt, hd.drawing_list, hd.next_drawing_id, hd.winner_flag
  from olap_sys.position_digit_history_header hh
     , olap_sys.position_digit_history_dtl hd
 where hh.drawing_id = (select max_id from resultado_tbl) 
   and hh.header_id = hd.header_id
   and hh.b_type = hd.b_type
)
, patron_resultados_tbl as (
select hd.id, hh.b_type, hh.drawing_id, hh.digit digit_hdr
     , hd.next_drawing_id, hd.history_digit, hd.winner_flag
  from olap_sys.position_digit_history_header hh
     , olap_sys.position_digit_history_dtl hd
 where hd.winner_flag is not null
   and hh.header_id = hd.header_id
   and hh.b_type = hd.b_type
)
, patron_resultados_cnt_tbl as (
select b_type
     , id
     , count(1) cnt
  from patron_resultados_tbl
 where drawing_id >=  (select max_id from resultado_tbl) -100
 group by b_type
     , id 
)
, rango_resultados_tbl as (
select b_type rango_b_type
     , min(id) min_id
     , round(avg(id)) avg_id
  from patron_resultados_cnt_tbl
 group by b_type
)
, patron_output_tbl as (
select p.drawing_id, p.b_type, p.digit_hdr, p.history_digit digit, p.match_cnt
     , nvl(decode(c.color_ley_tercio,-1,'#',1,'R',2,'G',3,'B'),'#') lt
     , case when c.chng_posicion is null then '.' else 'C' end chng
     , case when id between (select min_id from rango_resultados_tbl where rango_b_type=p.b_type) and (select avg_id from rango_resultados_tbl where rango_b_type=p.b_type) then 'Y' else 'N' end jugar_flag
  from patron_posicion_tbl p
     , olap_sys.s_calculo_stats c
 where c.drawing_id(+) = p.drawing_id
   and c.b_type(+) = p.b_type
   and c.digit(+) = p.history_digit
) --select * from patron_output_tbl;
--![[capa de transformaciones]]
, output_tbl as (
select ia1, ia2, ia3, ia4, ia5, ia6
     --!frecuencia
     , decode(fr1,-1,'#',1,'R',2,'G',3,'B') fr1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') fr2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') fr3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') fr4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') fr5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') fr6
     --!ley del tercio
     , decode(lt1,-1,'#',1,'R',2,'G',3,'B') lt1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') lt2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') lt3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') lt4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') lt5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') lt6
     --!ciclo de aparicion
     , avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6
     --!decenas
     , d1, d2, d3, d4, d5, d6
     , caso_id
     --!numeros favorables
     , pf1, pf2, pf3, pf4, pf5, pf6, pf1 + pf2 + pf3 + pf4 + pf5 + pf6 pf_sum
     --!contador de primos, impares, pares
     , pn_cnt, none_cnt, par_cnt
     , t1_cnt, t2_cnt, 0 decena
     , sorteo_actual, patron_6_accum, repetidos_cnt, patron_25_cnt, patron_25_accum
     --!numeros sin cambios
     , chg1, chg2, chg3, chg4, chg5, chg6
     , aciertos_accum, digito_cnt, ia6_cnt, 'N' jugar_flag, ca_sum, comb_sum
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg
     , list_id, 0 terminacion_str, 0 t_cnt
  from olap_sys.gl_automaticas_detail
 where 1=1
   --![[caso_id]]
   --![[caso_id in (1,2,3,4,5): c1=primo, caso_id in (6,7,8,9): c1=par]]
   --and caso_id > 6
   --![[rango de aciertos mas amplio]]
   and aciertos_accum between 6 and 12
   --![[terminaciones]]
   and (t1_cnt, t2_cnt) in ((4,2),(6,0))
   --![[patron de primos-pares-impares]]
   --![[1:primo, 2:par, 3:impar]]
--   and olap_sys.w_common_pkg.get_digit_type(ia1) = 1
--   and olap_sys.w_common_pkg.get_digit_type(ia4) = 2
--   and olap_sys.w_common_pkg.get_digit_type(ia5) = 2
--   and olap_sys.w_common_pkg.get_digit_type(ia6) = 3
   --![[patrones de numeros mas frecuentes]]
--   and digito_cnt > 2
   --![[numeros sin cambio en la posicion]]
   and chg1 = '.'
  -- and chg4 = '.'
   and chg6 = '.'
   --![[3 a 6 numeros pares]]
--   and par_cnt > 2
   --![[numeros repetidos en base al sorteo anterior]]
   and repetidos_cnt < 3   
   --![[numeros favorables]]
--   and ((pf1=1 and pf2=1) or (pf5=1 and pf6=1))    
   --![[informacion de gigaloterias]]
   and IA1 in (select digit from e1_b1_tbl)
   and IA2 in (select digit from e1_b2_tbl)
   and IA3 in (select digit from e1_b3_tbl)
   and IA4 in (select digit from e1_b4_tbl)
   and IA5 in (select digit from e1_b5_tbl)
   and IA6 in (select digit from e1_b6_tbl)
   --![[patron de conteo basado en decenas, digitos y sorteos]]
/*   and IA1 in (select pos from b1_tbl)
--   and IA2 in (select pos from b2_tbl)
   and IA3 in (select pos from b3_tbl)
   and IA4 in (select pos from b4_tbl)
   and IA5 in (select pos from b5_tbl)
   and IA6 in (select pos from b6_tbl)  */
   --<<lista de predicciones por decena>>
   and (D1,D2,D3,D4,D5,D6) in (select pred1, pred2, pred3, pred4, pred5, pred6 from olap_sys.predicciones_all where prediccion_tipo = '10.DECENA' and prediccion_sorteo = (select max_id from resultado_tbl))     
   --![[CA esta por debajo o por arriba del promedio]]
   --and ((avg_ca_flag1, avg_ca_flag2) in (('B','B')) or (avg_ca_flag5, avg_ca_flag6) in (('B','B')))
--   and avg_ca_flag1 = 'B'
--   and avg_ca_flag6 = 'B'
   --![[patron de posicion 2 al 5]]
   and patron_25_cnt in (3,4)
--   and IA1 = 3
)
select distinct ia1, ia2, ia3, ia4, ia5, ia6, 
                 fr1, fr2, fr3, fr4, fr5, fr6, 
                 lt1, lt2, lt3, lt4, lt5, lt6, 
                 avg_ca_flag1, avg_ca_flag2, avg_ca_flag3, avg_ca_flag4, avg_ca_flag5, avg_ca_flag6, 
                 d1, d2, d3, d4, d5, d6, caso_id,
                 pf1, pf2, pf3, pf4, pf5, pf6, 
                 pn_cnt, none_cnt, par_cnt, t1_cnt, t2_cnt, 
                 decena, sorteo_actual, patron_6_accum, repetidos_cnt, 
                 patron_25_cnt, patron_25_accum, 
                 chg1, chg2, chg3, chg4, chg5, chg6, 
                 aciertos_accum, digito_cnt, ia6_cnt, jugar_flag, ca_sum, comb_sum, t_cnt
  from output_tbl
 where 1=1
   --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
   and sum_chg in (0,1,2,3)
   --!solo jugadas con 1 o 2 numeros repetidos
   --and repetidos_cnt in (1,2) 
   --![[numeros favorables]]
--   and pf_sum in (2,3,4)     
   --!lt summary
--   and lt1 in (select lt from lt_summary_tbl where b_type_end = 'B1')   
--   and lt3 in (select lt from lt_summary_tbl where b_type_end = 'B3')
--   and lt6 in (select lt from lt_summary_tbl where b_type_end = 'B6')   
   --!solo jugadas que esten el patron de ley del tercio
   --!esta condicion es sustituida por lt_summary de arriba
--   and (lt1, lt2, lt3, lt4, lt5, lt6) in (select lt1, lt2, lt3, lt4, lt5, lt6 from lt_details_tbl)   
 order by ia1, ia2, ia3, ia4, ia5, ia6 --repetidos_cnt desc, digito_cnt desc, ia6_cnt desc, 
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