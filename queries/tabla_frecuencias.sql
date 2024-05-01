with resultado_tbl as (
select max(gambling_id) max_id from olap_sys.sl_gamblings
) 
, gl_tbl as (
select drawing_id id, b_type, digit, color_ubicacion cfr, ubicacion vfr, color_ley_tercio clt, ciclo_aparicion ca, nvl(pronos_ciclo,0) pxc,  nvl(preferencia_flag,'.') pre, case when CHNG_POSICION is null then '.' else 'X' end chg
  from olap_sys.s_calculo_stats
 where drawing_id = (select max_id from resultado_tbl) 
)
, ley_tercio_pattern_tbl as (
select lt1, lt2, lt3, lt4, lt5, lt6
  from olap_sys.s_gl_ley_tercio_patterns 
 where last_drawing_id = (select max_id from resultado_tbl)
   and null_cnt  = 0 
   and red_cnt   < 3 
   and match_cnt in (0,1)
)
--!escenario1 
, e1_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1)   
   and clt = 2
)
, e1_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and cfr = 3
)
, e1_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3'
   and cfr = 3
   and clt in (2,3)
)
, e1_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
)
, e1_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and clt in (2,3)
)
, e1_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
--!escenario2 
, e2_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1) 
)
, e2_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and cfr = 3
   and clt in (2,3)
)
, e2_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3'
   and clt in (2,3)
)
, e2_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
   and cfr = 1
)
, e2_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and clt in (2,3)
)
, e2_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
--!escenario3 
, e3_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1) 
)
, e3_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and cfr = 1
   and clt in (2,3)
)
, e3_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3'
   and cfr = 2
   and clt in (2,3) 
)
, e3_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
)
, e3_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and clt in (2,3)
)
, e3_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
--!escenario4 
, e4_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1) 
)
, e4_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and clt in (2,3)
)
, e4_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3'
   and cfr = 2
   and clt in (2,3) 
)
, e4_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
   and cfr = 1 
   and clt in (2,3)
)
, e4_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and clt in (2,3)
)
, e4_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
--!escenario5 
, e5_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1) 
)
, e5_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and cfr = 2
   and clt in (2,3)
)
, e5_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3' 
)
, e5_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
   and cfr = 1
   and clt in (2,3)
)
, e5_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and clt in (2,3)
)
, e5_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
--!escenario6 
, e6_b1_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B1'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr1,0,cfr,1) 
)
, e6_b2_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B2'
   and clt in (2,3)
)
, e6_b3_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B3'
   and clt in (2,3) 
)
, e6_b4_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B4'
   and cfr = 1
   and clt in (2,3)
)
, e6_b5_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B5'
   and cfr = 1
   and clt in (2,3) 
)
, e6_b6_tbl as (
select id, b_type, digit, cfr, vfr, clt, ca, pxc,  pre
  from gl_tbl
 where b_type = 'B6'
   --!0 quita el filtro. default 1
   and cfr = decode(:pn_fr6,0,cfr,1)-- 1
)
, output_tbl as (
--!escenario1
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e1' escenario
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e1_b1_tbl)
   and IA2 in (select digit from e1_b2_tbl)
   and IA3 in (select digit from e1_b3_tbl)
   and IA4 in (select digit from e1_b4_tbl)
   and IA5 in (select digit from e1_b5_tbl)
   and IA6 in (select digit from e1_b6_tbl)
   and list_id = :pn_list_id
union
--!escenario2
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e2' escenario
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e2_b1_tbl)
   and IA2 in (select digit from e2_b2_tbl) 
   and IA3 in (select digit from e2_b3_tbl) 
   and IA4 in (select digit from e2_b4_tbl) 
   and IA5 in (select digit from e2_b5_tbl) 
   and IA6 in (select digit from e2_b6_tbl)
   and list_id = :pn_list_id
union
--!escenario3
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e3' escenario
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e3_b1_tbl)
   and IA2 in (select digit from e3_b2_tbl) 
   and IA3 in (select digit from e3_b3_tbl) 
   and IA4 in (select digit from e3_b4_tbl) 
   and IA5 in (select digit from e3_b5_tbl) 
   and IA6 in (select digit from e3_b6_tbl)
   and list_id = :pn_list_id
union
--!escenario4
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e4' escenario
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e4_b1_tbl)
   and IA2 in (select digit from e4_b2_tbl) 
   and IA3 in (select digit from e4_b3_tbl) 
   and IA4 in (select digit from e4_b4_tbl) 
   and IA5 in (select digit from e4_b5_tbl) 
   and IA6 in (select digit from e4_b6_tbl)
   and list_id = :pn_list_id
union
--!escenario5
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e5' escenario
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e5_b1_tbl)
   and IA2 in (select digit from e5_b2_tbl) 
   and IA3 in (select digit from e5_b3_tbl) 
   and IA4 in (select digit from e5_b4_tbl) 
   and IA5 in (select digit from e5_b5_tbl) 
   and IA6 in (select digit from e5_b6_tbl)   
   and list_id = :pn_list_id
union
--!escenario6
select ia1, ia2, ia3, ia4, ia5, ia6, decode(fr1,-1,'#',1,'R',2,'G',3,'B') f1, decode(fr2,-1,'#',1,'R',2,'G',3,'B') f2, decode(fr3,-1,'#',1,'R',2,'G',3,'B') f3, decode(fr4,-1,'#',1,'R',2,'G',3,'B') f4, decode(fr5,-1,'#',1,'R',2,'G',3,'B') f5, decode(fr6,-1,'#',1,'R',2,'G',3,'B') f6, decode(lt1,-1,'#',1,'R',2,'G',3,'B') t1, decode(lt2,-1,'#',1,'R',2,'G',3,'B') t2, decode(lt3,-1,'#',1,'R',2,'G',3,'B') t3, decode(lt4,-1,'#',1,'R',2,'G',3,'B') t4, decode(lt5,-1,'#',1,'R',2,'G',3,'B') t5, decode(lt6,-1,'#',1,'R',2,'G',3,'B') t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, 0 consecutivos_cnt, t2_cnt terminacion_str, 0 decena, sorteo_actual, 0 rcnt, 0 b1_b4_b6_flag, repetidos_cnt, 0 c1_c6_flag, 0 mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, 'N' jugar_flag, ca1+ca2+ca3+ca4+ca5+ca6 ca_sum, ia1+ia2+ia3+ia4+ia5+ia6 comb_sum, 'e6' escenario    
     , case when chg1 = '.' then 0 else 1 end + case when chg2 = '.' then 0 else 1 end + case when chg3 = '.' then 0 else 1 end + case when chg4 = '.' then 0 else 1 end + case when chg5 = '.' then 0 else 1 end + case when chg6 = '.' then 0 else 1 end sum_chg, list_id, t2_cnt
  from olap_sys.gl_automaticas_detail 
 where IA1 in (select digit from e6_b1_tbl)
   and IA2 in (select digit from e6_b2_tbl) 
   and IA3 in (select digit from e6_b3_tbl) 
   and IA4 in (select digit from e6_b4_tbl) 
   and IA5 in (select digit from e6_b5_tbl) 
   and IA6 in (select digit from e6_b6_tbl)  
   and list_id = :pn_list_id
)
select distinct ia1, ia2, ia3, ia4, ia5, ia6, f1, f2, f3, f4, f5, f6, t1, t2, t3, t4, t5, t6, ca1, ca2, ca3, ca4, ca5, ca6, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pf1, pf2, pf3, pf4, pf5, pf6, pn_cnt, none_cnt, par_cnt, consecutivos_cnt, terminacion_str, decena, sorteo_actual, rcnt, b1_b4_b6_flag, repetidos_cnt, c1_c6_flag, mapa_primos, chg1, chg2, chg3, chg4, chg5, chg6, aciertos_accum, incidencia, incidencia_cnt, jugar_flag, ca_sum, comb_sum, list_id
  from output_tbl
 where 1=1
   --!filtrar jugadas cuyos digitos no han tenido cambios en su posicion
   and sum_chg in (0,1)
   --!solo jugadas con 1 o 2 numeros repetidos
   and repetidos_cnt in (1,2)
   --!solo ocupar jugadas que ya han tenido aciertos anteriormente
   and aciertos_accum > 0
   --!solo jugadas que esten el patron de ley del tercio
   and (t1, t2, t3, t4, t5, t6) in (select lt1, lt2, lt3, lt4, lt5, lt6 from ley_tercio_pattern_tbl)
   --!solo jugadas que tengan una repeticion
   and t2_cnt = 2
 order by ia1, ia2, ia3, ia4, ia5, ia6;