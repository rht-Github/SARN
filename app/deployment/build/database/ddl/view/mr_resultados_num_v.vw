rem purpose      : mixing data between pm gigaloterias stats 
rem creation_date: 01/11/2024

create or replace view olap_sys.mr_resultados_num_v as
select to_char(to_date(g.GAMBLING_DATE,'DD-MM-YYYY'),'YYYY') YEAR
     , to_char(to_date(g.GAMBLING_DATE,'DD-MM-YYYY'),'Q') QTR
	 , to_char(to_date(g.GAMBLING_DATE,'DD-MM-YYYY'),'DY') DAY
     , g.GAMBLING_DATE
     , g.GAMBLING_ID
     , g.COMB1
     , g.COMB2
     , g.COMB3
     , g.COMB4
     , g.COMB5
     , g.COMB6
	 , g.ADDITIONAL
     , PN_NONE_CNT  
     , PN_PAR_CNT  
     , cr.MOD3_SUM XMOD
     , (select count(1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.winner_flag is not null) gl_cnt
     , decode(cr.PN1,1,g.COMB1,0)PN1 
     , decode(cr.PN2,1,g.COMB2,0)PN2
     , decode(cr.PN3,1,g.COMB3,0)PN3
     , decode(cr.PN4,1,g.COMB4,0)PN4
     , decode(cr.PN5,1,g.COMB5,0)PN5
     , decode(cr.PN6,1,g.COMB6,0)PN6	 
     , cr.PN_CNT
     , decode(cr.NONE1,1,g.COMB1,0)NONE1
     , decode(cr.NONE2,1,g.COMB2,0)NONE2
     , decode(cr.NONE3,1,g.COMB3,0)NONE3
     , decode(cr.NONE4,1,g.COMB4,0)NONE4
     , decode(cr.NONE5,1,g.COMB5,0)NONE5
     , decode(cr.NONE6,1,g.COMB6,0)NONE6
     , cr.NONE_CNT
     , decode(cr.PAR1,1,g.COMB1,0)PAR1
     , decode(cr.PAR2,1,g.COMB2,0)PAR2
     , decode(cr.PAR3,1,g.COMB3,0)PAR3
     , decode(cr.PAR4,1,g.COMB4,0)PAR4
     , decode(cr.PAR5,1,g.COMB5,0)PAR5
     , decode(cr.PAR6,1,g.COMB6,0)PAR6
     , cr.PAR_CNT
     , decode(cr.M3_1,1,g.COMB1,0)M3_1
     , decode(cr.M3_2,1,g.COMB2,0)M3_2
     , decode(cr.M3_3,1,g.COMB3,0)M3_3
     , decode(cr.M3_4,1,g.COMB4,0)M3_4
     , decode(cr.M3_5,1,g.COMB5,0)M3_5
     , decode(cr.M3_6,1,g.COMB6,0)M3_6
     , cr.M3_CNT
     , decode(cr.M4_1,1,g.COMB1,0)M4_1
     , decode(cr.M4_2,1,g.COMB2,0)M4_2
     , decode(cr.M4_3,1,g.COMB3,0)M4_3
     , decode(cr.M4_4,1,g.COMB4,0)M4_4
     , decode(cr.M4_5,1,g.COMB5,0)M4_5
     , decode(cr.M4_6,1,g.COMB6,0)M4_6
     , cr.M4_CNT
     , decode(cr.M5_1,1,g.COMB1,0)M5_1
     , decode(cr.M5_2,1,g.COMB2,0)M5_2
     , decode(cr.M5_3,1,g.COMB3,0)M5_3
     , decode(cr.M5_4,1,g.COMB4,0)M5_4
     , decode(cr.M5_5,1,g.COMB5,0)M5_5
     , decode(cr.M5_6,1,g.COMB6,0)M5_6
     , cr.M5_CNT
     , decode(cr.M7_1,1,g.COMB1,0)M7_1
     , decode(cr.M7_2,1,g.COMB2,0)M7_2
     , decode(cr.M7_3,1,g.COMB3,0)M7_3
     , decode(cr.M7_4,1,g.COMB4,0)M7_4
     , decode(cr.M7_5,1,g.COMB5,0)M7_5
     , decode(cr.M7_6,1,g.COMB6,0)M7_6
     , cr.M7_CNT     
     , cr.DIST_C1_C2-1 DIST_C1_C2 
     , cr.DIST_C1_C3-1 DIST_C1_C3
     , cr.DIST_C1_C4-1 DIST_C1_C4 
     , cr.DIST_C1_C5-1 DIST_C1_C5 
     , cr.DIST_C1_C6-1 DIST_C1_C6
     , cr.D1
     , cr.D2
     , cr.D3
     , cr.D4
     , cr.D5
     , cr.D6
     , nvl(olap_sys.w_common_pkg.get_dozen_sort(g.COMB1,g.COMB2,g.COMB3,g.COMB4,g.COMB5,g.COMB6),0) DS
     , nvl(olap_sys.w_common_pkg.get_dozen_rank(g.COMB1,g.COMB2,g.COMB3,g.COMB4,g.COMB5,g.COMB6),0) DR
	 , cr.d01_09
	 , cr.d10_19
	 , cr.d20_29
	 , cr.d30_39
     , cr.T1
     , cr.T2
     , cr.T3
     , cr.T4
     , cr.T5
     , cr.T6
     , cr.T7
     , cr.T8
     , cr.T9
     , cr.T0
	 , olap_sys.w_common_pkg.termination_counter(cr.T1||','||cr.T2||','||cr.T3||','||cr.T4||','||cr.T5||','||cr.T6||','||cr.T7||','||cr.T8||','||cr.T9||','||cr.T0,1) term1_cnt
     , olap_sys.w_common_pkg.termination_counter(cr.T1||','||cr.T2||','||cr.T3||','||cr.T4||','||cr.T5||','||cr.T6||','||cr.T7||','||cr.T8||','||cr.T9||','||cr.T0,2) term2_cnt
     , cr.CO_1
     , cr.CO_2
     , cr.CO_3
     , cr.CO_4
     , cr.CO_5
     , cr.CO_CNT
	 , g.REP_COMB1
	 , g.REP_COMB2
	 , g.REP_COMB3
	 , g.REP_COMB4
	 , g.REP_COMB5
	 , g.REP_COMB6
	 , g.REP_COMB1
	 + g.REP_COMB2
	 + g.REP_COMB3
	 + g.REP_COMB4
	 + g.REP_COMB5
	 + g.REP_COMB6 REP_CNT	 
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0) cu1
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0) cu2
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0) cu3
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0) cu4
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0) cu5
     , nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) cu6
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0) clt1
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0) clt2
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0) clt3
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0) clt4
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0) clt5
     , nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) clt6
     , (select lt.null_cnt from olap_sys.s_gl_ley_tercio_patterns lt where lt.drawing_type=g.gambling_type 
          and lt.lt1 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#')
          and lt.lt2 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#')
          and lt.lt3 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#')
          and lt.lt4 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#')
          and lt.lt5 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#')
          and lt.lt6 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) null_cnt
     , (select lt.red_cnt from olap_sys.s_gl_ley_tercio_patterns lt where lt.drawing_type=g.gambling_type 
          and lt.lt1 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#')
          and lt.lt2 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#')
          and lt.lt3 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#')
          and lt.lt4 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#')
          and lt.lt5 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#')
          and lt.lt6 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) red_cnt
     , (select lt.green_cnt from olap_sys.s_gl_ley_tercio_patterns lt where lt.drawing_type=g.gambling_type 
          and lt.lt1 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#')
          and lt.lt2 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#')
          and lt.lt3 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#')
          and lt.lt4 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#')
          and lt.lt5 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#')
          and lt.lt6 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) green_cnt
     , (select lt.blue_cnt from olap_sys.s_gl_ley_tercio_patterns lt where lt.drawing_type=g.gambling_type 
          and lt.lt1 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#')
          and lt.lt2 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#')
          and lt.lt3 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#')
          and lt.lt4 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#')
          and lt.lt5 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#')
          and lt.lt6 = nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) blue_cnt         	                  		  
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0) c1_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0) c2_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0) c3_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0) c4_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0) c5_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) c6_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) sum_ca	 
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0) pxc1
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0) pxc2
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0) pxc3
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0) pxc4
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0) pxc5
     , nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) pxc6
     , decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0),0,0,1) pxc_cnt	 
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0) pre1
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0) pre2
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0) pre3
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0) pre4
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0) pre5
     , nvl((select decode(gs.preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) pre6     
     , nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'X',1),0) precnt	     	 
     , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos1
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi1
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt1
	 , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca1
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc1
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag1
	 , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos2
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi2
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt2
     , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca2
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc2
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag2	 
     , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos3
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi3
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt3
     , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca3
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc3
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag3	 
     , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos4
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi4
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt4
     , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca4
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc4
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag4	 
     , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos5
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi5
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt5
     , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca5
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc5
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag5	 
     , nvl((select decode(gs.chng_posicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pos6
     , nvl((select decode(gs.chng_ubicacion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ubi6
     , nvl((select decode(gs.chng_ley_tercio,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_lt6     
     , nvl((select decode(gs.chng_ciclo_aparicion,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_ca6
     , nvl((select decode(gs.chng_pronos_ciclo,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_pxc6
     , nvl((select decode(gs.chng_preferencia_flag,null,0,1) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null),0) chng_flag6
	 , g.COMB_SUM
	 , cr.term_cnt
     , case when olap_sys.w_common_pkg.is_prime_number(g.comb1) = 1 then 'PR' else 
       case when mod(g.comb1,2) = 0 then 'PA' else 
	   case when mod(g.comb1,2) > 0 then 'IN' end end end
     ||'-'|| case when olap_sys.w_common_pkg.is_prime_number(g.comb6) = 1 then 'PR' else
             case when mod(g.comb6,2) = 0 then 'PA' else 
			 case when mod(g.comb6,2) > 0 then 'IN' end end end	c1_c6_type 	
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),-1) pre_n1
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),-1) pre_n2
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),-1) pre_n3
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),-1) pre_n4
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),-1) pre_n5
     , nvl((select gs.preferencia_num from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),-1) pre_n6	
  from olap_sys.sl_gamblings g
     , olap_sys.w_combination_responses_fs cr
 where g.gambling_type = cr.attribute3
   and g.seq_id = cr.seq_id
   and g.GAMBLING_ID > 594
/

show errors; 
/

show errors;