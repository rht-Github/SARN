rem purpose      : mixing data between pm gigaloterias stats 
rem creation_date: 05/08/2019

create or replace view olap_sys.pm_mr_resultados_v2 as
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
     , olap_sys.w_common_pkg.get_dozen_sort(g.COMB1,g.COMB2,g.COMB3,g.COMB4,g.COMB5,g.COMB6) DS
     , olap_sys.w_common_pkg.get_dozen_rank(g.COMB1,g.COMB2,g.COMB3,g.COMB4,g.COMB5,g.COMB6) DR
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
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null) cu1
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null) cu2
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null) cu3
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null) cu4
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null) cu5
     , (select decode(gs.color_ubicacion,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null) cu6
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null) clt1
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null) clt2
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null) clt3
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null) clt4
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null) clt5
     , (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null) clt6
     , (select lt.seq_no_percentage from olap_sys.s_gl_ley_tercio_patterns lt where lt.lt1= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#') 
           and lt.lt2= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#') 
           and lt.lt3= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#') 
           and lt.lt4= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#') 
           and lt.lt5= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#') 
           and lt.lt6= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) lt_seq_no_pct
     , (select lt.seq_no from olap_sys.s_gl_ley_tercio_patterns lt where lt.lt1= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#') 
           and lt.lt2= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#') 
           and lt.lt3= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#') 
           and lt.lt4= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#') 
           and lt.lt5= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#') 
           and lt.lt6= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) lt_seq_no
     , (select lt.drawing_case from olap_sys.s_gl_ley_tercio_patterns lt where lt.lt1= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#') 
           and lt.lt2= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#') 
           and lt.lt3= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#') 
           and lt.lt4= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#') 
           and lt.lt5= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#') 
           and lt.lt6= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) dcase
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
     , (select rank_cnt from olap_sys.s_gl_ley_tercio_patterns where lt1= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'#') 
           and lt2= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'#') 
           and lt3= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'#') 
           and lt4= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'#') 
           and lt5= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'#') 
           and lt6= nvl((select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'#')) rank_cnt                   		  
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null) c1_ca
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null) c2_ca
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null) c3_ca
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null) c4_ca
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null) c5_ca
     , (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null) c6_ca
     , nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0)
     + nvl((select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0) sum_ca	 
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null) pxc1
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null) pxc2
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null) pxc3
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null) pxc4
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null) pxc5
     , (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null) pxc6
     , decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),0),0,0,1) 
     + decode(nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),0),0,0,1) pxc_cnt	 
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null) pre1
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null) pre2
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null) pre3
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null) pre4
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null) pre5
     , (select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null) pre6     
     , nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'X',1),0)
     + nvl(replace((select upper(preferencia_flag) from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'X',1),0) precnt	     
	 /*, (select case when gs.rango_ley_tercio between 1 and 3 then 'H' when gs.rango_ley_tercio between 4 and 6 then 'M' when gs.rango_ley_tercio >= 7 then 'L' end  from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r1
     , (select case when gs.rango_ley_tercio between 1 and 3 then 'H' when gs.rango_ley_tercio between 4 and 6 then 'M' when gs.rango_ley_tercio >= 7 then 'L' end from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r2
     , (select case when gs.rango_ley_tercio between 1 and 5 then 'H' when gs.rango_ley_tercio between 6 and 10 then 'M' when gs.rango_ley_tercio >= 11 then 'L' end from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r3
     , (select case when gs.rango_ley_tercio between 1 and 6 then 'H' when gs.rango_ley_tercio between 7 and 12 then 'M' when gs.rango_ley_tercio >= 13 then 'L' end from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r4
     , (select case when gs.rango_ley_tercio between 1 and 5 then 'H' when gs.rango_ley_tercio between 6 and 10 then 'M' when gs.rango_ley_tercio >= 11 then 'L' end from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r5
     , (select case when gs.rango_ley_tercio between 1 and 3 then 'H' when gs.rango_ley_tercio between 4 and 6 then 'M' when gs.rango_ley_tercio >= 7 then 'L' end from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) r6*/	 	 
     , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos1
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi1
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt1
	 , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca1
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc1
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B1' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag1
	 , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos2
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi2
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt2
     , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca2
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc2
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B2' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag2	 
     , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos3
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi3
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt3
     , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca3
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc3
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B3' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag3	 
     , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos4
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi4
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt4
     , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca4
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc4
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B4' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag4	 
     , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos5
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi5
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt5
     , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca5
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc5
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B5' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag5	 
     , (select gs.chng_posicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pos6
     , (select gs.chng_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ubi6
     , (select gs.chng_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_lt6     
     , (select gs.chng_ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_ca6
     , (select gs.chng_pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_pxc6
     , (select gs.chng_preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.b_type='B6' and gs.rango_ley_tercio is not null and gs.winner_flag is not null) chng_flag6
     , g.diferencia_tipo_info dtipo_info
	 , cr.seq_no comb_seq_no
	 , cr.seq_no_percentage comb_seq_no_pct
	 , g.COMB_SUM
	 , cr.term_cnt
	 , cr.seq_id
	 , olap_sys.w_common_pkg.gl_get_nf_group(
       nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null),'.')
     , nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null),'.')
     , nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null),'.')
     , nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null),'.')
     , nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null),'.')
     , nvl((select gs.preferencia_flag from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null),'.')     
	 ) nf_group 
	 , case when cr.PN_CNT = 2 then olap_sys.w_common_pkg.pm_filtrar_pareja_primos(g.COMB1,g.COMB2,g.COMB3,g.COMB4,g.COMB5,g.COMB6) else null end below_avg
     , case when olap_sys.w_common_pkg.is_prime_number(g.comb1) = 1 then 'PR' else 
       case when mod(g.comb1,2) = 0 then 'PA' else 
	   case when mod(g.comb1,2) > 0 then 'IN' end end end
     ||'-'|| case when olap_sys.w_common_pkg.is_prime_number(g.comb6) = 1 then 'PR' else
             case when mod(g.comb6,2) = 0 then 'PA' else 
			 case when mod(g.comb6,2) > 0 then 'IN' end end end	c1_c6_type 	 
	 , olap_sys.w_common_pkg.get_mapa_numeros_primos(g.gambling_id, g.comb1, g.comb2, g.comb3, g.comb4, g.comb5, g.comb6) mapa_primos	
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
/

show errors;