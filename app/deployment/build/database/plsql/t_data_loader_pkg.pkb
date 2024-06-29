create or replace package body olap_sys.t_data_loader_pkg as

  g$GambTbl             typ_gamb;
  gv$BINGO     CONSTANT VARCHAR2(1) := 'Y';
  GV$SORTEO_PREVIO CONSTANT VARCHAR2(4) := 'PREV';
  GV$SORTEO_ACTUAL CONSTANT VARCHAR2(4) := 'CURR';
  gn$err_code           			number := 0;
  gv$err_msg            			varchar2(4000);
  gn$array_index                 	number := 0;
  gn$prev_gambling_id            	number := 0;
  gn$curr_gambling_id            	number := 0;
  gn$chng_posicion_cnt				number := 0;
  gn$chng_ubicacion_cnt 			number := 0;
  gn$chng_ley_tercio_cnt 			number := 0;
  gn$chng_ciclo_aparicion_cnt 		number := 0;
  gn$chng_pronos_ciclo_cnt 			number := 0;
  gn$chng_preferencia_flag_cnt		number := 0;
  gv$chng_posicion_stmt				varchar2(200);
  gv$chng_ubicacion_stmt 			varchar2(200);
  gv$chng_ley_tercio_stmt 			varchar2(200);
  gv$chng_ciclo_aparicion_stmt 		varchar2(200);
  gv$chng_pronos_ciclo_stmt 		varchar2(200);
  gv$chng_preferencia_flag_stmt		varchar2(200);
  gv$chng_cambios_stmt			varchar2(200);
  
  procedure upd_s_comb_sum_min_max_stats (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                        , pn_comb_sum                   olap_sys.sl_gamblings.comb_sum%type
                                        , pn_num_drawings               number default null
                                        , x_err_code      in out NOCOPY number) is
  LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_s_comb_sum_min_max_stats';
  
  cursor c_comb_sum (pv_gambling_type  	           olap_sys.sl_gamblings.gambling_type%type
                   , pn_comb_sum                   olap_sys.sl_gamblings.comb_sum%type
                   , pn_num_drawings               number) is
  with vndr_comb_sum_stats_tbl as (
  select distinct attribute3 drawing_type
       , comb_sum
    from olap_sys.w_combination_responses_fs
   where attribute3 = pv_gambling_type
  ) 
  select drawing_type
       , comb_sum
       , (select count(1)
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum 
           ) total_cnt 
       , (select count(1)
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum 
           and gambling_id >= olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type)
           ) l2y_cnt            
       , (select max(trunc(SYSDATE-to_date(gambling_date,'DD-MM-YYYY')))
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum
           and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))           
           ) max_days_ago
       , (select gambling_id
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum
           and trunc(sysdate-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = (select max(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
                                                                       from olap_sys.sl_gamblings
                                                                      where gambling_type = cs.drawing_type 
                                                                        and comb_sum = cs.comb_sum
                                                                        and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
                                                                        )) max_id     
       , (select to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum
           and trunc(sysdate-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = (select max(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
                                                                       from olap_sys.sl_gamblings
                                                                      where gambling_type = cs.drawing_type 
                                                                        and comb_sum = cs.comb_sum
                                                                        and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
                                                                        )) max_drawing_date                                                                             
     , (select min(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum 
           and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
           ) min_days_ago
     , (select gambling_id
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum
           and trunc(sysdate-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = (select min(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
                                                                       from olap_sys.sl_gamblings
                                                                      where gambling_type = cs.drawing_type 
                                                                        and comb_sum = cs.comb_sum 
                                                                        and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
                                                                      )) min_id   
     , (select to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)
          from olap_sys.sl_gamblings
         where gambling_type = cs.drawing_type 
           and comb_sum = cs.comb_sum
           and trunc(sysdate-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = (select min(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
                                                                       from olap_sys.sl_gamblings
                                                                      where gambling_type = cs.drawing_type 
                                                                        and comb_sum = cs.comb_sum 
                                                                        and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
                                                                      )) min_drawing_date  
     , (select max(gambling_id)
          from olap_sys.sl_gamblings
         where gambling_type = pv_gambling_type) current_drawing_id                                                                  
     , nvl(((select max(gambling_id)
          from olap_sys.sl_gamblings
         where gambling_type = pv_gambling_type)-(select gambling_id
                                          from olap_sys.sl_gamblings
                                         where gambling_type = cs.drawing_type 
                                           and comb_sum = cs.comb_sum
                                           and trunc(sysdate-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = (select min(trunc(SYSDATE-to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)))
                                                                                                       from olap_sys.sl_gamblings
                                                                                                      where gambling_type = cs.drawing_type 
                                                                                                        and comb_sum = cs.comb_sum 
                                                                                                        and gambling_id >= decode(pn_num_drawings,0,0,null,olap_sys.w_common_pkg.get_gigamelate_range (pv_drawing_type=>pv_gambling_type),(select max(sg.gambling_id)-nvl(pn_num_drawings,20) from olap_sys.sl_gamblings sg))
                                                                                                    )
                                        )
       ),0) drawings_ago
  from vndr_comb_sum_stats_tbl cs
   where comb_sum = pn_comb_sum
 order by drawings_ago 
     , comb_sum 
;
begin
   dbms_output.put_line('------------------------------------------');
   dbms_output.put_line(LV$PROCEDURE_NAME);
   dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
   dbms_output.put_line('pn_comb_sum: '||pn_comb_sum);
   dbms_output.put_line('pn_num_drawings: '||pn_num_drawings);
   
   for k in c_comb_sum (pv_gambling_type => pv_gambling_type
                      , pn_comb_sum      => pn_comb_sum     
                      , pn_num_drawings  => pn_num_drawings) loop
       update olap_sys.s_comb_sum_min_max_stats
          set l2y_cnt            = k.l2y_cnt
            , max_days_ago       = k.max_days_ago
            , max_id             = k.max_id
            , max_drawing_date   = k.max_drawing_date
            , min_days_ago       = k.min_days_ago
            , min_id             = k.min_id
            , min_drawing_date   = k.min_drawing_date
            , current_drawing_id = k.current_drawing_id
            , drawings_ago       = k.drawings_ago
            , update_cnt         = nvl(update_cnt,0)+1
            , updated_by         = user
            , updated_date       = sysdate
        where drawing_type       = k.drawing_type
          and comb_sum           = k.comb_sum;      
      
      dbms_output.put_line(sql%rowcount||' rows updated.');
   end loop;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;    
  end upd_s_comb_sum_min_max_stats;

  procedure upd_s_average_patterns (pv_gambling_type               olap_sys.s_average_patterns.drawing_type%type
                                  , pn_out_comb_sum                olap_sys.s_average_patterns.out_comb_sum%type
                                  , pn_out_1                       olap_sys.s_average_patterns.out_1%type
                                  , pn_out_2                       olap_sys.s_average_patterns.out_2%type
                                  , pn_out_3                       olap_sys.s_average_patterns.out_3%type
                                  , pn_out_4                       olap_sys.s_average_patterns.out_4%type
                                  , pn_out_5                       olap_sys.s_average_patterns.out_5%type
                                  , pn_out_6                       olap_sys.s_average_patterns.out_6%type
                                  , pn_in_last_year                olap_sys.s_average_patterns.in_last_year%type
                                  , pn_in_last_qtr                 olap_sys.s_average_patterns.in_last_qtr%type
                                  , pn_in_last_month               olap_sys.s_average_patterns.in_last_month%type
                                  , pn_avg_pattern_cnt             olap_sys.s_average_patterns.avg_pattern_cnt%type                                  
                                  , x_err_code       in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_s_average_patterns';
  begin
     update olap_sys.s_average_patterns
        set in_cnt        = in_cnt + 1
          , in_last_year  = pn_in_last_year 
          , in_last_qtr   = pn_in_last_qtr  
          , in_last_month = pn_in_last_month
          , updated_by    = user
          , updated_date  = sysdate
      where drawing_type     = pv_gambling_type
        and out_1            = pn_out_1     
        and out_2            = pn_out_2
        and out_3            = pn_out_3
        and out_4            = pn_out_4
        and out_5            = pn_out_5
        and out_6            = pn_out_6
        and out_comb_sum     = pn_out_comb_sum     
        and avg_pattern_cnt  = pn_avg_pattern_cnt;
        
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;   
     dbms_output.put_line(sql%rowcount||' rows updated on olap_sys.s_average_patterns');   
        
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end upd_s_average_patterns;
  
  procedure compute_inbound_values (pv_gambling_type  	               olap_sys.sl_gamblings.gambling_type%type
                                  , pv_gambling_date                   olap_sys.sl_gamblings.gambling_date%type
                                  , pn_gambling_id                     olap_sys.sl_gamblings.gambling_id%type
                                  , pn_comb1                           olap_sys.sl_gamblings.comb1%type
                                  , pn_comb2                           olap_sys.sl_gamblings.comb2%type
                                  , pn_comb3                           olap_sys.sl_gamblings.comb3%type
                                  , pn_comb4                           olap_sys.sl_gamblings.comb4%type
                                  , pn_comb5                           olap_sys.sl_gamblings.comb5%type
                                  , pn_comb6                           olap_sys.sl_gamblings.comb6%type
                                  , xn_out_comb_sum      in out NOCOPY olap_sys.sl_gamblings.comb6%type
                                  , xn_out_1             in out NOCOPY number
                                  , xn_out_2             in out NOCOPY number
                                  , xn_out_3             in out NOCOPY number
                                  , xn_out_4             in out NOCOPY number
                                  , xn_out_5             in out NOCOPY number
                                  , xn_out_6             in out NOCOPY number
                                  , xn_year              in out NOCOPY number
                                  , xn_quarter           in out NOCOPY number
                                  , xn_month             in out NOCOPY number
                                  , xn_avg_pattern_cnt   in out NOCOPY number                                  
                                  , x_err_code           in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'compute_inbound_values';
                                        
  begin

     xn_out_comb_sum     := pn_comb1+pn_comb2+pn_comb3+pn_comb4+pn_comb5+pn_comb6;
     if pn_comb1 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb1') then xn_out_1 := 1; else xn_out_1 := 0; end if;
     if pn_comb2 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb2') then xn_out_2 := 1; else xn_out_2 := 0; end if;
     if pn_comb3 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb3') then xn_out_3 := 1; else xn_out_3 := 0; end if;
     if pn_comb4 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb4') then xn_out_4 := 1; else xn_out_4 := 0; end if;
     if pn_comb5 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb5') then xn_out_5 := 1; else xn_out_5 := 0; end if;
     if pn_comb6 >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb6') then xn_out_6 := 1; else xn_out_6 := 0; end if;
     if xn_out_comb_sum >= olap_sys.w_common_pkg.get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb_sum') then xn_out_comb_sum := 1; else xn_out_comb_sum := 0; end if;
     xn_year         := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_year_date_format));
     xn_quarter      := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_quarter_date_format));
     xn_month        := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format));
     xn_avg_pattern_cnt := olap_sys.w_common_pkg.get_average_pattern_count(pv_gambling_type,pn_comb1,pn_comb2,pn_comb3,pn_comb4,pn_comb5,pn_comb6);                                                                    

     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;             
  end compute_inbound_values;
     
  procedure average_patterns_handler (pv_gambling_type  	     olap_sys.sl_gamblings.gambling_type%type
                                    , pv_gambling_date               olap_sys.sl_gamblings.gambling_date%type
                                    , pn_gambling_id                 olap_sys.sl_gamblings.gambling_id%type
                                    , pn_comb1                       olap_sys.sl_gamblings.comb1%type
                                    , pn_comb2                       olap_sys.sl_gamblings.comb2%type
                                    , pn_comb3                       olap_sys.sl_gamblings.comb3%type
                                    , pn_comb4                       olap_sys.sl_gamblings.comb4%type
                                    , pn_comb5                       olap_sys.sl_gamblings.comb5%type
                                    , pn_comb6                       olap_sys.sl_gamblings.comb6%type
                                    , x_err_code       in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'average_patterns_handler';
    ln$out_comb_sum               number := 0;
    ln$out_1                      number := 0;
    ln$out_2                      number := 0;
    ln$out_3                      number := 0;
    ln$out_4                      number := 0;
    ln$out_5                      number := 0;
    ln$out_6                      number := 0;
    ln$year                       number := 0;
    ln$quarter                    number := 0;
    ln$month                      number := 0;
    ln$avg_pattern_cnt            number := 0;
                                    
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pv_gambling_date: '||pv_gambling_date);     
     dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);     
     dbms_output.put_line('pn_comb1: '||pn_comb1);     
     dbms_output.put_line('pn_comb2: '||pn_comb2);     
     dbms_output.put_line('pn_comb3: '||pn_comb3);     
     dbms_output.put_line('pn_comb4: '||pn_comb4);     
     dbms_output.put_line('pn_comb5: '||pn_comb5);     
     dbms_output.put_line('pn_comb6: '||pn_comb6);     

     compute_inbound_values (pv_gambling_type    => pv_gambling_type
                           , pv_gambling_date    => pv_gambling_date
                           , pn_gambling_id      => pn_gambling_id  
                           , pn_comb1            => pn_comb1        
                           , pn_comb2            => pn_comb2        
                           , pn_comb3            => pn_comb3        
                           , pn_comb4            => pn_comb4        
                           , pn_comb5            => pn_comb5        
                           , pn_comb6            => pn_comb6        
                           , xn_out_comb_sum     => ln$out_comb_sum    
                           , xn_out_1            => ln$out_1        
                           , xn_out_2            => ln$out_2        
                           , xn_out_3            => ln$out_3        
                           , xn_out_4            => ln$out_4        
                           , xn_out_5            => ln$out_5        
                           , xn_out_6            => ln$out_6        
                           , xn_year             => ln$year        
                           , xn_quarter          => ln$quarter     
                           , xn_month            => ln$month       
                           , xn_avg_pattern_cnt  => ln$avg_pattern_cnt          
                           , x_err_code          => x_err_code
                            ); 
dbms_output.put_line('ln$out_comb_sum: '||ln$out_comb_sum);     
dbms_output.put_line('ln$out_1: '||ln$out_1);     
dbms_output.put_line('ln$out_2: '||ln$out_2);     
dbms_output.put_line('ln$out_3: '||ln$out_3);     
dbms_output.put_line('ln$out_4: '||ln$out_4);     
dbms_output.put_line('ln$out_5: '||ln$out_5);     
dbms_output.put_line('ln$out_6: '||ln$out_6);     
dbms_output.put_line('ln$year: '||ln$year);     
dbms_output.put_line('ln$quarter: '||ln$quarter);     
dbms_output.put_line('ln$month: '||ln$month);     
dbms_output.put_line('ln$gm: '||ln$avg_pattern_cnt);     

     if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
        upd_s_average_patterns (pv_gambling_type    => pv_gambling_type
                              , pn_out_comb_sum     => ln$out_comb_sum    
                              , pn_out_1            => ln$out_1           
                              , pn_out_2            => ln$out_2           
                              , pn_out_3            => ln$out_3           
                              , pn_out_4            => ln$out_4           
                              , pn_out_5            => ln$out_5           
                              , pn_out_6            => ln$out_6           
                              , pn_in_last_year     => ln$year   
                              , pn_in_last_qtr      => ln$quarter
                              , pn_in_last_month    => ln$month  
                              , pn_avg_pattern_cnt  => ln$avg_pattern_cnt                           
                              , x_err_code          => x_err_code);
     end if;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end average_patterns_handler;

  procedure upd_s_digits_patterns (pv_gambling_type               olap_sys.s_digits_patterns.drawing_type%type
                                  , pn_out_comb_sum                olap_sys.s_digits_patterns.out_comb_sum%type
                                  , pn_out_sum_par_comb            olap_sys.s_digits_patterns.out_sum_par_comb%type
                                  , pn_out_sum_mod_comb            olap_sys.s_digits_patterns.out_sum_mod_comb%type
                                  , pn_out_c1                      olap_sys.s_digits_patterns.out_c1%type
                                  , pn_out_c2                      olap_sys.s_digits_patterns.out_c2%type
                                  , pn_out_c3                      olap_sys.s_digits_patterns.out_c3%type
                                  , pn_out_c4                      olap_sys.s_digits_patterns.out_c4%type
                                  , pn_out_c5                      olap_sys.s_digits_patterns.out_c5%type
                                  , pn_out_c6                      olap_sys.s_digits_patterns.out_c6%type
                                  , pn_in_last_year                olap_sys.s_digits_patterns.in_last_year%type
                                  , pn_in_last_qtr                 olap_sys.s_digits_patterns.in_last_qtr%type
                                  , pn_in_last_month               olap_sys.s_digits_patterns.in_last_month%type
                                  , pn_in_gm                       olap_sys.s_digits_patterns.in_gm%type
                                  , x_err_code       in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_s_digits_patterns';
  begin
     update olap_sys.s_digits_patterns
        set in_cnt        = in_cnt + 1
          , in_gm         = pn_in_gm
          , in_last_year  = pn_in_last_year 
          , in_last_qtr   = pn_in_last_qtr  
          , in_last_month = pn_in_last_month
          , updated_by    = user
          , updated_date  = sysdate
      where drawing_type     = pv_gambling_type
        and out_c1           = pn_out_c1     
        and out_c2           = pn_out_c2
        and out_c3           = pn_out_c3
        and out_c4           = pn_out_c4
        and out_comb_sum     = pn_out_comb_sum     
        and out_sum_par_comb = pn_out_sum_par_comb
        and out_sum_mod_comb = pn_out_sum_mod_comb;
        
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;   
     dbms_output.put_line(sql%rowcount||' rows updated on olap_sys.s_digits_patterns');   
        
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end upd_s_digits_patterns;
  
  procedure compute_inbound_values (pv_gambling_type  	               olap_sys.sl_gamblings.gambling_type%type
                                  , pv_gambling_date                   olap_sys.sl_gamblings.gambling_date%type
                                  , pn_gambling_id                     olap_sys.sl_gamblings.gambling_id%type
                                  , pn_comb1                           olap_sys.sl_gamblings.comb1%type
                                  , pn_comb2                           olap_sys.sl_gamblings.comb2%type
                                  , pn_comb3                           olap_sys.sl_gamblings.comb3%type
                                  , pn_comb4                           olap_sys.sl_gamblings.comb4%type
                                  , pn_comb5                           olap_sys.sl_gamblings.comb5%type
                                  , pn_comb6                           olap_sys.sl_gamblings.comb6%type
                                  , xn_out_comb_sum      in out NOCOPY olap_sys.sl_gamblings.comb6%type
                                  , xn_out_sum_par_comb  in out NOCOPY olap_sys.sl_gamblings.comb6%type
                                  , xn_out_sum_mod_comb  in out NOCOPY olap_sys.sl_gamblings.comb6%type
                                  , xn_year              in out NOCOPY number
                                  , xn_quarter           in out NOCOPY number
                                  , xn_month             in out NOCOPY number
                                  , xn_gm                in out NOCOPY number
                                  , x_err_code           in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'compute_inbound_values';
                                        
  begin

     xn_out_comb_sum     := pn_comb1+pn_comb2+pn_comb3+pn_comb4+pn_comb5+pn_comb6;
     xn_out_sum_par_comb := olap_sys.w_common_pkg.get_par_f (pn_comb1)
                          + olap_sys.w_common_pkg.get_par_f (pn_comb2)
                          + olap_sys.w_common_pkg.get_par_f (pn_comb3)
                          + olap_sys.w_common_pkg.get_par_f (pn_comb4)
                          + olap_sys.w_common_pkg.get_par_f (pn_comb5)
                          + olap_sys.w_common_pkg.get_par_f (pn_comb6);
     xn_out_sum_mod_comb := mod(pn_comb1,3)
                          + mod(pn_comb2,3)
                          + mod(pn_comb3,3)
                          + mod(pn_comb4,3)
                          + mod(pn_comb5,3)
                          + mod(pn_comb6,3);

     xn_year         := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_year_date_format));
     xn_quarter      := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_quarter_date_format));
     xn_month        := to_number(to_char(to_date(pv_gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format));
     xn_gm           := olap_sys.w_common_pkg.get_gigaloterias_count (pv_gambling_type
                                                                    , pn_gambling_id-1
                                                                    , pn_comb1
                                                                    , pn_comb2
                                                                    , pn_comb3
                                                                    , pn_comb4
                                                                    , pn_comb5
                                                                    , pn_comb6);

     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;             
  end compute_inbound_values;
     
  procedure digits_patterns_handler (pv_gambling_type  	     olap_sys.sl_gamblings.gambling_type%type
                                    , pv_gambling_date               olap_sys.sl_gamblings.gambling_date%type
                                    , pn_gambling_id                 olap_sys.sl_gamblings.gambling_id%type
                                    , pn_comb1                       olap_sys.sl_gamblings.comb1%type
                                    , pn_comb2                       olap_sys.sl_gamblings.comb2%type
                                    , pn_comb3                       olap_sys.sl_gamblings.comb3%type
                                    , pn_comb4                       olap_sys.sl_gamblings.comb4%type
                                    , pn_comb5                       olap_sys.sl_gamblings.comb5%type
                                    , pn_comb6                       olap_sys.sl_gamblings.comb6%type
                                    , x_err_code       in out NOCOPY number) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'digits_patterns_handler';
    ln$out_comb_sum               number := 0;
    ln$out_sum_par_comb           number := 0;
    ln$out_sum_mod_comb           number := 0;
    ln$year                       number := 0;
    ln$quarter                    number := 0;
    ln$month                      number := 0;
    ln$gm                         number := 0;
                                    
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pv_gambling_date: '||pv_gambling_date);     
     dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);     
     dbms_output.put_line('pn_comb1: '||pn_comb1);     
     dbms_output.put_line('pn_comb2: '||pn_comb2);     
     dbms_output.put_line('pn_comb3: '||pn_comb3);     
     dbms_output.put_line('pn_comb4: '||pn_comb4);     
     dbms_output.put_line('pn_comb5: '||pn_comb5);     
     dbms_output.put_line('pn_comb6: '||pn_comb6);     

     compute_inbound_values (pv_gambling_type    => pv_gambling_type
                           , pv_gambling_date    => pv_gambling_date
                           , pn_gambling_id      => pn_gambling_id  
                           , pn_comb1            => pn_comb1        
                           , pn_comb2            => pn_comb2        
                           , pn_comb3            => pn_comb3        
                           , pn_comb4            => pn_comb4        
                           , pn_comb5            => pn_comb5        
                           , pn_comb6            => pn_comb6        
                           , xn_out_comb_sum     => ln$out_comb_sum    
                           , xn_out_sum_par_comb => ln$out_sum_par_comb
                           , xn_out_sum_mod_comb => ln$out_sum_mod_comb
                           , xn_year             => ln$year        
                           , xn_quarter          => ln$quarter     
                           , xn_month            => ln$month       
                           , xn_gm               => ln$gm
                           , x_err_code          => x_err_code
                            ); 
dbms_output.put_line('ln$out_comb_sum: '||ln$out_comb_sum);     
dbms_output.put_line('ln$out_sum_par_comb: '||ln$out_sum_par_comb);     
dbms_output.put_line('ln$out_sum_mod_comb: '||ln$out_sum_mod_comb);     
dbms_output.put_line('ln$year: '||ln$year);     
dbms_output.put_line('ln$quarter: '||ln$quarter);     
dbms_output.put_line('ln$month: '||ln$month);     
dbms_output.put_line('ln$gm: '||ln$gm);

     if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
        upd_s_digits_patterns (pv_gambling_type    => pv_gambling_type
                              , pn_out_comb_sum     => ln$out_comb_sum    
                              , pn_out_sum_par_comb => ln$out_sum_par_comb
                              , pn_out_sum_mod_comb => ln$out_sum_mod_comb
                              , pn_out_c1           => pn_comb1           
                              , pn_out_c2           => pn_comb2           
                              , pn_out_c3           => pn_comb3           
                              , pn_out_c4           => pn_comb4           
                              , pn_out_c5           => pn_comb5           
                              , pn_out_c6           => pn_comb6           
                              , pn_in_last_year     => ln$year   
                              , pn_in_last_qtr      => ln$quarter
                              , pn_in_last_month    => ln$month  
                              , pn_in_gm            => ln$gm
                              , x_err_code          => x_err_code);
     end if;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end digits_patterns_handler;

  procedure upd_ciclo_aparicion_stats (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                     , pn_gambling_id                     number
                                     , x_err_code           in out NOCOPY number) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'upd_ciclo_aparicion_stats';                
     lv$b_type                 olap_sys.s_gl_ciclo_aparicion_stats.b_type%type;                

     cursor c_ca_stats (pv_gambling_type    varchar2
                      , pv_b_type           varchar2 
                      , pn_gambling_id      number
                       ) is
     select round(avg(ciclo_aparicion_cnt),2) ca_avg
          , round(stddev(ciclo_aparicion_cnt)-1,2) ca_stddev
          , round(avg(ciclo_aparicion_cnt) - (STDDEV(ciclo_aparicion_cnt)-1),2) ca_low_range
          , round(avg(ciclo_aparicion_cnt) + (STDDEV(ciclo_aparicion_cnt)-1),2) ca_high_range
       from olap_sys.s_gl_ciclo_aparicion_stats gs
      where drawing_type = pv_gambling_type
        and b_type       = pv_b_type
        and drawing_id   = pn_gambling_id;

     cursor c_ca_dtl_stats (pv_gambling_type    varchar2
                          , pv_b_type           varchar2 
                          , pn_gambling_id      number
                          , pn_ca_ranking       number
                           ) is
     select round(avg(ciclo_aparicion_cnt),2) ca_ranking_avg
       from olap_sys.s_gl_ciclo_aparicion_stats gs
      where drawing_type            = pv_gambling_type
        and b_type                  = pv_b_type
        and drawing_id              = pn_gambling_id
        and ciclo_aparicion_ranking = pn_ca_ranking;

  begin
     olap_sys.w_common_pkg.g_index := 1;
     for p in 1..6 loop
         if olap_sys.w_common_pkg.g_index = 1 then
            lv$b_type := 'B1';	
         elsif olap_sys.w_common_pkg.g_index = 2 then
            lv$b_type := 'B2';
         elsif olap_sys.w_common_pkg.g_index = 3 then
            lv$b_type := 'B3';
         elsif olap_sys.w_common_pkg.g_index = 4 then
            lv$b_type := 'B4';
         elsif olap_sys.w_common_pkg.g_index = 5 then
            lv$b_type := 'B5';
         elsif olap_sys.w_common_pkg.g_index = 6 then
            lv$b_type := 'B6';
         end if;
         
         --[opening cursor for computing avg, low_range and high_range 
         for m in c_ca_stats (pv_gambling_type => pv_gambling_type
                            , pv_b_type        => lv$b_type       
                            , pn_gambling_id   => pn_gambling_id  
                             ) loop
             
             --[updating ca_avg                
             update olap_sys.s_gl_ciclo_aparicion_stats gs
                set ciclo_aparicion_avg    = m.ca_avg
                  , ciclo_aparicion_stddev = m.ca_stddev
              where drawing_type        = pv_gambling_type
                and b_type              = lv$b_type
                and drawing_id          = pn_gambling_id;                
             
             --[updating ca_ranking 
             update olap_sys.s_gl_ciclo_aparicion_stats gs
                set ciclo_aparicion_ranking = case when ciclo_aparicion_cnt > m.ca_high_range then 1
                                                   when ciclo_aparicion_cnt between m.ca_low_range and m.ca_high_range then 2
                                                   when ciclo_aparicion_cnt < m.ca_low_range then 3
                                               end    
              where drawing_type        = pv_gambling_type
                and b_type              = lv$b_type
                and drawing_id          = pn_gambling_id;                
             
             --[reading ca_rankings 
             for k in 1..3 loop
                 for t in c_ca_dtl_stats (pv_gambling_type => pv_gambling_type
                                        , pv_b_type        => lv$b_type
                                        , pn_gambling_id   => pn_gambling_id
                                        , pn_ca_ranking    => k
                                         ) loop

                     --[updating ca_range 
                     update olap_sys.s_gl_ciclo_aparicion_stats gs
                        set ciclo_aparicion_range = case when ciclo_aparicion_cnt >= t.ca_ranking_avg then 'A'
                                                         when ciclo_aparicion_cnt < t.ca_ranking_avg then 'B'
                                                       end    
                      where drawing_type            = pv_gambling_type
                        and b_type                  = lv$b_type
                        and drawing_id              = pn_gambling_id
                        and ciclo_aparicion_ranking = k;                
                                         
                 end loop;                        
             end loop; 
              
         end loop;                    
         olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
     end loop;     
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end upd_ciclo_aparicion_stats;

  procedure ins_ciclo_aparicion_stats (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                     , pn_gambling_id                     number
                                     , x_err_code           in out NOCOPY number) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'ins_ciclo_aparicion_stats';
     lv$b_type                 olap_sys.s_gl_ciclo_aparicion_stats.b_type%type;                

  begin
     olap_sys.w_common_pkg.g_index := 1;
     for p in 1..6 loop
         if olap_sys.w_common_pkg.g_index = 1 then
            lv$b_type := 'B1';	
         elsif olap_sys.w_common_pkg.g_index = 2 then
            lv$b_type := 'B2';
         elsif olap_sys.w_common_pkg.g_index = 3 then
            lv$b_type := 'B3';
         elsif olap_sys.w_common_pkg.g_index = 4 then
            lv$b_type := 'B4';
         elsif olap_sys.w_common_pkg.g_index = 5 then
            lv$b_type := 'B5';
         elsif olap_sys.w_common_pkg.g_index = 6 then
            lv$b_type := 'B6';
         end if;

        insert into olap_sys.s_gl_ciclo_aparicion_stats 
        (drawing_type, b_type, ciclo_aparicion, ciclo_aparicion_cnt, ciclo_aparicion_avg, ciclo_aparicion_stddev, ciclo_aparicion_ranking, ciclo_aparicion_range, drawing_id)
        select pv_gambling_type drawing_type
             , b_type
             , ca
             , ca_cnt_chr ca_cnt
             , 0 ca_avg
             , 0 ca_stddev
             , 0 ca_rkn
             , null ca_ran
             , pn_gambling_id drawing_id
         from (
               with b_master_tbl as ( 
               select gambling_id drawing_id
                 from olap_sys.sl_gamblings
                where gambling_type = pv_gambling_type
                  and gambling_id >= (select min(drawing_id) from olap_sys.s_calculo_stats where drawing_type = pv_gambling_type)
               ), b_tbl as (   
               select upper(lv$b_type)
                    , drawing_id
                    , ciclo_aparicion ca
                 from olap_sys.s_calculo_stats
                where b_type = upper(lv$b_type)
                  and nvl(winner_flag,'X') = 'Y'
               ), b_cnt as (
               select upper(lv$b_type)
                    , ciclo_aparicion ca
                    , count(1) ca_cnt
                 from olap_sys.s_calculo_stats
                where b_type = upper(lv$b_type)
                  and nvl(winner_flag,'X') = 'Y'
                 group by ciclo_aparicion 
               ) select upper(lv$b_type) b_type
                      , drawing_id+1 drawing_id
                      , nvl((select bt.ca from b_tbl bt where bt.drawing_id = mt.drawing_id),0) ca
                      , nvl((select bc.ca_cnt from b_tbl bt, b_cnt bc where bt.drawing_id = mt.drawing_id and bt.ca = bc.ca),0) ca_cnt
                      , to_char(nvl((select bc.ca_cnt from b_tbl bt, b_cnt bc where bt.drawing_id = mt.drawing_id and bt.ca = bc.ca),0)) ca_cnt_chr
                   from b_master_tbl mt
               ) pivot (count(ca_cnt)
                 for drawing_id in (1,2)
              )
              ;

         olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
     end loop; 
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

  end ins_ciclo_aparicion_stats;
  
  --[main procedure used for computing ciclo aparicion stats
  procedure ciclo_aparicion_stats_handler (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                     number
                                         , x_err_code           in out NOCOPY number) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'ciclo_aparicion_stats_handler';                
  begin
     ins_ciclo_aparicion_stats (pv_gambling_type => pv_gambling_type
                              , pn_gambling_id   => pn_gambling_id
                              , x_err_code       => x_err_code
                               );
     
     if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
        upd_ciclo_aparicion_stats (pv_gambling_type => pv_gambling_type
                                 , pn_gambling_id   => pn_gambling_id
                                 , x_err_code       => x_err_code
                                  );
     end if;  
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end ciclo_aparicion_stats_handler;

  procedure upd_s_outbound_digit_avg_stats (pv_gambling_type  	           olap_sys.sl_gamblings.gambling_type%type
                                          , pn_comb1                       number
                                          , pn_comb2                       number
                                          , pn_comb3                       number
                                          , pn_comb4                       number
                                          , pn_comb5                       number
                                          , pn_comb6                       number
                                          , pn_sum_par_comb                number
                                          , pn_sum_mod_comb                number
                                          , x_err_code       in out NOCOPY number       
                                           ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'upd_s_outbound_digit_avg_stats';                  
  begin
     update olap_sys.s_outbound_digit_avg_stats
        set in_cnt       = in_cnt + 1
          , updated_by   = USER
          , updated_date = SYSDATE
      where drawing_type     = pv_gambling_type
        and out_c1           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb1 => pn_comb1)
        and out_c2           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb2 => pn_comb2)
        and out_c3           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb3 => pn_comb3)
        and out_c4           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb4 => pn_comb4)
        and out_c5           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb5 => pn_comb5)
        and out_c6           = olap_sys.w_common_pkg.get_average_pattern_count('mrtr',pn_comb6 => pn_comb6)
        and out_sum_par_comb = pn_sum_par_comb
        and out_sum_mod_comb = pn_sum_mod_comb;
     
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception 
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;          
  end upd_s_outbound_digit_avg_stats;
  
  --[ procedure used for updating stating table based on digit average stats
  procedure upd_outbound_digit_handler (pv_gambling_type  	           olap_sys.sl_gamblings.gambling_type%type
                                      , pn_comb1                       number
                                      , pn_comb2                       number
                                      , pn_comb3                       number
                                      , pn_comb4                       number
                                      , pn_comb5                       number
                                      , pn_comb6                       number
                                      , pn_sum_par_comb                number
                                      , pn_sum_mod_comb                number
                                      , x_err_code       in out NOCOPY number       
                                       ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'upd_outbound_digit_handler';                  
  begin
     upd_s_outbound_digit_avg_stats (pv_gambling_type => pv_gambling_type
                                   , pn_comb1         => pn_comb1        
                                   , pn_comb2         => pn_comb2        
                                   , pn_comb3         => pn_comb3        
                                   , pn_comb4         => pn_comb4        
                                   , pn_comb5         => pn_comb5        
                                   , pn_comb6         => pn_comb6        
                                   , pn_sum_par_comb  => pn_sum_par_comb 
                                   , pn_sum_mod_comb  => pn_sum_mod_comb 
                                   , x_err_code       => x_err_code      
                                    );
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;          
  end upd_outbound_digit_handler;
  
  --[function used for validating ans formatting gambling date
  function is_gambling_date_valid (pv_gambling_date         varchar2) return varchar2 is
      ld_gambling_date  date;
      lv_new_date       varchar2(10);
      lv_date_valid     varchar2(1) := 'N';
      ln_day            number(2);
      ln_month          number(2);
  begin
     dbms_output.put_line('pv_gambling_date: '||pv_gambling_date);
     lv_new_date := replace(pv_gambling_date,substr(pv_gambling_date,3,1),substr(olap_sys.w_common_pkg.g_date_format,3,1));
     dbms_output.put_line('lv_new_date: '||lv_new_date);
     ld_gambling_date := to_date(lv_new_date,olap_sys.w_common_pkg.g_date_format);
     ln_day        := to_number(substr(lv_new_date,1,2));
     ln_month      := to_number(substr(lv_new_date,4,2));
     lv_new_date   := to_char(ln_day)||substr(olap_sys.w_common_pkg.g_date_format,3,1)
                    ||to_char(ln_month)||substr(olap_sys.w_common_pkg.g_date_format,3,1)
                    ||substr(lv_new_date,7,4);
     dbms_output.put_line('formatted lv_new_date: '||lv_new_date);
     return lv_new_date;
  exception
    when others then
       dbms_output.put_line(sqlerrm);
       return null;  
  end is_gambling_date_valid;

  --[ main procedure used for computing number of matches between table sl_gamblings and s_gl_abril_combinations
  procedure set_abril_combinations_handler (pv_gambling_type  	           olap_sys.sl_gamblings.gambling_type%type
                                          , pn_gambling_id                 number
                                          , pv_comb_list                   varchar2
                                          , x_err_code       in out NOCOPY number
                                           ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'set_abril_combinations_handler';
    ln$current_id                 olap_sys.s_gl_abril_combinations.current_id%type;
  begin
/*   olap_sys.w_common_pkg.g_dml_stmt := 'update olap_sys.s_gl_abril_combinations set num_matches=num_matches+1';
   olap_sys.w_common_pkg.g_dml_stmt := ', updated_date=SYSDATE, updated_by=USER, current_id=:1';
   olap_sys.w_common_pkg.g_dml_stmt := ' where drawing_type=:2 and :3 between id_from and id_to';
   olap_sys.w_common_pkg.g_dml_stmt := ' and comb:4=:5';*/

     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
     dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);

   if pv_comb_list is not null then

      select nvl(max(current_id),0) current_id
        into ln$current_id
        from olap_sys.s_gl_abril_combinations
       where drawing_type = pv_gambling_type;

      dbms_output.put_line('ln$current_id: '||ln$current_id);
      if pn_gambling_id > ln$current_id then
          update olap_sys.s_gl_abril_combinations
             set num_matches=0, current_id=null, updated_by=null, updated_date=null
           where drawing_type = pv_gambling_type
             and nvl(status,'X') not in ('W');

          dbms_output.put_line('pv_comb_list: '||pv_comb_list);

          for r in (select regexp_substr(pv_comb_list,'[^,]+',1,level) str
                        from dual
                      connect by level <= length(pv_comb_list)-length(replace(pv_comb_list,',',''))+1) loop
            begin
               olap_sys.w_common_pkg.g_index := 1;
               for i in 1..6 loop
	             olap_sys.w_common_pkg.g_dml_stmt := 'update olap_sys.s_gl_abril_combinations set num_matches=num_matches+1';
	             olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||', updated_date=SYSDATE, updated_by=USER, current_id='||pn_gambling_id;
	             olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' where drawing_type='||chr(39)||pv_gambling_type||chr(39)||' and '||pn_gambling_id||' between id_from and id_to';
	             olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and comb'||to_char(olap_sys.w_common_pkg.g_index)||'='||to_number(r.str);
                   --dbms_output.put_line(olap_sys.w_common_pkg.g_dml_stmt);
                   execute immediate olap_sys.w_common_pkg.g_dml_stmt;
                   olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index +1;
              end loop;
            exception
               when value_error then
                  dbms_output.put_line('Invalid value for '||r.str);
                  x_err_code := 1;
                  exit;
            end;
          end loop;

          update olap_sys.s_gl_abril_combinations
             set match_history   = match_history||decode(num_matches,0,null,'|'||current_id||':'||num_matches)
               , status          = decode(num_matches,6,'W',null)
           where drawing_type = pv_gambling_type
             and num_matches>0 ;

          update olap_sys.s_gl_abril_combinations
             set win_drawing_cnt = olap_sys.w_common_pkg.count_win_drawings(match_history)
               , num_matches_cnt = win_drawing_cnt+num_matches
           where drawing_type = pv_gambling_type;

          x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
      elsif pn_gambling_id = ln$current_id then
          dbms_output.put_line('Drawing Id '||pn_gambling_id||' is the latest drawing loaded.');
          x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
      elsif pn_gambling_id < ln$current_id then
          dbms_output.put_line('Unable to load drawing Id '||pn_gambling_id||' due to it is a old drawing id.');
          x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
      end if;
  else
     dbms_output.put_line('Combination list is null');
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
  end if;


  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());

      raise;
  end set_abril_combinations_handler;


  --!proceso principal para detectar cambios en la info del sorteo actual con respecto al anterior 
  procedure get_num_primos_info_handler (pn_gambling_id                 number
									   , pn_comb1                       number
									   , pn_comb2                       number
									   , pn_comb3                       number
									   , pn_comb4                       number
									   , pn_comb5                       number
									   , pn_comb6                       number
									   , x_err_code       in out NOCOPY number 								 
									    ) is
										   
    LV$PROCEDURE_NAME    	CONSTANT VARCHAR2(30) := 'get_num_primos_info_handler'; 
	CN$DOS_NUMEROS_PRIMOS 	CONSTANT NUMBER(1) := 2;
	ln$comb1             	number := 0;
    ln$comb2             	number := 0;
    ln$comb3             	number := 0;
    ln$comb4             	number := 0;
    ln$comb5             	number := 0;
    ln$comb6				number := 0;
	ln$numero_primo1 		number := 0;
	ln$numero_primo2 		number := 0;	
	ln$diferencia_tipo		number := 0;
	ln$diferencia			number := 0;
	lv$play_flag			varchar2(1);
	lv$above_avg_flag		varchar2(1);
	lv$diferencia_tipo_info varchar2(20);
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	DBMS_OUTPUT.ENABLE(NULL);
	
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	
	begin
		select DECODE(PN1,1,COMB1,NULL) COMB1
			 , DECODE(PN2,1,COMB2,NULL) COMB2
			 , DECODE(PN3,1,COMB3,NULL) COMB3
			 , DECODE(PN4,1,COMB4,NULL) COMB4
			 , DECODE(PN5,1,COMB5,NULL) COMB5
			 , DECODE(PN6,1,COMB6,NULL) COMB6 
	      into ln$comb1 
			 , ln$comb2
			 , ln$comb3
			 , ln$comb4
			 , ln$comb5
			 , ln$comb6
		  from olap_sys.w_combination_responses_fs
		 where pn_cnt = CN$DOS_NUMEROS_PRIMOS
		   and comb1 = pn_comb1 
		   and comb2 = pn_comb2 
		   and comb3 = pn_comb3 
		   and comb4 = pn_comb4 
		   and comb5 = pn_comb5 
		   and comb6 = pn_comb6; 			

dbms_output.put_line('10.  '||ln$comb1||' - '||ln$comb2||' - '||ln$comb3||' - '||ln$comb4||' - '||ln$comb5||' - '||ln$comb6);	
		--!obteniendo los numeros primos
		ln$numero_primo1 := 0;
		ln$numero_primo2 := 0;

		if ln$comb1 is not null then
			ln$numero_primo1 := ln$comb1;
		end if;

		if ln$comb2 is not null then
			if ln$numero_primo1 = 0 then
				ln$numero_primo1 := ln$comb2;
			else
				ln$numero_primo2 := ln$comb2;
			end if;	
		end if;			

		if ln$comb3 is not null then
			if ln$numero_primo1 = 0 then
				ln$numero_primo1 := ln$comb3;
			else
				ln$numero_primo2 := ln$comb3;
			end if;	
		end if;	

		if ln$comb4 is not null then
			if ln$numero_primo1 = 0 then
				ln$numero_primo1 := ln$comb4;
			else
				ln$numero_primo2 := ln$comb4;
			end if;	
		end if;				

		if ln$comb5 is not null then
			if ln$numero_primo1 = 0 then
				ln$numero_primo1 := ln$comb5;
			else
				ln$numero_primo2 := ln$comb5;
			end if;	
		end if;	
		
		if ln$comb6 is not null then
			ln$numero_primo2 := ln$comb6;
		end if;				


dbms_output.put_line('20.  '||ln$numero_primo1||' - '||ln$numero_primo2);	
		if ln$numero_primo1 > 0 and ln$numero_primo2 > 0 then
			begin
				select case when diferencia < 0 then 1 else 2 end diferencia_tipo
					 , diferencia
				  into ln$diferencia_tipo
					 , ln$diferencia
				  from olap_sys.pm_parejas_primos par
				 where primo_ini = ln$numero_primo1
				   and primo_fin = ln$numero_primo2; 

dbms_output.put_line('30.  '||ln$diferencia_tipo||' - '||ln$diferencia);
				begin
					select play_flag
						 , above_avg_flag
					  into lv$play_flag
						 , lv$above_avg_flag
					  from olap_sys.pm_panorama_primos pan
					 where pn_cnt = CN$DOS_NUMEROS_PRIMOS
					   and comb1 = pn_comb1 
					   and comb2 = pn_comb2 
					   and comb3 = pn_comb3 
					   and comb4 = pn_comb4 
					   and comb5 = pn_comb5 
					   and comb6 = pn_comb6; 

dbms_output.put_line('40.  '||lv$play_flag||' - '||lv$above_avg_flag);	
					lv$diferencia_tipo_info := to_char(ln$diferencia_tipo)||'|P:'||lv$play_flag||'|AA:'||lv$above_avg_flag||'|D:'||to_char(ln$diferencia);
dbms_output.put_line('50.  '||lv$diferencia_tipo_info||', len: '||length(lv$diferencia_tipo_info));	


					--!actualizando tabla sl_gamblings
					update olap_sys.sl_gamblings
					   set diferencia_tipo_info	= lv$diferencia_tipo_info
					 where gambling_id = pn_gambling_id;

dbms_output.put_line('60.  '||sql%rowcount||' rows updated');	
					x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;					 
				exception
					when no_data_found then
						lv$play_flag	  := null;
						lv$above_avg_flag := null;
				end;
			exception
				when no_data_found then
					ln$diferencia_tipo := null;
					ln$diferencia 	   := null;			
			end;
		end if;
	exception
		when no_data_found then
		ln$comb1 := null;
		ln$comb2 := null;
		ln$comb3 := null;
		ln$comb4 := null;
		ln$comb5 := null;
		ln$comb6 := null;
	end;

	
  exception
     when others then
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end get_num_primos_info_handler; 


  procedure ins_mr_resultados_summary is
  begin
  
	insert into olap_sys.mr_resultados_summary 
	select year, qtr, gambling_date, gambling_id, pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt, none1, none2, none3, none4, none5, none6, none_cnt, par1, par2, par3, par4, par5, par6, par_cnt, d2, d3, d4, d5, d6, dr, decode(cu1,'R',1,'G',2,'B',3,0) cu1, decode(cu2,'R',1,'G',2,'B',3,0) cu2, decode(cu3,'R',1,'G',2,'B',3,0) cu3, decode(cu4,'R',1,'G',2,'B',3,0) cu4, decode(cu5,'R',1,'G',2,'B',3,0) cu5, decode(cu6,'R',1,'G',2,'B',3,0) cu6, decode(clt1,'R',1,'G',2,'B',3,0) clt1, decode(clt2,'R',1,'G',2,'B',3,0) clt2, decode(clt3,'R',1,'G',2,'B',3,0) clt3, decode(clt4,'R',1,'G',2,'B',3,0) clt4, decode(clt5,'R',1,'G',2,'B',3,0) clt5, decode(clt6,'R',1,'G',2,'B',3,0) clt6, c1_ca, c2_ca, c3_ca, c4_ca, c5_ca, c6_ca, sum_ca, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pre1, pre2, pre3, pre4, pre5, pre6
	  from olap_sys.pm_mr_resultados_v2 pm
	 where pm.gambling_id  >= 595
	   and not exists (select 1 
						 from olap_sys.mr_resultados_summary rs
						where pm.gambling_id = rs.gambling_id);
  end ins_mr_resultados_summary;

  --!insertando el resultado del sorteo en la tabla mr_resultados
  procedure ins_mr_resultados (pn_gambling_id   number) is 
  begin
	insert into olap_sys.mr_resultados (year, qtr, day, gambling_date, gambling_id, comb1, comb2, comb3, comb4, comb5, comb6, additional, pn_none_cnt, pn_par_cnt, xmod, gl_cnt, pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt, none1, none2, none3, none4, none5, none6, none_cnt, par1, par2, par3, par4, par5, par6, par_cnt, m3_1, m3_2, m3_3, m3_4, m3_5, m3_6, m3_cnt, m4_1, m4_2, m4_3, m4_4, m4_5, m4_6, m4_cnt, m5_1, m5_2, m5_3, m5_4, m5_5, m5_6, m5_cnt, m7_1, m7_2, m7_3, m7_4, m7_5, m7_6, m7_cnt, dist_c1_c2, dist_c1_c3, dist_c1_c4, dist_c1_c5, dist_c1_c6, d1, d2, d3, d4, d5, d6, ds, dr, d01_09, d10_19, d20_29, d30_39, t1, t2, t3, t4, t5, t6, t7, t8, t9, t0, term1_cnt, term2_cnt, co_1, co_2, co_3, co_4, co_5, co_cnt, rep_comb1, rep_comb2, rep_comb3, rep_comb4, rep_comb5, rep_comb6, rep_cnt, cu1, cu2, cu3, cu4, cu5, cu6, clt1, clt2, clt3, clt4, clt5, clt6, lt_seq_no_pct, lt_seq_no, dcase, null_cnt, red_cnt, green_cnt, blue_cnt, rank_cnt, c1_ca, c2_ca, c3_ca, c4_ca, c5_ca, c6_ca, sum_ca, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pxc_cnt, pre1, pre2, pre3, pre4, pre5, pre6, precnt, chng_pos1, chng_ubi1, chng_lt1, chng_ca1, chng_pxc1, chng_flag1, chng_pos2, chng_ubi2, chng_lt2, chng_ca2, chng_pxc2, chng_flag2, chng_pos3, chng_ubi3, chng_lt3, chng_ca3, chng_pxc3, chng_flag3, chng_pos4, chng_ubi4, chng_lt4, chng_ca4, chng_pxc4, chng_flag4, chng_pos5, chng_ubi5, chng_lt5, chng_ca5, chng_pxc5, chng_flag5, chng_pos6, chng_ubi6, chng_lt6, chng_ca6, chng_pxc6, chng_flag6, dtipo_info, comb_seq_no, comb_seq_no_pct, comb_sum, term_cnt, seq_id) 
	select year, qtr, day, gambling_date, gambling_id, comb1, comb2, comb3, comb4, comb5, comb6, additional, pn_none_cnt, pn_par_cnt, xmod, gl_cnt, pn1, pn2, pn3, pn4, pn5, pn6, pn_cnt, none1, none2, none3, none4, none5, none6, none_cnt, par1, par2, par3, par4, par5, par6, par_cnt, m3_1, m3_2, m3_3, m3_4, m3_5, m3_6, m3_cnt, m4_1, m4_2, m4_3, m4_4, m4_5, m4_6, m4_cnt, m5_1, m5_2, m5_3, m5_4, m5_5, m5_6, m5_cnt, m7_1, m7_2, m7_3, m7_4, m7_5, m7_6, m7_cnt, dist_c1_c2, dist_c1_c3, dist_c1_c4, dist_c1_c5, dist_c1_c6, d1, d2, d3, d4, d5, d6, ds, dr, d01_09, d10_19, d20_29, d30_39, t1, t2, t3, t4, t5, t6, t7, t8, t9, t0, term1_cnt, term2_cnt, co_1, co_2, co_3, co_4, co_5, co_cnt, rep_comb1, rep_comb2, rep_comb3, rep_comb4, rep_comb5, rep_comb6, rep_cnt, cu1, cu2, cu3, cu4, cu5, cu6, clt1, clt2, clt3, clt4, clt5, clt6, lt_seq_no_pct, lt_seq_no, dcase, null_cnt, red_cnt, green_cnt, blue_cnt, rank_cnt, c1_ca, c2_ca, c3_ca, c4_ca, c5_ca, c6_ca, sum_ca, pxc1, pxc2, pxc3, pxc4, pxc5, pxc6, pxc_cnt, pre1, pre2, pre3, pre4, pre5, pre6, precnt, chng_pos1, chng_ubi1, chng_lt1, chng_ca1, chng_pxc1, chng_flag1, chng_pos2, chng_ubi2, chng_lt2, chng_ca2, chng_pxc2, chng_flag2, chng_pos3, chng_ubi3, chng_lt3, chng_ca3, chng_pxc3, chng_flag3, chng_pos4, chng_ubi4, chng_lt4, chng_ca4, chng_pxc4, chng_flag4, chng_pos5, chng_ubi5, chng_lt5, chng_ca5, chng_pxc5, chng_flag5, chng_pos6, chng_ubi6, chng_lt6, chng_ca6, chng_pxc6, chng_flag6, dtipo_info, comb_seq_no, comb_seq_no_pct, comb_sum, term_cnt, seq_id
	  from olap_sys.pm_mr_resultados_v2 v2
	 where v2.gambling_id > pn_gambling_id - 1 
	   and not exists (select 'X'
						 from olap_sys.mr_resultados r
						where r.gambling_id = v2.gambling_id
	   ); 	
  exception
     when others then
		DBMS_OUTPUT.PUT_LINE(SQLERRM);	 
  end;
  
  
  procedure main_p (p_gambling_type                olap_sys.t_gambling_types.gambling_type%type
                  , x_err_code       in out NOCOPY number     
                  , x_err_msg        in out NOCOPY varchar2   
                   ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'main_p';              
     l$OneDigit                 constant number(1)                               := 1;
     l$Priority                          number := 0;
     l$gambling_date_formatted           olap_sys.sl_gamblings.gambling_date%type;
     le$gambling_date_invalid       exception;
     pragma exception_init (le$gambling_date_invalid, -20011);
	 
	 
     cursor c_gam is
     select trim(gambling_date) gambling_date  
          , trim(gambling_id) gambling_id    
          , trim(comb1) comb1          
          , trim(comb2) comb2          
          , trim(comb3) comb3          
          , trim(comb4) comb4          
          , trim(comb5) comb5          
          , trim(comb6) comb6          
          , trim(additional) additional     
       from olap_sys.t_super_lotto_loader_ext;

     cursor c_mlt is
     select trim(gambling_date)      gambling_date
          , trim(gambling_id)        gambling_id   
          , trim(comb1)              comb1   
          , trim(comb2)              comb2   
          , trim(comb3)              comb3   
          , trim(comb4)              comb4   
          , trim(comb5)              comb5   
          , trim(comb6)              comb6   
          , trim(additional)       	 additional
          , nvl(trim(price),'0')  price
       from olap_sys.t_melate_loader_ext;

     cursor c_mrtr is
     select trim(ext.gambling_date)      gambling_date
          , trim(ext.gambling_id)        gambling_id   
          , trim(ext.comb1)              comb1   
          , trim(ext.comb2)              comb2   
          , trim(ext.comb3)              comb3   
          , trim(ext.comb4)              comb4   
          , trim(ext.comb5)              comb5   
          , trim(ext.comb6)              comb6   
          , trim(ext.additional)       	 additional
          , nvl(trim(ext.price),'0')  price
       from olap_sys.t_mretro_loader_ext ext
      where not exists (select 1 from olap_sys.sl_gamblings g where g.gambling_type = 'mrtr' and g.gambling_id = trim(ext.gambling_id));


  BEGIN
     dbms_output.enable(NULL);
     validate_gambling_type (p_gambling_type => p_gambling_type);
     
     g_inscnt    := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     g_errorcnt  := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     g_rowcnt    := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     gn$err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     
     --[ super lotto ]--
     if p_gambling_type = 'sl7' then      
        for i in c_gam loop
           g_rowcnt              := g_rowcnt + 1;                    
           
           ins_target_table (p_gambling_date         => i.gambling_date
                           , p_gambling_id           => i.gambling_id
                           , p_comb1                 => i.comb1
                           , p_comb2                 => i.comb2
                           , p_comb3                 => i.comb3
                           , p_comb4                 => i.comb4
                           , p_comb5                 => i.comb5
                           , p_comb6                 => i.comb6
                           , p_additional            => i.additional
                           , p_gambling_type         => p_gambling_type
                           , p_comb_sum              => i.comb1 + i.comb2 + i.comb3 + i.comb4 + i.comb5 + i.comb6
                           , p_priority       	     => l$Priority	
                           , p_inscnt                => g_inscnt
                           , p_errorcnt              => g_errorcnt
                           );
           
           if mod(g_rowcnt,100) = 0 then
              commit;
           end if;              
        end loop;
     end if;
     
     --[ melate ]--
     if p_gambling_type = 'mlt' then
        for i in c_mlt loop
           g_rowcnt := g_rowcnt + 1;

           ins_target_table (p_gambling_date  => i.gambling_date
                           , p_gambling_id    => i.gambling_id
                           , p_comb1          => i.comb1
                           , p_comb2          => i.comb2
                           , p_comb3          => i.comb3
                           , p_comb4          => i.comb4
                           , p_comb5          => i.comb5
                           , p_comb6          => i.comb6
                           , p_additional     => i.additional
                           , p_gambling_type  => p_gambling_type
                           , p_comb_sum       => i.comb1 + i.comb2 + i.comb3 + i.comb4 + i.comb5 + i.comb6
                           , p_price          => i.price
                           , p_priority       => l$Priority	                           
                           , p_inscnt         => g_inscnt
                           , p_errorcnt       => g_errorcnt
                           );
          
           if mod(g_rowcnt,100) = 0 then
              commit;
           end if;              
        end loop;
     end if;

     --[ melate retro ]--
     if p_gambling_type = 'mrtr' then
dbms_output.put_line(LV$PROCEDURE_NAME||' s100');     
        for i in c_mrtr loop
           l$gambling_date_formatted := is_gambling_date_valid (pv_gambling_date => i.gambling_date);
           if l$gambling_date_formatted is not null then 
	           g_rowcnt := g_rowcnt + 1;
	
	           ins_target_table (p_gambling_date  => l$gambling_date_formatted
	                           , p_gambling_id    => i.gambling_id
	                           , p_comb1          => i.comb1
	                           , p_comb2          => i.comb2
	                           , p_comb3          => i.comb3
	                           , p_comb4          => i.comb4
	                           , p_comb5          => i.comb5
	                           , p_comb6          => i.comb6
	                           , p_additional     => i.additional
	                           , p_gambling_type  => p_gambling_type
	                           , p_comb_sum       => i.comb1 + i.comb2 + i.comb3 + i.comb4 + i.comb5 + i.comb6
	                           , p_price          => i.price
	                           , p_priority       => l$Priority	                           
	                           , p_inscnt         => g_inscnt
	                           , p_errorcnt       => g_errorcnt
	                           );
	
	           --[ inserting basic template for manually user input
	           ins_calculo_stats_handler (pv_gambling_type => p_gambling_type
	                                       , pn_gambling_id   => i.gambling_id
	                                       , pn_comb1         => i.comb1
	                                       , pn_comb2         => i.comb2
	                                       , pn_comb3         => i.comb3
	                                       , pn_comb4         => i.comb4
	                                       , pn_comb5         => i.comb5
	                                       , pn_comb6         => i.comb6
	                                       , x_err_code       => g_errorcnt
	                                        );
	                      
	           --[updating staging table tied to counts and min/max drawing_id based on comb_sum
	           upd_s_comb_sum_min_max_stats (pv_gambling_type => p_gambling_type
	                                       , pn_comb_sum      => i.gambling_id
	                                       , x_err_code       => x_err_code);
	
	           --[procedure used for updating counts on gigaloteria patterns
	           gigaloterias_patterns_handler (pv_gambling_type => p_gambling_type
	                                        , pn_gambling_id   => i.gambling_id  
	                                        , x_err_code       => x_err_code);   
	           
	           --[proceudre used for updating stats on table s_average_patterns
	           average_patterns_handler (pv_gambling_type => p_gambling_type
	                                   , pv_gambling_date => i.gambling_date
	                                   , pn_gambling_id   => i.gambling_id
	                                   , pn_comb1         => i.comb1
	                                   , pn_comb2         => i.comb2
	                                   , pn_comb3         => i.comb3
	                                   , pn_comb4         => i.comb4
	                                   , pn_comb5         => i.comb5
	                                   , pn_comb6         => i.comb6
	                                   , x_err_code       => x_err_code
	                                    );
	           
	           --[proceudre used for updating stats on table s_digits_patterns                          
	           digits_patterns_handler (pv_gambling_type => p_gambling_type  
	                                  , pv_gambling_date => i.gambling_date  
	                                  , pn_gambling_id   => i.gambling_id    
	                                  , pn_comb1         => i.comb1          
	                                  , pn_comb2         => i.comb2          
	                                  , pn_comb3         => i.comb3          
	                                  , pn_comb4         => i.comb4          
	                                  , pn_comb5         => i.comb5          
	                                  , pn_comb6         => i.comb6          
	                                  , x_err_code       => x_err_code
	                                   );          
	           
	             --[ main procedure used for computing stats based on general_index and comb_sum 
	             get_gi_comb_sum_stats_handler (pv_gambling_type => p_gambling_type
	                                          , pn_gambling_id   => i.gambling_id    
	                                          , pn_comb1         => i.comb1   
	                                          , pn_comb2         => i.comb2   
	                                          , pn_comb3         => i.comb3   
	                                          , pn_comb4         => i.comb4   
	                                          , pn_comb5         => i.comb5   
	                                          , pn_comb6         => i.comb6   
	                                          , x_err_code       => x_err_code
	                                           );                                                                                                                                   
	
	             --[ main procedure used for computing inbound digit counts and inserting computed data into staging table 
	             inbound_digit_stat_handler (pv_gambling_type => p_gambling_type 
	                                       , pn_gambling_id   => i.gambling_id   
	                                       , pn_comb1         => i.comb1         
	                                       , pn_comb2         => i.comb2         
	                                       , pn_comb3         => i.comb3         
	                                       , pn_comb4         => i.comb4         
	                                       , pn_comb5         => i.comb5         
	                                       , pn_comb6         => i.comb6         
	                                       , x_err_code       => x_err_code      
	                                        );
	             
	             --[main procedure used for computing ciclo aparicion stats
	             /* table removed
	             ciclo_aparicion_stats_handler (pv_gambling_type => p_gambling_type 
	                                          , pn_gambling_id   => i.gambling_id   
	                                          , x_err_code       => x_err_code
	                                           );
	             */
	             --[ procedure used for updating stating table based on digit average stats
	             upd_outbound_digit_handler (pv_gambling_type => p_gambling_type
	                                       , pn_comb1         => i.comb1
	                                       , pn_comb2         => i.comb2
	                                       , pn_comb3         => i.comb3
	                                       , pn_comb4         => i.comb4
	                                       , pn_comb5         => i.comb5
	                                       , pn_comb6         => i.comb6
	                                       , pn_sum_par_comb  => olap_sys.w_common_pkg.get_par_f (i.comb1)
	                                                           + olap_sys.w_common_pkg.get_par_f (i.comb2)
	                                                           + olap_sys.w_common_pkg.get_par_f (i.comb3)
	                                                           + olap_sys.w_common_pkg.get_par_f (i.comb4)
	                                                           + olap_sys.w_common_pkg.get_par_f (i.comb5)
	                                                           + olap_sys.w_common_pkg.get_par_f (i.comb6) 
	                                       , pn_sum_mod_comb  => mod(i.comb1,3)
	                                                           + mod(i.comb2,3)
	                                                           + mod(i.comb3,3)
	                                                           + mod(i.comb4,3)
	                                                           + mod(i.comb5,3)
	                                                           + mod(i.comb6,3)
	                                       , x_err_code       => x_err_code
	                                        );
	             
--	             set_abril_combinations_handler (pv_gambling_type => p_gambling_type
--                                                   , pn_gambling_id   => i.gambling_id
--                                                   , pv_comb_list     => i.comb1||','||i.comb2||','||i.comb3||','||i.comb4||','||i.comb5||','||i.comb6
--                                                   , x_err_code       => x_err_code
--                                                    );
                     
--                     s_drawings_comparisons_handler (pv_gambling_type => p_gambling_type
--                                                   , pn_gambling_id   => i.gambling_id
--                                                   , x_err_code       => x_err_code
--                                                     );                                                                                      

                repeated_numbers_handler (pv_gambling_type => p_gambling_type 
                                        , pn_gambling_id => i.gambling_id 
                                        , pn_comb1 => i.comb1
                                        , pn_comb2 => i.comb2
                                        , pn_comb3 => i.comb3
                                        , pn_comb4 => i.comb4
                                        , pn_comb5 => i.comb5
                                        , pn_comb6 => i.comb6
                                        , x_err_code => x_err_code
                                         );

                prime_pairs_handler (pv_gambling_type => p_gambling_type 
                                   , pn_gambling_id => i.gambling_id 
                                   , pn_comb1 => i.comb1
                                   , pn_comb2 => i.comb2
                                   , pn_comb3 => i.comb3
                                   , pn_comb4 => i.comb4
                                   , pn_comb5 => i.comb5
                                   , pn_comb6 => i.comb6
                                   , x_err_code => x_err_code
                                    );
			    
                ley_tercio_handler (pv_gambling_type => p_gambling_type 
                                  , pn_gambling_id => i.gambling_id 
                                  , x_err_code => x_err_code       
                                   ); 
				
                terminaciones_handler (pv_gambling_type => p_gambling_type 
                                     , pn_gambling_id => i.gambling_id 
                                     , pn_comb1 => i.comb1
                                     , pn_comb2 => i.comb2
                                     , pn_comb3 => i.comb3
                                     , pn_comb4 => i.comb4
                                     , pn_comb5 => i.comb5
                                     , pn_comb6 => i.comb6
                                     , x_err_code => x_err_code
                                      );				
	           
			   panorama_handler (pv_gambling_type => p_gambling_type 
                               , pn_gambling_id   => i.gambling_id 
                               , x_err_code       => x_err_code
                                );
			   
			   decenas_numeros_primos_handler (pv_gambling_type => p_gambling_type 
                                             , pn_gambling_id => i.gambling_id 
                                             , pn_comb1 => i.comb1
                                             , pn_comb2 => i.comb2
                                             , pn_comb3 => i.comb3
                                             , pn_comb4 => i.comb4
                                             , pn_comb5 => i.comb5
                                             , pn_comb6 => i.comb6
                                             , x_err_code => x_err_code
                                              );	

			  --!proceso principal para detectar cambios en la info del sorteo actual con respecto al anterior 
			  gl_comparar_sorteo_inf_handler (pv_gambling_type => p_gambling_type
										    , pn_gambling_id   => i.gambling_id
										    , x_err_code       => x_err_code
										 	 );

				--!proceso principal para detectar cambios en la info del sorteo actual con respecto al anterior
				get_num_primos_info_handler (pn_gambling_id => i.gambling_id
										  , pn_comb1 => i.comb1
										  , pn_comb2 => i.comb2
										  , pn_comb3 => i.comb3
										  , pn_comb4 => i.comb4
										  , pn_comb5 => i.comb5
										  , pn_comb6 => i.comb6
										  , x_err_code => x_err_code
										   );											
								 
				--!proceso general para realizar conteos de LT y FR de los ultimoas 100 jugadas en base a la ultima jugadas
				olap_sys.w_pick_panorama_pkg.get_frec_lt_count_wrapper (pn_drawing_id     => i.gambling_id
																	  , pv_auto_commit    => 'N'
																	  , pv_insert_pattern => 'Y'	
																	  , x_err_code        => x_err_code);

				--!proceso para insertar lo resultados en la tabla mr_resultados_summary
				--ins_mr_resultados_summary;
				
				--!insertando el resultado del sorteo en la tabla mr_resultados
				ins_mr_resultados (pn_gambling_id => i.gambling_id);
								
				--!actualizar la informacion de gl como frecuencia, ley del tercio, ciclo aparicion asi como numero de sorteo, etc
				w_gl_automaticas_pkg.upd_gl_automaticas_handler(pn_drawing_id => i.gambling_id
															  , pv_ca_comb_flag => 'Y');
				--!contar los aciertos y numeros repetidos del ultimo sorteo de la lista de combinaciones en base al ID del sorteo
				w_gl_automaticas_pkg.aciertos_repetidos_handler(pn_drawing_id => i.gambling_id);

				--!evaluacion de las predicciones
				w_gl_automaticas_pkg.evaluate_prediccion_handler(pn_drawing_id => i.gambling_id);
				
			   if mod(g_rowcnt,100) = 0 then
	              commit;
	           end if;
           else
              raise le$gambling_date_invalid;
           end if;              
        end loop;
     end if;

dbms_output.put_line(LV$PROCEDURE_NAME||' s110. gn$err_code: '||gn$err_code); 
     if gn$err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
        --[ marking out all bingo combinations
        winner_vendor_drawing_handler (p_gambling_type => p_gambling_type
                                     , x_err_code      => x_err_code
                                     , x_err_msg       => x_err_msg
                                      );
   
        dbms_output.put_line('*****   procedure t_data_loader_pkg.main (1)   *****'||chr(10));
        dbms_output.put_line('Gambling Type: '||p_gambling_type);
        get_rowcnt_target_table (p_gambling_type => p_gambling_type);
        dbms_output.put_line(g_rowcnt||' rows retrieved by external table.');        
        dbms_output.put_line(lpad(g_inscnt,7,' ')||' rows loaded into olap_sys.sl_gamblings.');
        dbms_output.put_line(lpad(g_errorcnt,7,' ')||' error exceptions.');
   
        commit; 
        dbms_output.put_line('*****   commit executed   *****');
        dbms_output.put_line('------------------------------------------------------------');
     else
        x_err_code := gn$err_code;
        x_err_msg  := gv$err_msg;
     end if;               
    
  exception
     when le$gambling_date_invalid then
        dbms_output.put_line('main -> Invalid Gambling Date.');
     when no_data_found then
        dbms_output.put_line('main -> '||sqlerrm );

        save_log (p_xcontext       => 'dataload'
                , p_package_name   => g_package_name 
                , p_procedure_name => 'main'
                , p_attribute1     => 'p_gambling_type'
                , p_attribute2     => p_gambling_type
                , p_attribute7     => sqlerrm
                 );
        rollback;
        raise;              
  END main_p;

  procedure validate_gambling_type (p_gambling_type olap_sys.t_gambling_types.gambling_type%type) is
    LV$PROCEDURE_NAME       constant varchar2(30) := 'validate_gambling_type';  
    l_exist_row  number(1) := 0;
    
  begin
    select 1
      into l_exist_row
      from olap_sys.t_gambling_types
     where gambling_type = p_gambling_type;

  exception
     when no_data_found then
        dbms_output.put_line('validate_gambling_type. '||sqlerrm );

        save_log (p_xcontext       => 'dataload'
                , p_package_name   => g_package_name 
                , p_procedure_name => 'validate_gambling_type'
                , p_attribute1     => 'p_gambling_type'
                , p_attribute2     => p_gambling_type
                , p_attribute7     => sqlerrm
                 );

        raise;         
  end validate_gambling_type;

  procedure ins_target_table (p_gambling_date         olap_sys.sl_gamblings.gambling_date%type
                            , p_gambling_id           olap_sys.sl_gamblings.gambling_id%type
                            , p_comb1                 olap_sys.sl_gamblings.comb1%type
                            , p_comb2                 olap_sys.sl_gamblings.comb2%type
                            , p_comb3                 olap_sys.sl_gamblings.comb3%type
                            , p_comb4                 olap_sys.sl_gamblings.comb4%type
                            , p_comb5                 olap_sys.sl_gamblings.comb5%type
                            , p_comb6                 olap_sys.sl_gamblings.comb6%type
                            , p_additional            olap_sys.sl_gamblings.additional%type
                            , p_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                            , p_comb_sum              olap_sys.sl_gamblings.comb_sum%type
                            , p_price                 olap_sys.sl_gamblings.price%type default 0
                            , p_priority       	      olap_sys.sl_gamblings.priority%type                            
                            , p_inscnt         in out number 
                            , p_errorcnt       in out number
                            ) is
    LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_target_table';       
  BEGIN
     --[building seq_id in order to retrieve user sort_by value	
     olap_sys.w_common_pkg.g_column_value := p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6;	
     insert into olap_sys.sl_gamblings (
                               gambling_date  
                             , gambling_id    
                             , comb1          
                             , comb2          
                             , comb3          
                             , comb4          
                             , comb5          
                             , comb6          
                             , additional     
                             , gambling_type  
                             , creation_date
                             , comb_sum
                             , price
                             , priority  
                             , bingo     
                             , week_day				
                             , xcomb1          
                             , xcomb2          
                             , xcomb3          
                             , xcomb4          
                             , xcomb5          
                             , xcomb6          
                             , xadditional     
                             , xcomb_sum
                             , xdcomb1          
                             , xdcomb2          
                             , xdcomb3          
                             , xdcomb4          
                             , xdcomb5          
                             , xdcomb6          
                             , xdadditional     
                             , xdcomb_sum
                             , xddcomb1          
                             , xddcomb2          
                             , xddcomb3          
                             , xddcomb4          
                             , xddcomb5          
                             , xddcomb6  
                             , sum_par_comb 
                             , sum_mod_comb 
                             , sort_by
                             , global_index
                             , global_index_pct
                             , elegible_flag
                             , elegible_cnt
                             , m4_comb1   
                             , m4_comb2   
                             , m4_comb3   
                             , m4_comb4   
                             , m4_comb5   
                             , m4_comb6   
                             , level_comb1
                             , level_comb2
                             , level_comb3
                             , level_comb4
                             , level_comb5
                             , level_comb6
                             , prime_number_cnt
                             , seq_id       
                              )  
     values( p_gambling_date  
           , p_gambling_id    
           , p_comb1          
           , p_comb2          
           , p_comb3          
           , p_comb4          
           , p_comb5          
           , p_comb6          
           , p_additional     
           , p_gambling_type
           , sysdate
           , p_comb_sum
           , p_price     
           , p_priority   
           , 'N'  
           , to_char(to_date(p_gambling_date,olap_sys.w_common_pkg.g_date_format),'DY')
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb1, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb2, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb3, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb4, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb5, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb6, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_additional, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_f (p_number => p_comb_sum, p_convert_type => 'SUM', p_gambling_type => p_gambling_type)             				          
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb1, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb2, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb3, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb4, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb5, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb6, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_additional, p_gambling_type => p_gambling_type)
           , olap_sys.w_common_pkg.convert_low_high_f (p_number => p_comb_sum, p_convert_type => 'SUM', p_gambling_type => p_gambling_type)             				          
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb1)
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb2)
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb3)
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb4)
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb5)
           , olap_sys.w_common_pkg.convert_low_high_dtl_f (p_number => p_comb6)  
           , olap_sys.w_common_pkg.get_par_f (p_comb1)
           + olap_sys.w_common_pkg.get_par_f (p_comb2)
           + olap_sys.w_common_pkg.get_par_f (p_comb3)
           + olap_sys.w_common_pkg.get_par_f (p_comb4)
           + olap_sys.w_common_pkg.get_par_f (p_comb5)
           + olap_sys.w_common_pkg.get_par_f (p_comb6) 
           , mod(p_comb1,3)
           + mod(p_comb2,3)
           + mod(p_comb3,3)
           + mod(p_comb4,3)
           + mod(p_comb5,3)
           + mod(p_comb6,3)  
           , olap_sys.w_common_pkg.get_user_sort_by (pn_seq_id => olap_sys.w_common_pkg.g_column_value)   
           , olap_sys.w_common_pkg.get_usr_global_index (pv_drawing_type => p_gambling_type
                                                       , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6)   
           , olap_sys.w_common_pkg.get_usr_global_index_pct (pv_drawing_type => p_gambling_type
                                                           , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6)   
           , olap_sys.w_common_pkg.get_usr_elegible_flag (pv_drawing_type => p_gambling_type
                                                        , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6)
           , olap_sys.w_common_pkg.get_usr_elegible_cnt (pv_drawing_type => p_gambling_type
                                                       , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6)
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                  , pv_column_name  => 'COMB1')                                           
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                  , pv_column_name  => 'COMB2')                                           
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                  , pv_column_name  => 'COMB3')                                           
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                 , pv_column_name  => 'COMB4')                                           
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                  , pv_column_name  => 'COMB5')                                           
           , olap_sys.w_common_pkg.get_usr_m4_comb (pv_drawing_type => p_gambling_type
                                                  , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                  , pv_column_name  => 'COMB6')                                           
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB1')                                                                                    
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB2')                                                                                    
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB3')                                                                                    
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB4')                                                                                    
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB5')                                                                                    
           , olap_sys.w_common_pkg.get_usr_level_comb (pv_drawing_type => p_gambling_type
                                                     , pn_seq_id       => p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6
                                                     , pv_column_name  => 'COMB6') 
           , olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb1)
           + olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb2)
           + olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb3)
           + olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb4)
           + olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb5)
           + olap_sys.w_common_pkg.is_prime_number (pn_digit => p_comb6) 
           ,  p_comb1||p_comb2||p_comb3||p_comb4||p_comb5||p_comb6                                                                                                                            
           );

     p_inscnt := p_inscnt + 1;
     
     --[ updating status to W (Winner) on table w_combination_responses_fs for every drawing loaded into table sl_gamblings
     olap_sys.w_common_pkg.upd_status_vendor_drawing (pv_drawing_type => p_gambling_type
                                                    , pn_comb1        => p_comb1
                                                    , pn_comb2        => p_comb2
                                                    , pn_comb3        => p_comb3
                                                    , pn_comb4        => p_comb4
                                                    , pn_comb5        => p_comb5
                                                    , pn_comb6        => p_comb6
                                                    , pv_status       => OLAP_SYS.W_COMMON_PKG.GV$WINNER
                                                    , x_err_code      => gn$err_code
                                                    , x_err_msg       => gv$err_msg
                                                     );

  exception
    when dup_val_on_index then
        p_errorcnt := p_errorcnt + 1; 
    when others then
        dbms_output.put_line('ins_target_table -> '||sqlerrm );
        p_errorcnt := p_errorcnt + 1;
        save_log (p_xcontext       => 'dataload'
                , p_package_name   => g_package_name 
                , p_procedure_name => 'validate_gambling_type'
                , p_attribute1     => 'p_gambling_type'
                , p_attribute2     => p_gambling_type
                , p_attribute7     => sqlerrm
                 );

        raise;                                 
  end ins_target_table;

  procedure get_rowcnt_target_table (p_gambling_type olap_sys.t_gambling_types.gambling_type%type) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_rowcnt_target_table';       
     l_rowcnt	   number := 0;
  begin
     
     select count(1)
       into l_rowcnt
       from olap_sys.sl_gamblings
      where gambling_type = p_gambling_type;
  
     dbms_output.put_line(l_rowcnt||' rows already exists into olap_sys.sl_gamblings table.');  
           
  end get_rowcnt_target_table;


  procedure save_log (p_xcontext       olap_sys.t_messages_log.xcontext%type
                    , p_package_name   olap_sys.t_messages_log.package_name%type default null
                    , p_procedure_name olap_sys.t_messages_log.procedure_name%type default null
                    , p_attribute1     olap_sys.t_messages_log.attribute1%type default null
                    , p_attribute2     olap_sys.t_messages_log.attribute2%type default null
                    , p_attribute3     olap_sys.t_messages_log.attribute3%type default null
                    , p_attribute4     olap_sys.t_messages_log.attribute4%type default null
                    , p_attribute5     olap_sys.t_messages_log.attribute5%type default null
                    , p_attribute6     olap_sys.t_messages_log.attribute6%type default null
                    , p_attribute7     olap_sys.t_messages_log.attribute7%type default null
                    , p_creation_date  olap_sys.t_messages_log.creation_date%type default sysdate
                    , p_created_by     olap_sys.t_messages_log.created_by%type default user
                     ) is
     pragma autonomous_transaction;                
  begin
  
    insert into olap_sys.t_messages_log(
                               xcontext      
                             , xsequence       
                             , package_name  
                             , procedure_name
                             , attribute1    
                             , attribute2    
                             , attribute3    
                             , attribute4    
                             , attribute5    
                             , attribute6    
                             , attribute7    
                             , creation_date 
                             , created_by                                  
                              )                
    values (
           p_xcontext                                         
         , t_messages_log_seq.nextval                          
         , p_package_name   
         , p_procedure_name 
         , p_attribute1     
         , p_attribute2     
         , p_attribute3     
         , p_attribute4     
         , p_attribute5     
         , p_attribute6         
         , p_attribute7    
         , p_creation_date 
         , p_created_by             
           );  
           
    commit;      
  exception
     when others then
        dbms_output.put_line('SAVE LOG. OTHERS. '||sqlerrm);
        rollback;
  end save_log;
  
  procedure winner_vendor_drawing_handler (p_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , x_err_code       in out NOCOPY number     
                                         , x_err_msg        in out NOCOPY varchar2   
                                          ) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'winner_vendor_drawing_handler';         
  begin
     load_plsql_table (p_gambling_type => p_gambling_type
                     , x_err_code      => x_err_code
                     , x_err_msg       => x_err_msg
                      );
     
     if x_err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then                 
        get_bingo_combs (x_GambTbl  => g$GambTbl
                       , x_err_code => x_err_code
                       , x_err_msg  => x_err_msg
                        );
        if x_err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
           upd_sl_gamblings_bingo (p_GambTbl => g$GambTbl
                                 , x_err_code      => x_err_code
                                 , x_err_msg       => x_err_msg
                                  );
        end if;                          
     end if;
  exception
    when others then
      x_err_code := sqlcode;
      x_err_msg  := sqlerrm;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                      
  end winner_vendor_drawing_handler;

  function get_max_gambling_id (p_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type) return number is  
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_max_gambling_id';         
     ln$max_gambling_id    olap_sys.sl_gamblings.gambling_id%type;  
  begin
     select nvl(max(gambling_id),0)
       into ln$max_gambling_id
       from olap_sys.sl_gamblings
      where gambling_type = p_gambling_type
        and bingo         = gv$BINGO;
     return ln$max_gambling_id;  
  exception
    when others then
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                      
  end get_max_gambling_id;
  
  procedure load_plsql_table (p_gambling_type  	             olap_sys.sl_gamblings.gambling_type%type
                            , x_err_code       in out NOCOPY number     
                            , x_err_msg        in out NOCOPY varchar2   
                             ) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'load_plsql_table';         
     ln$max_gambling_id    olap_sys.sl_gamblings.gambling_id%type;
  begin
     ln$max_gambling_id := get_max_gambling_id (p_gambling_type => p_gambling_type); 
     
     select gambling_date    
          , gambling_id      
          , comb1            
          , comb2            
          , comb3            
          , comb4            
          , comb5            
          , comb6            
          , additional       
          , gambling_type    
          , creation_date    
          , comb_sum         
          , price            
          , priority       		
          , bingo            
          , week_day 		
          , updated_date
          , xcomb1            
          , xcomb2            
          , xcomb3            
          , xcomb4            
          , xcomb5            
          , xcomb6            
          , xadditional       
          , xcomb_sum         
          , xdcomb1            
          , xdcomb2            
          , xdcomb3            
          , xdcomb4            
          , xdcomb5            
          , xdcomb6            
          , xdadditional       
          , xdcomb_sum         
          , xddcomb1            
          , xddcomb2            
          , xddcomb3            
          , xddcomb4            
          , xddcomb5            
          , xddcomb6
          , sum_par_comb
          , sum_mod_comb
          , sort_by
          , updated_by
          , sort_by_comb1_cnt
          , sort_by_comb4_cnt
          , sort_by_comb6_cnt  
          , global_index
          , global_index_pct
          , elegible_flag
          , elegible_cnt
          , m4_comb1   
          , m4_comb2   
          , m4_comb3   
          , m4_comb4   
          , m4_comb5   
          , m4_comb6   
          , level_comb1
          , level_comb2
          , level_comb3
          , level_comb4
          , level_comb5
          , level_comb6
          , prime_number_cnt
          , seq_id   
          , rep_comb1
		  , rep_comb2
		  , rep_comb3
		  , rep_comb4
		  , rep_comb5
		  , rep_comb6	
		  , diferencia_tipo_info		  
       bulk collect into g$GambTbl
       from olap_sys.sl_gamblings
      where gambling_type = p_gambling_type
        and gambling_id   > ln$Max_gambling_id
      order by gambling_id desc; 
      x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
      x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION;                     
  exception
    when others then
      x_err_code := sqlcode;
      x_err_msg  := sqlerrm;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                            
  end load_plsql_table;

  procedure get_bingo_combs (x_GambTbl        in out NOCOPY typ_gamb
                           , x_err_code       in out NOCOPY number     
                           , x_err_msg        in out NOCOPY varchar2   
                            ) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_bingo_combs';           
  begin
     dbms_output.enable(9999999);
     for t in x_GambTbl.first..x_GambTbl .last loop
        if t >1 then
           if x_GambTbl(t).price > x_GambTbl(t-1).price then
dbms_output.put_line('current: '||x_GambTbl(t).gambling_id||'~'||x_GambTbl(t).price||' . previous: '||x_GambTbl(t-1).gambling_id||'~'||x_GambTbl(t-1).price||' . date: '||to_char(to_date(x_GambTbl(t-1).gambling_date,olap_sys.w_common_pkg.g_date_format),'DY'));
              x_GambTbl(t-1).bingo    := gv$BINGO;
           end if;
        end if;
     end loop;
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION;               
  exception
    when others then
      x_err_code := sqlcode;
      x_err_msg  := sqlerrm;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                       
  end get_bingo_combs;

  procedure upd_sl_gamblings_bingo (p_GambTbl  	                   typ_gamb
                                  , x_err_code       in out NOCOPY number     
                                  , x_err_msg        in out NOCOPY varchar2   
                                   ) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_sl_gamblings_bingo';             
  begin
     for i in p_GambTbl.first..p_GambTbl .last loop
        if p_GambTbl(i).bingo = gv$BINGO then
           execute immediate 'update olap_sys.sl_gamblings set bingo = :1, updated_date = :3 where gambling_type = :4 and gambling_id = :5' 
           using p_GambTbl(i).bingo
               , sysdate
               , p_GambTbl(i).gambling_type
               , p_GambTbl(i).gambling_id;
        end if;
     end loop;
     commit;
      x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
      x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION;                    
  exception
    when others then
      x_err_code := sqlcode;
      x_err_msg  := sqlerrm;
      rollback;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                         
  end upd_sl_gamblings_bingo;

  procedure upd_gigamelate_tables_handler (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                 number
                                         , pn_comb_index                  number
                                         , pn_comb_value                  number
                                         , x_err_code       in out NOCOPY number     
                                          ) is
    LV$PROCEDURE_NAME              constant varchar2(30) := 'upd_gigamelate_tables_handler';
    lv$upd_gigamelate_stmt                  varchar2(500);
    lv$upd_gigamelate_stmt_2                varchar2(500);
    lv$upd_gigamelate_ltercio_stmt          varchar2(500);
    lv$upd_gigamelate_fu_stmt               varchar2(500);
    lv$upd_gigaloterias_stmt                varchar2(500);
    lv$upd_inbound_digit_stat_stmt          varchar2(500);
    lv$upd_ciclo_aparicion_stmt             varchar2(500);
    lv$b_type                               olap_sys.s_calculo_stats.b_type%type;
    ln$digit                                olap_sys.s_calculo_stats.digit%type;
    ln$ley_tercio_digit                     number := 6;
    ln$current_gambling_id                  number := pn_gambling_id+1;
    
    --[ metadata for ley tercio with rowcount > 0
    cursor c_ley_tercio_higher_zero (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                   , pn_current_gambling_id           number
                                   , pn_ley_tercio_digit              number) is
    select comb digit
         , count(1) nrows
      from olap_sys.vendor_digits_v
     where gambling_type = pv_gambling_type
       and gambling_id > pn_current_gambling_id-pn_ley_tercio_digit
     group by comb 
     order by nrows desc, comb desc;
   
  begin
    dbms_output.put_line('------------------------------------------');
    dbms_output.put_line(LV$PROCEDURE_NAME);
    dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);
    dbms_output.put_line('pn_comb_index: '||pn_comb_index);
    dbms_output.put_line('pn_comb_value: '||pn_comb_value);
--    lv$upd_gigamelate_stmt := 'update olap_sys.s_calculo_stats set winner_flag= :1 where drawing_type= :2 and drawing_id= :3 and b_type= :4 and digit= :5';   
--    lv$upd_gigamelate_ltercio_stmt := 'update olap_sys.s_gigamelate_ley_tercio_stats set winner_flag= :1 where drawing_type= :2 and drawing_id= :3 and digit= :5';

    if pn_comb_index = 1 then
       lv$b_type := 'B1';	
    elsif pn_comb_index = 2 then
       lv$b_type := 'B2';
    elsif pn_comb_index = 3 then
       lv$b_type := 'B3';
    elsif pn_comb_index = 4 then
       lv$b_type := 'B4';
    elsif pn_comb_index = 5 then
       lv$b_type := 'B5';
    elsif pn_comb_index = 6 then
       lv$b_type := 'B6';
    end if;
    
    ln$digit  := pn_comb_value;
    dbms_output.put_line('ln$digit: '||ln$digit||' lv$b_type: '||lv$b_type);

--    lv$upd_gigamelate_stmt            := 'update olap_sys.s_calculo_stats set winner_flag= '||chr(39)||'Y'||chr(39)||' where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id= '||pn_gambling_id||' and b_type= '||chr(39)||lv$b_type||chr(39)||' and digit= '||ln$digit;   
--    lv$upd_gigamelate_ltercio_stmt    := 'update olap_sys.s_gigamelate_ley_tercio_stats set winner_flag= '||chr(39)||lv$b_type||chr(39)||' where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id= '||pn_gambling_id||' and digit= '||ln$digit;
--    lv$upd_gigamelate_fu_stmt         := 'update olap_sys.s_gigamelate_frec_ubicaciones set winner_flag= '||chr(39)||'Y'||chr(39)||' where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id= '||pn_gambling_id||' and b_type= '||chr(39)||lv$b_type||chr(39)||' and digit= '||ln$digit;   
--    lv$upd_gigamelate_stmt_2       := 'update olap_sys.s_calculo_stats gs set frec_ubicacion = (select frec_ubicacion from olap_sys.s_gigamelate_frec_ubicaciones gf where gf.drawing_type=gs.drawing_type and gf.drawing_id=gs.drawing_id and gf.b_type=gs.b_type and gf.digit=gs.digit) where gs.drawing_type = '||chr(39)||pv_gambling_type||chr(39)||' and gs.drawing_id = '||pn_gambling_id+1;
--    lv$upd_gigaloterias_stmt       := 'update olap_sys.s_calculo_stats set winner_flag= '||chr(39)||'Y'||chr(39)||' where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id= '||pn_gambling_id||' and b_type= '||chr(39)||lv$b_type||chr(39)||' and digit= '||ln$digit;   
    lv$upd_gigaloterias_stmt          := 'update olap_sys.s_calculo_stats gs set gs.winner_flag= '||chr(39)||'Y'||chr(39);
    lv$upd_gigaloterias_stmt          := lv$upd_gigaloterias_stmt||' where gs.drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and gs.drawing_id= '||pn_gambling_id||' and gs.b_type= '||chr(39)||lv$b_type||chr(39)||' and gs.digit= '||ln$digit;   
--    lv$upd_inbound_digit_stat_stmt    := 'update olap_sys.s_inbound_digit_stats set winner_flag= '||chr(39)||'Y'||chr(39)||' where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and last_drawing_id= '||pn_gambling_id||' and b_type= '||chr(39)||lv$b_type||chr(39)||' and next_digit= '||ln$digit;   
--    lv$upd_ciclo_aparicion_stmt       := 'update olap_sys.s_gl_ciclo_aparicion_stats ca set ca.winner_flag = '||chr(39)||'Y'||chr(39)||' where ca.drawing_type = '||chr(39)||pv_gambling_type||chr(39)||' and ca.drawing_id = '||pn_gambling_id||' and ca.b_type = '||chr(39)||lv$b_type||chr(39);
--    lv$upd_ciclo_aparicion_stmt       := lv$upd_ciclo_aparicion_stmt ||' and ca.ciclo_aparicion = (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type = ca.drawing_type and gs.drawing_id = ca.drawing_id and gs.b_type = ca.b_type and gs.winner_flag is not null)';

    
    --dbms_output.put_line('1.'||substr(lv$upd_gigamelate_stmt,1,253));
    --execute immediate lv$upd_gigamelate_stmt;
    --dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_calculo_stats');
    --dbms_output.put_line('2.'||substr(lv$upd_gigamelate_ltercio_stmt,1,253));
    --execute immediate lv$upd_gigamelate_ltercio_stmt;
    --dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_gigamelate_ley_tercio_stats');
    --execute immediate lv$upd_gigamelate_fu_stmt;
    --dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_gigamelate_frec_ubicaciones');
    execute immediate lv$upd_gigaloterias_stmt;
    dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_calculo_stats');
    --execute immediate lv$upd_inbound_digit_stat_stmt;
    --dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_inbound_digit_stats');
--    execute immediate lv$upd_ciclo_aparicion_stmt;
--    dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_gl_ciclo_aparicion_stats');
    
--    execute immediate lv$upd_gigamelate_stmt_2;
--    dbms_output.put_line(sql%rowcount||' rows updated on table olap_sys.s_calculo_stats');
    dbms_output.put_line('#########################################################');
--    dbms_output.put_line(substr(lv$upd_gigamelate_stmt_2,1,255));
--    dbms_output.put_line(substr(lv$upd_gigamelate_stmt_2,256,255));
    dbms_output.put_line('#########################################################');
    --[updating data on table olap_sys.s_gigamelate_ley_tercio_stats with rowcount > 0 based on ley del tercio
--    lv$upd_gigamelate_ltercio_stmt := 'update olap_sys.s_gigamelate_ley_tercio_stats set seq_id= :1, xrowcount= :2 where drawing_type= '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id= '||pn_gambling_id||' and digit= '||p.digit;
/*
    lv$upd_gigamelate_ltercio_stmt := 'update olap_sys.s_gigamelate_ley_tercio_stats set seq_id= :1, xrowcount= :2 where drawing_type= :3 and drawing_id= :4 and digit= :5';
    olap_sys.w_common_pkg.g_index  := 1;
    olap_sys.w_common_pkg.g_updcnt := 0;
    dbms_output.put_line('updating data on table olap_sys.s_gigamelate_ley_tercio_stats with rowcount > 0 based on ley del tercio');
    for p in c_ley_tercio_higher_zero (pv_gambling_type       => pv_gambling_type
                                     , pn_current_gambling_id => ln$current_gambling_id 
                                     , pn_ley_tercio_digit    => ln$ley_tercio_digit) loop
        --dbms_output.put_line('3.'||substr(lv$upd_gigamelate_ltercio_stmt,1,253));                             
        execute immediate lv$upd_gigamelate_ltercio_stmt using olap_sys.w_common_pkg.g_index
                                                             , p.nrows
                                                             , pv_gambling_type
                                                             , ln$current_gambling_id
                                                             , p.digit;
        olap_sys.w_common_pkg.g_updcnt := olap_sys.w_common_pkg.g_updcnt + sql%rowcount;                 
        olap_sys.w_common_pkg.g_index  := olap_sys.w_common_pkg.g_index + 1;
    end loop;                     
*/
    x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
    
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                                                                
  end upd_gigamelate_tables_handler;
                                            
  procedure parser_vendor_drawing (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                 , pn_gambling_id                 number
                                 , pn_comb1                       number
                                 , pn_comb2                       number
                                 , pn_comb3                       number
                                 , pn_comb4                       number
                                 , pn_comb5                       number
                                 , pn_comb6                       number
                                 , pn_min_value                   number
                                 , pn_max_value                   number
                                 , x_err_code       in out NOCOPY number     
                                  ) is
    LV$PROCEDURE_NAME              constant varchar2(30) := 'parser_vendor_drawing';
    lv$winner_flag                          varchar2(1);
    lv$b_type                               olap_sys.s_calculo_stats.b_type%type;
    ln$gigamelate_stats_err_code            number := 0;
    ln$ley_tercio_stats_err_code            number := 0;
    ln$comb_value                           number := 0;   
    ln$g_index                              number := 0;                            
  begin
    dbms_output.put_line('------------------------------------------');
    dbms_output.put_line(LV$PROCEDURE_NAME);
    dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);
    dbms_output.put_line('pn_comb1: '||pn_comb1);
    dbms_output.put_line('pn_comb2: '||pn_comb2);
    dbms_output.put_line('pn_comb3: '||pn_comb3);
    dbms_output.put_line('pn_comb4: '||pn_comb4);
    dbms_output.put_line('pn_comb5: '||pn_comb5);
    dbms_output.put_line('pn_comb6: '||pn_comb6);
    dbms_output.put_line('pn_min_value: '||pn_min_value);
    dbms_output.put_line('pn_max_value: '||pn_max_value);
    
    --[ variable initialized as no data found
    olap_sys.w_common_pkg.g_data_found := 0;
    
    ln$g_index := 1;
    for p in pn_min_value..pn_max_value loop
        if ln$g_index = 1 then
           ln$comb_value := pn_comb1;
        elsif ln$g_index = 2 then
           ln$comb_value := pn_comb2;   	
        elsif ln$g_index = 3 then
           ln$comb_value := pn_comb3;   	
        elsif ln$g_index = 4 then
           ln$comb_value := pn_comb4;   	
        elsif ln$g_index = 5 then
           ln$comb_value := pn_comb5;   	
        elsif ln$g_index = 6 then
           ln$comb_value := pn_comb6;   	
        end if;
        dbms_output.put_line('g_index: '||ln$g_index||' comb_value: '||ln$comb_value);        	
        upd_gigamelate_tables_handler (pv_gambling_type => pv_gambling_type
                                     , pn_gambling_id   => pn_gambling_id-1
                                     , pn_comb_index    => ln$g_index
                                     , pn_comb_value    => ln$comb_value
                                     , x_err_code       => x_err_code
                                      );
        --dbms_output.put_line('000. x_err_code'||x_err_code);	
        if x_err_code != olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
           exit;
        end if;   	
        ln$g_index := ln$g_index + 1;                
        dbms_output.put_line('++ g_index: '||ln$g_index);        	
    end loop;

  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
 
      raise;               
  end parser_vendor_drawing;
                                  	
  procedure ins_calculo_stats_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                        , pn_gambling_id                 number
                                        , pn_comb1                       number
                                        , pn_comb2                       number
                                        , pn_comb3                       number
                                        , pn_comb4                       number
                                        , pn_comb5                       number
                                        , pn_comb6                       number
                                        , x_err_code       in out NOCOPY number       
                                         ) is
    LV$PROCEDURE_NAME              constant varchar2(30) := 'ins_calculo_stats_handler';
  begin
/*
    olap_sys.w_common_pkg.get_t_gambling_types_min_max (p_gambling_type => pv_gambling_type
                                                      , p_min_value     => ln$min_value
                                                      , p_max_value     => ln$max_value
                                                       );
*/
 
    parser_vendor_drawing (pv_gambling_type => pv_gambling_type
                         , pn_gambling_id   => pn_gambling_id 
                         , pn_comb1         => pn_comb1       
                         , pn_comb2         => pn_comb2       
                         , pn_comb3         => pn_comb3       
                         , pn_comb4         => pn_comb4       
                         , pn_comb5         => pn_comb5       
                         , pn_comb6         => pn_comb6       
                         , pn_min_value     => olap_sys.w_common_pkg.gn_min_value   
                         , pn_max_value     => olap_sys.w_common_pkg.gn_max_value   
                         , x_err_code       => x_err_code     
                          ); 
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;               
  end ins_calculo_stats_handler;
    
  function get_drawing_players_cnt (pv_drawing_type olap_sys.w_comb_setup_header_fs.attribute3%TYPE
                                  , pv_gambling_day VARCHAR2) return number is
                  
  begin
    olap_sys.w_common_pkg.g_rowcnt := 0;
    for g in olap_sys.w_common_pkg.c_details (pv_drawing_type => pv_drawing_type
                                            , pv_gambling_day => pv_gambling_day) loop
        olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;                                                
    end loop;                                                    
    return olap_sys.w_common_pkg.g_rowcnt;
  end get_drawing_players_cnt;
                  
  function is_valid_outbound_label (pv_gambling_type  	          olap_sys.s_preview_gamblings.drawing_type%type
                                  , pn_prime_number_cnt           olap_sys.s_preview_gamblings.prime_number_cnt%type
                                  , pv_level_comb1                olap_sys.s_preview_gamblings.level_comb1%type
                                  , pv_level_comb2                olap_sys.s_preview_gamblings.level_comb2%type
                                  , pv_level_comb3                olap_sys.s_preview_gamblings.level_comb3%type
                                  , pv_level_comb4                olap_sys.s_preview_gamblings.level_comb4%type
                                  , pv_level_comb5                olap_sys.s_preview_gamblings.level_comb5%type
                                  , pv_level_comb6                olap_sys.s_preview_gamblings.level_comb6%type
                                   ) return varchar2 is
                                   
    LV$PROCEDURE_NAME    constant varchar2(30) := 'is_valid_outbound_label';  
  begin
    dbms_output.put_line('--------------------------------');
    dbms_output.put_line(LV$PROCEDURE_NAME);
    dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
    dbms_output.put_line('pn_prime_number_cnt: '||pn_prime_number_cnt);
    dbms_output.put_line('pv_level_comb1: '||pv_level_comb1);
    dbms_output.put_line('pv_level_comb2: '||pv_level_comb2);
    dbms_output.put_line('pv_level_comb3: '||pv_level_comb3);
    dbms_output.put_line('pv_level_comb4: '||pv_level_comb4);
    dbms_output.put_line('pv_level_comb5: '||pv_level_comb5);
    dbms_output.put_line('pv_level_comb6: '||pv_level_comb6);

    select count(1)
      into olap_sys.w_common_pkg.g_rowcnt
      from olap_sys.c_drawing_criteria_rules
     where drawing_type     = pv_gambling_type 
       and status           = 'A'
       and criteria_type    = 'LABELS'
       and data_type        = 'CHAR' 
       and prime_number_cnt = pn_prime_number_cnt
       and out_attribute1   = pv_level_comb1 
       and out_attribute2   = pv_level_comb2
       and out_attribute3   = pv_level_comb3
       and out_attribute4   = pv_level_comb4
       and out_attribute5   = pv_level_comb5
       and out_attribute6   = pv_level_comb6;
    
    if olap_sys.w_common_pkg.g_rowcnt > 0 then
       return 'Y';
    else
       return 'N';
    end if;  

  exception
    when no_data_found then
       return 'N';      
  end is_valid_outbound_label;
  
  function is_winner_outbound_drawing (pv_gambling_type  	         olap_sys.s_preview_gamblings.drawing_type%type
                                     , pn_global_index                   olap_sys.s_preview_gamblings.global_index%type
                                      ) return boolean is
  begin
     select 1
       into olap_sys.w_common_pkg.g_data_found
       from dual
      where exists (select 1
                      from olap_sys.sl_gamblings g
                     where gambling_type = pv_gambling_type
                       and global_index  = pn_global_index 
                   ); 
     return true;
  exception
    when no_data_found then
      return false;   
  end is_winner_outbound_drawing;                           
                                                                                              
  
  procedure merge_s_preview_gamblings (pv_gambling_type  	            olap_sys.s_preview_gamblings.drawing_type%type
                                     , pn_seq_id                            olap_sys.s_preview_gamblings.seq_id%type
                                     , pn_comb1                             olap_sys.s_preview_gamblings.comb1%type
                                     , pn_comb2                             olap_sys.s_preview_gamblings.comb2%type
                                     , pn_comb3                             olap_sys.s_preview_gamblings.comb3%type
                                     , pn_comb4                             olap_sys.s_preview_gamblings.comb4%type
                                     , pn_comb5                             olap_sys.s_preview_gamblings.comb5%type
                                     , pn_comb6                             olap_sys.s_preview_gamblings.comb6%type
                                     , pn_comb_sum                          olap_sys.s_preview_gamblings.comb_sum%type
                                     , pn_sum_par_comb                      olap_sys.s_preview_gamblings.sum_par_comb%type
                                     , pn_sum_mod_comb                      olap_sys.s_preview_gamblings.sum_mod_comb%type
                                     , pn_global_index                      olap_sys.s_preview_gamblings.global_index%type
                                     , pn_current_odd                       olap_sys.s_preview_gamblings.current_odd%type
                                     , pn_use_cnt                           olap_sys.s_preview_gamblings.use_cnt%type
                                     , pn_prime_number_cnt                  olap_sys.s_preview_gamblings.prime_number_cnt%type
                                     , pv_level_comb1                       olap_sys.s_preview_gamblings.level_comb1%type 
                                     , pv_level_comb2                       olap_sys.s_preview_gamblings.level_comb2%type 
                                     , pv_level_comb3                       olap_sys.s_preview_gamblings.level_comb3%type 
                                     , pv_level_comb4                       olap_sys.s_preview_gamblings.level_comb4%type 
                                     , pv_level_comb5                       olap_sys.s_preview_gamblings.level_comb5%type 
                                     , pv_level_comb6                       olap_sys.s_preview_gamblings.level_comb6%type 
                                     , x_err_code       in out NOCOPY number       
                                      ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'merge_s_preview_gamblings';  
  begin
    dbms_output.put_line('--------------------------------');
    dbms_output.put_line(LV$PROCEDURE_NAME);
    dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
    dbms_output.put_line('pn_seq_id: '||pn_seq_id);
/*
    dbms_output.put_line('pn_comb1: '||pn_comb1);
    dbms_output.put_line('pn_comb2: '||pn_comb2);
    dbms_output.put_line('pn_comb3: '||pn_comb3);
    dbms_output.put_line('pn_comb4: '||pn_comb4);
    dbms_output.put_line('pn_comb5: '||pn_comb5);
    dbms_output.put_line('pn_comb6: '||pn_comb6);
    dbms_output.put_line('pn_comb_sum: '||pn_comb_sum);
    dbms_output.put_line('pn_sum_par_comb: '||pn_sum_par_comb);
    dbms_output.put_line('pn_sum_mod_comb: '||pn_sum_mod_comb);
    dbms_output.put_line('pn_current_odd: '||pn_current_odd);
*/
     merge into olap_sys.s_preview_gamblings pg using
     (with drawings_tbl as (select pv_gambling_type    drawing_type 
                                 , pn_seq_id           seq_id       
                                 , pn_comb1            comb1        
                                 , pn_comb2            comb2        
                                 , pn_comb3            comb3        
                                 , pn_comb4            comb4        
                                 , pn_comb5            comb5        
                                 , pn_comb6            comb6        
                                 , pn_comb_sum         comb_sum     
                                 , pn_sum_par_comb     sum_par_comb 
                                 , pn_sum_mod_comb     sum_mod_comb 
                                 , pn_global_index     global_index 
                                 , pn_current_odd      current_odd
                                 , 1                   use_cnt
                                 , 'N'                 use_flag   
                                 , pn_prime_number_cnt prime_number_cnt
                                 , pv_level_comb1      level_comb1
                                 , pv_level_comb2      level_comb2
                                 , pv_level_comb3      level_comb3
                                 , pv_level_comb4      level_comb4
                                 , pv_level_comb5      level_comb5
                                 , pv_level_comb6      level_comb6
                           from dual)
                           select drawing_type
                                , seq_id      
                                , comb1       
                                , comb2       
                                , comb3       
                                , comb4       
                                , comb5       
                                , comb6       
                                , comb_sum    
                                , sum_par_comb
                                , sum_mod_comb
                                , global_index
                                , current_odd
                                , use_cnt 
                                , use_flag
                                , prime_number_cnt
                                , level_comb1
                                , level_comb2
                                , level_comb3
                                , level_comb4
                                , level_comb5
                                , level_comb6
                             from drawings_tbl) pg_tbl
                         on (pg_tbl.drawing_type = pg.drawing_type
                            and pg_tbl.seq_id    = pg.seq_id
                            and pg_tbl.global_index = pg.global_index)
                         when matched then update set pg.use_cnt      = pg.use_cnt + 1
                                                    , pg.previous_odd = pg.current_odd
                                                    , pg.current_odd  = pg_tbl.current_odd
                                                    , pg.updated_by   = user
                                                    , pg.updated_date = sysdate 
                         when not matched then insert (drawing_type 
                                                     , seq_id       
                                                     , comb1        
                                                     , comb2        
                                                     , comb3        
                                                     , comb4        
                                                     , comb5        
                                                     , comb6        
                                                     , comb_sum     
                                                     , sum_par_comb 
                                                     , sum_mod_comb 
                                                     , global_index 
                                                     , current_odd  
                                                     , use_cnt 
                                                     , use_flag
                                                     , prime_number_cnt
                                                     , level_comb1
                                                     , level_comb2
                                                     , level_comb3
                                                     , level_comb4
                                                     , level_comb5
                                                     , level_comb6     
                                                     , created_by	
                                                     , creation_date)
                                               values (pg_tbl.drawing_type
                                                     , pg_tbl.seq_id       
                                                     , pg_tbl.comb1        
                                                     , pg_tbl.comb2        
                                                     , pg_tbl.comb3        
                                                     , pg_tbl.comb4        
                                                     , pg_tbl.comb5        
                                                     , pg_tbl.comb6        
                                                     , pg_tbl.comb_sum     
                                                     , pg_tbl.sum_par_comb 
                                                     , pg_tbl.sum_mod_comb 
                                                     , pg_tbl.global_index 
                                                     , pg_tbl.current_odd  
                                                     , pg_tbl.use_cnt
                                                     , pg_tbl.use_flag
                                                     , pg_tbl.prime_number_cnt
                                                     , pg_tbl.level_comb1 
                                                     , pg_tbl.level_comb2
                                                     , pg_tbl.level_comb3
                                                     , pg_tbl.level_comb4
                                                     , pg_tbl.level_comb5
                                                     , pg_tbl.level_comb6
                                                     , user
                                                     , sysdate      
                                                      );
     dbms_output.put_line(sql%rowcount||' rows inserted/updated');    
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;                                                 
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end merge_s_preview_gamblings;
/*
  procedure get_outbound_drawing_data (pv_gambling_type        	               olap_sys.w_combination_responses_fs.attribute3%type
                                     , pn_seq_id                               olap_sys.w_combination_responses_fs.seq_id%type
                                     , xn_comb_sum         in out NOCOPY       olap_sys.w_combination_responses_fs.comb_sum%type
                                     , xn_sum_par_comb     in out NOCOPY       olap_sys.w_combination_responses_fs.sum_par_comb%type
                                     , xn_sum_mod_comb     in out NOCOPY       olap_sys.w_combination_responses_fs.mod3_sum%type
                                     , xn_global_index     in out NOCOPY       olap_sys.w_combination_responses_fs.global_index%type
                                     , xn_prime_number_cnt in out NOCOPY       varchar2 
                                     , xv_level_comb1      in out NOCOPY       varchar2
                                     , xv_level_comb2      in out NOCOPY       varchar2
                                     , xv_level_comb3      in out NOCOPY       varchar2
                                     , xv_level_comb4      in out NOCOPY       varchar2
                                     , xv_level_comb5      in out NOCOPY       varchar2
                                     , xv_level_comb6      in out NOCOPY       varchar2
                                     , x_err_code          in out NOCOPY number       
                                      ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'get_outbound_drawing_data';  
  
  begin
--    dbms_output.put_line('--------------------------------');
--    dbms_output.put_line(LV$PROCEDURE_NAME);
--    dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
--    dbms_output.put_line('pn_seq_id: '||pn_seq_id);
    begin
      select comb_sum
           , sum_par_comb
           , sum_mod_comb
           , global_index
           , prime_number_cnt
           , level_comb1
           , level_comb2
           , level_comb3
           , level_comb4
           , level_comb5
           , level_comb6
        into xn_comb_sum
           , xn_sum_par_comb
           , xn_sum_mod_comb
           , xn_global_index
           , xn_prime_number_cnt
           , xv_level_comb1
           , xv_level_comb2
           , xv_level_comb3
           , xv_level_comb4
           , xv_level_comb5
           , xv_level_comb6
        from olap_sys.s_preview_gamblings
       where drawing_type = pv_gambling_type
         and seq_id       = pn_seq_id;    
    exception 
      when no_data_found then
         select comb_sum
              , sum_par_comb
              , mod3_sum
              , global_index
              , prime_number_cnt
              , level_comb1
              , level_comb2
              , level_comb3
              , level_comb4
              , level_comb5
              , level_comb6
           into xn_comb_sum
              , xn_sum_par_comb
              , xn_sum_mod_comb
              , xn_global_index
              , xn_prime_number_cnt
              , xv_level_comb1
              , xv_level_comb2
              , xv_level_comb3
              , xv_level_comb4
              , xv_level_comb5
              , xv_level_comb6
           from olap_sys.w_combination_responses_fs
          where attribute3 = pv_gambling_type
            and seq_id     = pn_seq_id;
    end;          
    x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;          
  end get_outbound_drawing_data;
*/     
  --[procedure used for reading data from external table and inserting sorted data into stating table                                         
  procedure preview_gamblings_handler (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                     , x_err_code       in out NOCOPY number       
                                      ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'preview_gamblings_handler';  
    ln$comb_sum                   olap_sys.w_combination_responses_fs.comb_sum%type;
    ln$sum_par_comb               olap_sys.w_combination_responses_fs.sum_par_comb%type;
    ln$sum_mod_comb               olap_sys.w_combination_responses_fs.mod3_sum%type;
    ln$global_index               olap_sys.w_combination_responses_fs.global_index%type;
    ln$prime_number_cnt           number;
    ln$comb1                      olap_sys.w_combination_responses_fs.comb1%type := 0;
    ln$comb2                      olap_sys.w_combination_responses_fs.comb2%type := 0;
    ln$comb3                      olap_sys.w_combination_responses_fs.comb3%type := 0;
    ln$comb4                      olap_sys.w_combination_responses_fs.comb4%type := 0;
    ln$comb5                      olap_sys.w_combination_responses_fs.comb5%type := 0;
    ln$comb6                      olap_sys.w_combination_responses_fs.comb6%type := 0;
    ln$seq_id                     olap_sys.w_combination_responses_fs.seq_id%type := 0;
    lv$level_comb1                varchar2(2);
    lv$level_comb2                varchar2(2);
    lv$level_comb3                varchar2(2);
    lv$level_comb4                varchar2(2);
    lv$level_comb5                varchar2(2);
    lv$level_comb6                varchar2(2);
    
    cursor c_ext_inbound_drawings is  
    select pg_ext.comb1
          ,pg_ext.comb2
          ,pg_ext.comb3
          ,pg_ext.comb4
          ,pg_ext.comb5
          ,pg_ext.comb6
          ,pg_ext.odd      
      from olap_sys.t_preview_gamblings_ext pg_ext;

  begin
     dbms_output.put_line('--------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pv_gambling_type: '||pv_gambling_type);
     olap_sys.w_common_pkg.g_rowcnt := 0;
     
        for p in c_ext_inbound_drawings loop
         olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
         ln$comb1            := p.comb1;
         ln$comb2            := p.comb2;
         ln$comb3            := p.comb3;
         ln$comb4            := p.comb4;
         ln$comb5            := p.comb5;
         ln$comb6            := p.comb6;

         olap_sys.w_common_pkg.sort_inbound_comb (x_comb1 => ln$comb1
                                                , x_comb2 => ln$comb2
                                                , x_comb3 => ln$comb3
                                                , x_comb4 => ln$comb4
                                                , x_comb5 => ln$comb5
                                                , x_comb6 => ln$comb6                           
                                                 );

         ln$seq_id := ln$comb1||ln$comb2||ln$comb3||ln$comb4||ln$comb5||ln$comb6;
         
                                                 
         x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
/*
         get_outbound_drawing_data (pv_gambling_type    => pv_gambling_type
                                  , pn_seq_id           => ln$seq_id
                                  , xn_comb_sum         => ln$comb_sum    
                                  , xn_sum_par_comb     => ln$sum_par_comb
                                  , xn_sum_mod_comb     => ln$sum_mod_comb
                                  , xn_global_index     => ln$global_index
                                  , xn_prime_number_cnt => ln$prime_number_cnt
                                  , xv_level_comb1      => lv$level_comb1
                                  , xv_level_comb2      => lv$level_comb2
                                  , xv_level_comb3      => lv$level_comb3
                                  , xv_level_comb4      => lv$level_comb4
                                  , xv_level_comb5      => lv$level_comb5
                                  , xv_level_comb6      => lv$level_comb6
                                  , x_err_code          => x_err_code
                                   );
         
         x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
          
         if x_err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
*/            merge_s_preview_gamblings (pv_gambling_type    => pv_gambling_type
                                     , pn_seq_id           => ln$seq_id
                                     , pn_comb1            => ln$comb1
                                     , pn_comb2            => ln$comb2
                                     , pn_comb3            => ln$comb3
                                     , pn_comb4            => ln$comb4
                                     , pn_comb5            => ln$comb5
                                     , pn_comb6            => ln$comb6
                                     , pn_comb_sum         => ln$comb_sum    
                                     , pn_sum_par_comb     => ln$sum_par_comb
                                     , pn_sum_mod_comb     => ln$sum_mod_comb
                                     , pn_global_index     => ln$global_index
                                     , pn_current_odd      => p.odd
                                     , pn_use_cnt          => 1
                                     , pn_prime_number_cnt => ln$prime_number_cnt
                                     , pv_level_comb1      => lv$level_comb1
                                     , pv_level_comb2      => lv$level_comb2
                                     , pv_level_comb3      => lv$level_comb3
                                     , pv_level_comb4      => lv$level_comb4
                                     , pv_level_comb5      => lv$level_comb5
                                     , pv_level_comb6      => lv$level_comb6
                                     , x_err_code          => x_err_code
                                      );
            if x_err_code != OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
               dbms_output.put_line('exit');
               exit;                 
            end if;   
            
 --        end if; 
         if mod(olap_sys.w_common_pkg.g_rowcnt,100) = 0 then
            olap_sys.w_common_pkg.do_commit;
         end if;                          
     end loop;                          
     dbms_output.put_line(olap_sys.w_common_pkg.g_rowcnt||' rows retrieved from external table.');
     if x_err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then                         
        olap_sys.w_common_pkg.do_commit;
     end if;                                                                                                      

  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end preview_gamblings_handler;

  procedure build_global_index_str (pv_gambling_type  	              olap_sys.sl_gamblings.gambling_type%type
                                  , pv_gambling_day                   varchar2
                                  , xv_global_index_str in out NOCOPY varchar2
                                  , x_err_code          in out NOCOPY number       
                                    ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'build_global_index_str';  
    ln$drawings_to_play_cnt       number := 0;
    le$not_enough_drawings        exception;
    pragma exception_init (le$not_enough_drawings, -20012);
    
    cursor c_drawings (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                     , pn_drawings_to_play_cnt        number
                      ) is
    with preview_gamblings_tbl as (
    select global_index 
      from olap_sys.s_preview_gamblings
     where drawing_type = pv_gambling_type
       and (trunc(creation_date) = trunc(sysdate) or trunc(execution_updated_date) = trunc(sysdate))
--       and execution_status = 'Y'
       and current_odd > 0
     order by current_odd desc
         , use_cnt desc 
         , dbms_random.RANDOM)
     select global_index
       from preview_gamblings_tbl
      where rownum <= pn_drawings_to_play_cnt 
     ; 
                                  
  begin
  
     ln$drawings_to_play_cnt := olap_sys.w_common_pkg.get_sum_drawings_per_day (pv_drawing_type => pv_gambling_type
                                                                              , pv_gambling_day => pv_gambling_day);

     xv_global_index_str            := 'GLOBAL_INDEX IN (';
     olap_sys.w_common_pkg.g_rowcnt := 0;
     for t in c_drawings (pv_gambling_type  	  => pv_gambling_type
                        , pn_drawings_to_play_cnt => ln$drawings_to_play_cnt
                         ) loop
         xv_global_index_str := xv_global_index_str ||t.global_index||',';
         olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
     end loop;
     dbms_output.put_line('# drawings to play: '||olap_sys.w_common_pkg.g_rowcnt);
     
     if olap_sys.w_common_pkg.g_rowcnt < ln$drawings_to_play_cnt then 
        raise le$not_enough_drawings;
     else
        xv_global_index_str := substr(xv_global_index_str,1,length(xv_global_index_str)-1)||')';
        x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     end if;        
  exception
    when le$not_enough_drawings then
      x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||'Number of drawings to play is lower than '||ln$drawings_to_play_cnt);    

      raise;          
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end build_global_index_str;
  
  procedure merge_metadata_select_headers (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                         , pv_gambling_day                varchar2
                                         , pv_global_index_str            varchar2
                                         , x_err_code       in out NOCOPY number       
                                           ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'merge_metadata_select_headers';   
  begin
  
     merge into olap_sys.s_metadata_select_headers ms using
     (with drawings_tbl as (select pv_gambling_type             drawing_type 
                                 , pv_gambling_day              drawing_day       
                                 , 'ELEGIBLE_FLAG IN ('||chr(39)||'N'||','||chr(39)||'Y'||chr(39)||')' attribute10        
                                 , pv_global_index_str          attribute12        
                                 , 'A'                          status        
                           from dual)
                           select drawing_type
                                , drawing_day      
                                , attribute10       
                                , attribute12       
                                , status       
                             from drawings_tbl) ms_tbl
                         on (ms_tbl.drawing_type   = ms.drawing_type
                            and ms_tbl.drawing_day = ms.drawing_day)
                         when matched then update set ms.attribute12  = ms_tbl.attribute12
                                                    , ms.updated_by   = USER
                                                    , ms.updated_date = SYSDATE 
                         when not matched then insert (drawing_type 
                                                     , drawing_day       
                                                     , attribute10        
                                                     , attribute12        
                                                     , status        
                                                     , created_by	
                                                     , creation_date)
                                               values (ms_tbl.drawing_type
                                                     , ms_tbl.drawing_day       
                                                     , ms_tbl.attribute10        
                                                     , ms_tbl.attribute12        
                                                     , ms_tbl.status        
                                                     , USER
                                                     , SYSDATE      
                                                      );
  
     dbms_output.put_line(sql%rowcount||' rows inserted/updated');    
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;                                                 
  
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end merge_metadata_select_headers;
  
  --[procedure used for setting up metadata headers on table s_metadata_select_headers in order to split the drawings among active players                                         
  procedure setup_metadata_headers_handler (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                          , pv_gambling_day                varchar2
                                          , x_err_code       in out NOCOPY number       
                                           ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'setup_metadata_headers_handler';
    lv$global_index_str           varchar2(500);  
  begin
     build_global_index_str (pv_gambling_type  	 => pv_gambling_type 
                           , pv_gambling_day     => pv_gambling_day  
                           , xv_global_index_str => lv$global_index_str       
                           , x_err_code          => x_err_code
                            );
     
     if x_err_code = OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
        merge_metadata_select_headers (pv_gambling_type    => pv_gambling_type
                                     , pv_gambling_day     => pv_gambling_day
                                     , pv_global_index_str => lv$global_index_str 
                                     , x_err_code          => x_err_code      
                                      );
     end if;                              
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end setup_metadata_headers_handler;

  procedure upd_gigaloterias_patterns (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                     , pv_pattern_type                varchar2
                                     , pn_pattern1                    number
                                     , pn_pattern2                    number
                                     , pn_pattern3                    number
                                     , pn_pattern4                    number
                                     , pn_pattern5                    number
                                     , pn_pattern6                    number
                                     , pn_rowcount                    number
                                     , x_err_code       in out NOCOPY number       
                                     ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_gigaloterias_patterns';  
  begin                           
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pat1: '||pn_pattern1);
     dbms_output.put_line('pat2: '||pn_pattern2);
     dbms_output.put_line('pat3: '||pn_pattern3);
     dbms_output.put_line('pat4: '||pn_pattern4);
     dbms_output.put_line('pat5: '||pn_pattern5);
     dbms_output.put_line('pat6: '||pn_pattern6);
             
     update olap_sys.s_gigaloterias_patterns
        set xrowcount    = pn_rowcount
          , updated_by   = USER
          , updated_date = SYSDATE
      where drawing_type = pv_gambling_type
        and pattern_type = pv_pattern_type
        and pattern1 = pn_pattern1
        and pattern2 = pn_pattern2
        and pattern3 = pn_pattern3
        and pattern4 = pn_pattern4
        and pattern5 = pn_pattern5
        and pattern6 = pn_pattern6;
       dbms_output.put_line(sql%rowcount||' rows updated');

     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;                    
  end upd_gigaloterias_patterns;

  procedure gigaloterias_patterns_wrapper (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                 number
                                         , x_err_code       in out NOCOPY number       
                                          ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'gigaloterias_patterns_wrapper';
    ln$p1         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;
    ln$p2         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;
    ln$p3         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;
    ln$p4         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;
    ln$p5         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;
    ln$p6         olap_sys.s_calculo_stats.color_ley_tercio%type := 0;                                
    ln$rowcount   number := 0;                                
    
    procedure upd_LT_gigaloterias_patterns (pv_gambling_type  	            olap_sys.sl_gamblings.gambling_type%type
                                           , pn_gambling_id                 number
                                           , pv_pattern_type                varchar2
                                           , x_err_code       in out NOCOPY number       
                                            ) is
       LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_LT_gigaloterias_patterns';
    begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
        WITH drawings_dtl AS (
        SELECT nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1'),0) lt1,
               nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2'),0) lt2,
               nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3'),0) lt3,
               nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4'),0) lt4,
               nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5'),0) lt5,
               nvl((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6'),0) lt6
          FROM olap_sys.sl_gamblings
         WHERE gambling_type = pv_gambling_type
           AND gambling_id = pn_gambling_id
        )
        SELECT dd.lt1
             , dd.lt2
             , dd.lt3
             , dd.lt4
             , dd.lt5
             , dd.lt6
             , count(1) cnt
          INTO ln$p1 
             , ln$p2
             , ln$p3
             , ln$p4
             , ln$p5
             , ln$p6
             , ln$rowcount
        FROM drawings_dtl dd
        group by dd.lt1
               , dd.lt2 
               , dd.lt3 
               , dd.lt4 
               , dd.lt5 
               , dd.lt6;
               
        upd_gigaloterias_patterns (pv_gambling_type => pv_gambling_type
                                 , pv_pattern_type  => 'LT' 
                                 , pn_pattern1      => ln$p1
                                 , pn_pattern2      => ln$p2
                                 , pn_pattern3      => ln$p3
                                 , pn_pattern4      => ln$p4
                                 , pn_pattern5      => ln$p5
                                 , pn_pattern6      => ln$p6
                                 , pn_rowcount      => ln$rowcount
                                 , x_err_code       => x_err_code);
  exception
    when no_data_found then
	  x_err_code := sqlcode;
	  dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

	  raise;                                
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;               
    end upd_LT_gigaloterias_patterns;

     procedure upd_UB_gigaloterias_patterns (pv_gambling_type  	            olap_sys.sl_gamblings.gambling_type%type
                                           , pn_gambling_id                 number
                                           , pv_pattern_type                varchar2
                                           , x_err_code       in out NOCOPY number       
                                            ) is
       LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_UB_gigaloterias_patterns';
    begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);    
        WITH drawings_dtl AS (
        SELECT nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1'),0) cu1,  
               nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2'),0) cu2,  
               nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3'),0) cu3,  
               nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4'),0) cu4,  
               nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5'),0) cu5,  
               nvl((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6'),0) cu6
          FROM olap_sys.sl_gamblings
         WHERE gambling_type = pv_gambling_type
           AND gambling_id   = pn_gambling_id
        )
        SELECT dd.cu1
             , dd.cu2
             , dd.cu3
             , dd.cu4
             , dd.cu5
             , dd.cu6
             , count(1) cnt
          INTO ln$p1 
             , ln$p2
             , ln$p3
             , ln$p4
             , ln$p5
             , ln$p6
             , ln$rowcount
        FROM drawings_dtl dd
        group by dd.cu1
               , dd.cu2 
               , dd.cu3 
               , dd.cu4 
               , dd.cu5 
               , dd.cu6;

        upd_gigaloterias_patterns (pv_gambling_type => pv_gambling_type
                                 , pv_pattern_type  => 'UB' 
                                 , pn_pattern1      => ln$p1
                                 , pn_pattern2      => ln$p2
                                 , pn_pattern3      => ln$p3
                                 , pn_pattern4      => ln$p4
                                 , pn_pattern5      => ln$p5
                                 , pn_pattern6      => ln$p6
                                 , pn_rowcount      => ln$rowcount
                                 , x_err_code       => x_err_code);

  exception
    when no_data_found then
	  x_err_code := sqlcode;
	  dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

	  raise;                                
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
 
      raise;               
    end upd_UB_gigaloterias_patterns;
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);

     upd_LT_gigaloterias_patterns (pv_gambling_type => pv_gambling_type
                                 , pn_gambling_id   => pn_gambling_id  
                                 , pv_pattern_type  => 'LT' 
                                 , x_err_code       => x_err_code      
                                  ); 

     upd_UB_gigaloterias_patterns (pv_gambling_type => pv_gambling_type
                                 , pn_gambling_id   => pn_gambling_id  
                                 , pv_pattern_type  => 'UB' 
                                 , x_err_code       => x_err_code      
                                  ); 

  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;           
  end gigaloterias_patterns_wrapper;                                      

  function is_enable_gigaloterias_pattern (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id             number) return varchar2 is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'is_enable_gigaloterias_pattern';
                                         
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);

    olap_sys.w_common_pkg.g_rowcnt := 0;
    select count(1)
      into olap_sys.w_common_pkg.g_rowcnt 
      from olap_sys.s_calculo_stats gs 
     where gs.drawing_type= pv_gambling_type 
       and gs.drawing_id  = pn_gambling_id-1
       and gs.winner_flag is not null;
    
    if olap_sys.w_common_pkg.g_rowcnt = 0 then
       return 'N';
    else
       return 'Y';
    end if;         
  end is_enable_gigaloterias_pattern;                                       
  
  --[procedure used for updating counts on gigaloteria patterns   
  procedure gigaloterias_patterns_handler (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                 number
                                         , x_err_code       in out NOCOPY number       
                                        ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'gigaloterias_patterns_handler';
                                        
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);

--     if is_enable_gigaloterias_pattern (pv_gambling_type => pv_gambling_type
--                                      , pn_gambling_id   => pn_gambling_id) = 'Y' then
        gigaloterias_patterns_wrapper (pv_gambling_type => pv_gambling_type
                                     , pn_gambling_id   => pn_gambling_id  
                                     , x_err_code       => x_err_code      
                                      );
--     end if;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;      
  end gigaloterias_patterns_handler;                                      

  procedure upd_s_gi_comb_sum_stats (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                   , pn_avg_global_index_low     number
                                   , pn_avg_global_index_high    number 
                                   , pn_comb_sum                 number
                                   , pn_drawing_id               number 
                                   , x_err_code                in out nocopy number  
                                   ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'upd_s_gi_comb_sum_stats';
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);

    update olap_sys.s_gi_comb_sum_stats
       set in_cnt          = in_cnt + 1
         , last_drawing_id = pn_drawing_id 
         , updated_by      = user
         , updated_date    = sysdate
     where drawing_type = pv_gambling_type
       and nvl(avg_global_index_low,-1)  = nvl(pn_avg_global_index_low,-1)
       and nvl(avg_global_index_high,-1) = nvl(pn_avg_global_index_high,-1) 
       and comb_sum                       = pn_comb_sum;
    
    dbms_output.put_line(sql%rowcount||' rows updated.');
    if sql%found then
      x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;      
    else
      x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION; 
    end if;      
  end upd_s_gi_comb_sum_stats;
  
  procedure get_global_index_range (pn_global_index                         number
                                  , xn_avg_global_index_low   in out nocopy number
                                  , xn_avg_global_index_high  in out nocopy number 
                                  , x_err_code                in out nocopy number  
                                   ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'get_global_index_range';
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('pn_global_index: '||pn_global_index);
     
     if pn_global_index > olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'HIGH') then
        xn_avg_global_index_low := null;
        xn_avg_global_index_high := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'HIGH'));
     elsif pn_global_index between olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'AVG') and 
                                   olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'HIGH') then
        xn_avg_global_index_low := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'AVG'));
        xn_avg_global_index_high := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'HIGH'));
     elsif pn_global_index between olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'LOW') and
                                   olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'AVG') then
        xn_avg_global_index_low := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'LOW'));
        xn_avg_global_index_high := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'AVG'));
     elsif pn_global_index < olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'LOW') then
        xn_avg_global_index_low := round(olap_sys.w_common_pkg.get_avg_global_index (pv_type=>'LOW'));
        xn_avg_global_index_high := null;
     end if;   
     
     dbms_output.put_line('xn_avg_global_index_low: '||xn_avg_global_index_low);
     dbms_output.put_line('xn_avg_global_index_high: '||xn_avg_global_index_high);
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;               
  end get_global_index_range;

  --[ main procedure used for computing stats based on general_index and comb_sum 
  procedure get_gi_comb_sum_stats_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                 number
                                         , pn_comb1                       number
                                         , pn_comb2                       number
                                         , pn_comb3                       number
                                         , pn_comb4                       number
                                         , pn_comb5                       number
                                         , pn_comb6                       number
                                         , x_err_code       in out NOCOPY number       
                                          ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'get_gi_comb_sum_stats_handler';
    ln$comb_sum                   olap_sys.s_gi_comb_sum_stats.comb_sum%type;
    ln$global_index               number := 0;
    ln$avg_global_index_low       number := 0; 
    ln$avg_global_index_high      number := 0;
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line(pn_comb1||' - '||pn_comb2||' - '||pn_comb3||' - '||pn_comb4||' - '||pn_comb5||' - '||pn_comb6);
     
     ln$comb_sum      := pn_comb1 + pn_comb2 + pn_comb3 + pn_comb4 + pn_comb5 + pn_comb6;
     
     dbms_output.put_line('ln$comb_sum: '||ln$comb_sum);
     
     ln$global_index := olap_sys.w_common_pkg.get_usr_global_index (pv_drawing_type => pv_gambling_type
                                                                  , pn_seq_id       => pn_comb1||pn_comb2||pn_comb3||pn_comb4||pn_comb5||pn_comb6);
                                                                      
     get_global_index_range (pn_global_index          => ln$global_index
                           , xn_avg_global_index_low  => ln$avg_global_index_low
                           , xn_avg_global_index_high => ln$avg_global_index_high
                           , x_err_code               => x_err_code 
                            );
     
     if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then                       
        upd_s_gi_comb_sum_stats (pv_gambling_type  	  => pv_gambling_type
                               , pn_avg_global_index_low  => ln$avg_global_index_low
                               , pn_avg_global_index_high => ln$avg_global_index_high
                               , pn_comb_sum              => ln$comb_sum
                               , pn_drawing_id            => pn_gambling_id
                               , x_err_code               => x_err_code
                                );                       
     end if;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end get_gi_comb_sum_stats_handler;
  
  procedure ins_s_inbound_digit_stats (pv_drawing_type	       olap_sys.s_inbound_digit_stats.drawing_type%type
                                     , pn_last_drawing_id       olap_sys.s_inbound_digit_stats.last_drawing_id%type
                                     , pv_b_type                olap_sys.s_inbound_digit_stats.b_type%type
                                     , pn_last_digit            olap_sys.s_inbound_digit_stats.last_digit%type
                                     , pn_next_digit            olap_sys.s_inbound_digit_stats.next_digit%type
                                     , pn_xrowcount             olap_sys.s_inbound_digit_stats.xrowcount%type
                                     , x_err_code              in out NOCOPY number      
                                      ) is
    LV$PROCEDURE_NAME         constant varchar2(30) := 'ins_s_inbound_digit_stats';
  begin
     dbms_output.put_line('----------------------------------');   
     dbms_output.put_line(LV$PROCEDURE_NAME);   
--     dbms_output.put_line('pv_drawing_type: '||pv_drawing_type);
     dbms_output.put_line('pn_last_drawing_id: '||pn_last_drawing_id);
     dbms_output.put_line('pv_b_type: '||pv_b_type);
     dbms_output.put_line('pn_last_digit: '||pn_last_digit);
     dbms_output.put_line('pn_next_digit: '||pn_next_digit);
     dbms_output.put_line('pn_xrowcount: '||pn_xrowcount);

     insert into olap_sys.s_inbound_digit_stats (drawing_type	
                                               , last_drawing_id
                                               , b_type         
                                               , last_digit     
                                               , next_digit     
                                               , xrowcount      
                                               , selected_flag  
                                               , winner_flag    
                                               , created_by     
                                               , creation_date 	
                                               )  
     select pv_drawing_type	
          , pn_last_drawing_id
          , pv_b_type         
          , pn_last_digit     
          , pn_next_digit     
          , pn_xrowcount
          , 'N'
          , NULL
          , USER
          , SYSDATE
       from dual
      where not exists (select 1
                          from olap_sys.s_inbound_digit_stats
                         where drawing_type    = pv_drawing_type	
                           and last_drawing_id = pn_last_drawing_id
                           and b_type          = pv_b_type    
                           and next_digit      = pn_next_digit

                       );
     dbms_output.put_line(sql%rowcount||' rows inserted.');                           
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end ins_s_inbound_digit_stats;
    
  --[procedure used for computing stats for next digit value baed on current digit value provided as input parameter
  procedure compute_digit_stats (pv_drawing_type                       varchar2
                               , pn_gambling_id                        number 
                               , p_in_tbl                              gt$inbound_tbl 
                               , pn_comb                               number default null
                               , pn_comb_position                      number default null
                               , x_cnt_tbl               in out NOCOPY gt$inb_cnt_tbl
                               , x_err_code              in out NOCOPY number
                                ) is
    LV$PROCEDURE_NAME         constant varchar2(30) := 'compute_digit_stats';
    type l$digit_tbl is table of number(2);
    l$digit1_list   l$digit_tbl;  
    l$digit2_list   l$digit_tbl;
    l$digit3_list   l$digit_tbl;
    l$digit4_list   l$digit_tbl;
    l$digit5_list   l$digit_tbl;
    l$digit6_list   l$digit_tbl;
    
    cursor c_outputs is
    select attribute1 drawing_id
       , attribute95 comb
       , attribute2 current_digit
       , attribute3 next_digit
       , attribute4 cnt
       , attribute5 percentaje
    from olap_sys.tmp_computed_values
   where attribute4 > 0 
   order by attribute5 desc, attribute3 desc;
  begin
     dbms_output.put_line('----------------------------------');   
     dbms_output.put_line(LV$PROCEDURE_NAME);   
     olap_sys.w_common_pkg.g_index := 0;
     olap_sys.w_common_pkg.g_rowcnt := p_in_tbl.count;
     
     --initializing collections
     l$digit1_list := l$digit_tbl(1,10);
     l$digit2_list := l$digit_tbl(3,20);
     l$digit3_list := l$digit_tbl(8,25);
     l$digit4_list := l$digit_tbl(16,31);
     l$digit5_list := l$digit_tbl(21,37);
     l$digit6_list := l$digit_tbl(31,39);
     x_cnt_tbl.delete;
     
     delete olap_sys.tmp_computed_values;
     dbms_output.put_line('B1: '||l$digit1_list(l$digit1_list.first)||' '||l$digit1_list(l$digit1_list.last));
     dbms_output.put_line('B2: '||l$digit2_list(l$digit2_list.first)||' '||l$digit2_list(l$digit2_list.last));
     dbms_output.put_line('B3: '||l$digit3_list(l$digit3_list.first)||' '||l$digit3_list(l$digit3_list.last));
     dbms_output.put_line('B4: '||l$digit4_list(l$digit4_list.first)||' '||l$digit4_list(l$digit4_list.last));
     dbms_output.put_line('B5: '||l$digit5_list(l$digit5_list.first)||' '||l$digit5_list(l$digit5_list.last));
     dbms_output.put_line('B6: '||l$digit6_list(l$digit6_list.first)||' '||l$digit6_list(l$digit6_list.last));
     dbms_output.put_line('GN$DEFAULT_LOW_DIGIT: '||GN$DEFAULT_LOW_DIGIT);
     dbms_output.put_line('GN$DEFAULT_HIGH_DIGIT: '||GN$DEFAULT_HIGH_DIGIT);
     
     --initialize plsql table
     if pn_comb_position = 1 then      
        for i in l$digit1_list(l$digit1_list.first)..l$digit1_list(l$digit1_list.last)+1 loop
            x_cnt_tbl(i).attribute  := 'B1';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            if i > l$digit1_list(l$digit1_list.last) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_HIGH_DIGIT;
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;
     elsif pn_comb_position = 2 then
        for i in l$digit2_list(l$digit2_list.first)-1..l$digit2_list(l$digit2_list.last)+1 loop
            x_cnt_tbl(i).attribute  := 'B2';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            
            if i < l$digit2_list(l$digit2_list.first) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_LOW_DIGIT;
            elsif i > l$digit2_list(l$digit2_list.last) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_HIGH_DIGIT;    
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;   
     elsif pn_comb_position = 3 then
        for i in l$digit3_list(l$digit3_list.first)-1..l$digit3_list(l$digit3_list.last)+1 loop
            x_cnt_tbl(i).attribute  := 'B3';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            
            if i < l$digit3_list(l$digit3_list.first) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_LOW_DIGIT;
            elsif i > l$digit3_list(l$digit3_list.last) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_HIGH_DIGIT;    
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;   
     elsif pn_comb_position = 4 then
        for i in l$digit4_list(l$digit4_list.first)-1..l$digit4_list(l$digit4_list.last)+1 loop
            x_cnt_tbl(i).attribute  := 'B4';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            
            if i < l$digit4_list(l$digit4_list.first) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_LOW_DIGIT;
            elsif i > l$digit4_list(l$digit4_list.last) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_HIGH_DIGIT;    
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;   
     elsif pn_comb_position = 5 then
        for i in l$digit5_list(l$digit5_list.first)-1..l$digit5_list(l$digit5_list.last)+1 loop
            x_cnt_tbl(i).attribute  := 'B5';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            
            if i < l$digit5_list(l$digit5_list.first) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_LOW_DIGIT;
            elsif i > l$digit5_list(l$digit5_list.last) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_HIGH_DIGIT;    
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;         
     elsif pn_comb_position = 6 then
        for i in l$digit6_list(l$digit6_list.first)-1..l$digit6_list(l$digit6_list.last) loop
            x_cnt_tbl(i).attribute  := 'B6';
            x_cnt_tbl(i).cur_value1 := pn_comb;
            
            if i < l$digit6_list(l$digit6_list.first) then
               x_cnt_tbl(i).next_value1 := GN$DEFAULT_LOW_DIGIT;
            else   
              x_cnt_tbl(i).next_value1 := i;
            end if;       
            x_cnt_tbl(i).cnt       := 0;
        end loop;   
     end if;
     
     --[counting next drawing id
     for k in p_in_tbl.first..p_in_tbl.last loop
         if k = p_in_tbl.last then
            exit;
         end if;   
         --[next drawing id
         olap_sys.w_common_pkg.g_index := k + 1;
         
         if pn_comb_position = 1 then
            if p_in_tbl(k).comb1 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb1 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb1 > l$digit1_list(l$digit1_list.last) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_HIGH_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;   
                   end if; 
               end loop;
            end if;
         elsif pn_comb_position = 2 then
            if p_in_tbl(k).comb2 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb2 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb2 < l$digit2_list(l$digit2_list.first) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_LOW_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb2 > l$digit2_list(l$digit2_list.last) then      
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_HIGH_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;   
                      end if;   
                   end if; 
               end loop;
            end if;          
         elsif pn_comb_position = 3 then
            if p_in_tbl(k).comb3 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb3 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb3 < l$digit3_list(l$digit3_list.first) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_LOW_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb3 > l$digit3_list(l$digit3_list.last) then      
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_HIGH_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;   
                      end if;   
                   end if; 
               end loop;
            end if;          
         elsif pn_comb_position = 4 then
            if p_in_tbl(k).comb4 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb4 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb4 < l$digit4_list(l$digit4_list.first) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_LOW_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb4 > l$digit4_list(l$digit4_list.last) then      
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_HIGH_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;   
                      end if;   
                   end if; 
               end loop;
            end if;
         elsif pn_comb_position = 5 then
            if p_in_tbl(k).comb5 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb5 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb5 < l$digit5_list(l$digit5_list.first) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_LOW_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb5 > l$digit5_list(l$digit5_list.last) then      
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_HIGH_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;   
                      end if;   
                   end if; 
               end loop;
            end if;          
         elsif pn_comb_position = 6 then
            if p_in_tbl(k).comb6 = pn_comb then
               for m in x_cnt_tbl.first..x_cnt_tbl.last loop
                   if p_in_tbl(olap_sys.w_common_pkg.g_index).comb6 = x_cnt_tbl(m).next_value1 then
                      x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                   elsif p_in_tbl(olap_sys.w_common_pkg.g_index).comb6 < l$digit6_list(l$digit6_list.first) then
                      if x_cnt_tbl(m).next_value1 = GN$DEFAULT_LOW_DIGIT then
                         x_cnt_tbl(m).cnt := x_cnt_tbl(m).cnt + 1;
                      end if;   
                   end if; 
               end loop;
            end if;          
         end if;
     end loop;
     
     --[inserting values into temporal table for sorting data
     gn$array_index := 0;
     for p in x_cnt_tbl.first..x_cnt_tbl.last loop
        --[ saving computed values into staging table
        ins_s_inbound_digit_stats (pv_drawing_type    => pv_drawing_type
                                 , pn_last_drawing_id => pn_gambling_id
                                 , pv_b_type          => x_cnt_tbl(p).attribute
                                 , pn_last_digit      => pn_comb
                                 , pn_next_digit      => x_cnt_tbl(p).next_value1
                                 , pn_xrowcount       => x_cnt_tbl(p).cnt
                                 , x_err_code         => x_err_code
                                  );
     end loop;
            
     dbms_output.put_line('drawing_id~comb~current_digit~next_digit~cnt~percentaje');
     --[print inbound for counts   
     for t in c_outputs loop
        dbms_output.put_line(t.drawing_id||'~'||t.comb||'~'||t.current_digit||'~'||t.next_digit||'~'||t.cnt||'~'||t.percentaje);
     end loop;
     
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  EXCEPTION                 
    when others THEN  	
      x_err_code := sqlcode;
      dbms_output.put_line(LV$PROCEDURE_NAME||'. others: '||sqlerrm);	
  END compute_digit_stats;


  procedure load_inbound_data (pv_drawing_type                       varchar2
                             , x_in_tbl                in out NOCOPY gt$inbound_tbl 
                             , x_err_code              in out NOCOPY number
                              ) is
    LV$PROCEDURE_NAME         constant varchar2(30) := 'load_inbound_data';
    ln$drawing_id_ini                  number := 0;
  begin
     dbms_output.put_line('----------------------------------');   
     dbms_output.put_line(LV$PROCEDURE_NAME);
     
     x_in_tbl.delete;
       
     select gambling_id
          , comb1
          , comb2
          , comb3
          , comb4
          , comb5
          , comb6
          , comb_sum
          , sum_par_comb
          , sum_mod_comb
          , global_index
          , prime_number_cnt
       bulk collect into x_in_tbl   
       from olap_sys.sl_gamblings
      where gambling_type = pv_drawing_type
      order by gambling_id;
  
     dbms_output.put_line('x_in_tbl.count: '||x_in_tbl.count);
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  
  EXCEPTION                 
    when others THEN  	
      x_err_code := sqlcode;
      dbms_output.put_line(LV$PROCEDURE_NAME||'. others: '||sqlerrm);	
  END load_inbound_data;

  --[ main procedure used for computing stats based on general_index and comb_sum 
  procedure ins_last_inbound_digit_stats (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                        , pn_gambling_id                 number
                                        , pn_comb1                       number
                                        , pn_comb2                       number
                                        , pn_comb3                       number
                                        , pn_comb4                       number
                                        , pn_comb5                       number
                                        , pn_comb6                       number
                                        , x_err_code       in out NOCOPY number       
                                         ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'ins_last_inbound_digit_stats';
    l$in_tbl                      gt$inbound_tbl;
    l$inb_cnt_tbl                 gt$inb_cnt_tbl;
    ln$comb                       number := 0;
    ln$comb_position              number := 0; 
  begin
     dbms_output.put_line('----------------------------------');   
     dbms_output.put_line(LV$PROCEDURE_NAME);

     load_inbound_data (pv_drawing_type    => pv_gambling_type   
                      , x_in_tbl           => l$in_tbl          
                      , x_err_code         => x_err_code        
                       );     

     if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
        for k in 1..6 loop
            if k = 1 then
               ln$comb := pn_comb1;        
            elsif k = 2 then
               ln$comb := pn_comb2;
            elsif k = 3 then
               ln$comb := pn_comb3;
            elsif k = 4 then
               ln$comb := pn_comb4;
            elsif k = 5 then
               ln$comb := pn_comb5;
            elsif k = 6 then
               ln$comb := pn_comb6;
            end if;   
            
            compute_digit_stats (pv_drawing_type   => pv_gambling_type
                                , pn_gambling_id   => pn_gambling_id
                                , p_in_tbl         => l$in_tbl
                                , pn_comb          => ln$comb
                                , pn_comb_position => k 
                                , x_cnt_tbl        => l$inb_cnt_tbl
                                --, x_tmp_tbl        => lt$tmp_tbl  
                                , x_err_code       => x_err_code
                                 );
        end loop;
     end if;                        
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end ins_last_inbound_digit_stats;

  --[ main procedure used for computing inbound digit counts and inserting computed data into staging table 
  procedure inbound_digit_stat_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                      , pn_gambling_id                 number
                                      , pn_comb1                       number
                                      , pn_comb2                       number
                                      , pn_comb3                       number
                                      , pn_comb4                       number
                                      , pn_comb5                       number
                                      , pn_comb6                       number
                                      , x_err_code       in out NOCOPY number       
                                       ) is
    LV$PROCEDURE_NAME    constant varchar2(30) := 'inbound_digit_stat_handler';
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line(pn_gambling_id||' - '||pn_comb1||' - '||pn_comb2||' - '||pn_comb3||' - '||pn_comb4||' - '||pn_comb5||' - '||pn_comb6);
     ins_last_inbound_digit_stats (pv_gambling_type => pv_gambling_type
                                 , pn_gambling_id   => pn_gambling_id    
                                 , pn_comb1         => pn_comb1          
                                 , pn_comb2         => pn_comb2          
                                 , pn_comb3         => pn_comb3          
                                 , pn_comb4         => pn_comb4          
                                 , pn_comb5         => pn_comb5          
                                 , pn_comb6         => pn_comb6          
                                 , x_err_code       => x_err_code              
                                  );
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end inbound_digit_stat_handler;
/* 
  procedure ins_drawings_comparisons_IN (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                       , pn_gambling_id                 number
                                       , pn_seqno                       number
                                       , pv_type                        varchar2
                                       , x_err_code       in out NOCOPY number
                                       ) is
     LV$PROCEDURE_NAME    constant varchar2(30) := 'ins_drawings_comparisons_IN';
  
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     
	INSERT INTO OLAP_SYS.S_DRAWINGS_COMPARISONS(
	 DRAWING_TYPE,
	 SEQNO,
	 TYPE,
	 DRAWING_ID,
	 COMB1, 
	 COMB2, 
	 COMB3, 
	 COMB4, 
	 COMB5, 
	 COMB6, 
	 ADDITIONAL, 
	 SUM_PAR_COMB, 
	 SUM_MOD_COMB, 
	 GLOBAL_INDEX,
	 COLOR_LEY_TERCIO_C1,
	 COLOR_LEY_TERCIO_C2,
	 COLOR_LEY_TERCIO_C3,
	 COLOR_LEY_TERCIO_C4,
	 COLOR_LEY_TERCIO_C5,
	 COLOR_LEY_TERCIO_C6,
	 CICLO_APARICION_C1, 
	 CICLO_APARICION_C2, 
	 CICLO_APARICION_C3, 
	 CICLO_APARICION_C4, 
	 CICLO_APARICION_C5, 
	 CICLO_APARICION_C6, 
	 PRONOS_CICLO_C1, 
	 PRONOS_CICLO_C2, 
	 PRONOS_CICLO_C3, 
	 PRONOS_CICLO_C4, 
	 PRONOS_CICLO_C5, 
	 PRONOS_CICLO_C6, 
	 ON1, 
	 ON2, 
	 ON3, 
	 ON4, 
	 ON5, 
	 ON6, 
	 PN1, 
	 PN2, 
	 PN3, 
	 PN4, 
	 PN5, 
	 PN6,
	 CREATED_BY, 
	 CREATION_DATE 
	 )
	 WITH drawings_dtl AS (
	SELECT gambling_id,
	     comb1,
	     comb2,
	     comb3,
	     comb4,
	     comb5,
	     comb6,
	     additional,
	     comb_sum,
	     sum_par_comb,
	     sum_mod_comb,
	     week_day,
	     global_index,
	     to_number(to_char(to_date(gambling_date,'DD-MM-YYYY'),'YYYY')) year,
	     to_number(to_char(to_date(gambling_date,'DD-MM-YYYY'),'Q')) qtr,
	     prime_number_cnt,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) clt1,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2' and gs.winner_flag is not null) clt2,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3' and gs.winner_flag is not null) clt3,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4' and gs.winner_flag is not null) clt4,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5' and gs.winner_flag is not null) clt5,
	     (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6' and gs.winner_flag is not null) clt6,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) nclt1,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2' and gs.winner_flag is not null) nclt2,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3' and gs.winner_flag is not null) nclt3,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4' and gs.winner_flag is not null) nclt4,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5' and gs.winner_flag is not null) nclt5,
	     (select nvl(gs.color_ley_tercio,0) from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6' and gs.winner_flag is not null) nclt6,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) lt1,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2' and gs.winner_flag is not null) lt2,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3' and gs.winner_flag is not null) lt3,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4' and gs.winner_flag is not null) lt4,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5' and gs.winner_flag is not null) lt5,
	     (select gs.ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6' and gs.winner_flag is not null) lt6,
	     (select gs.ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) ubi1,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) c1_ca,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2' and gs.winner_flag is not null) c2_ca,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3' and gs.winner_flag is not null) c3_ca,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4' and gs.winner_flag is not null) c4_ca,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5' and gs.winner_flag is not null) c5_ca,
	     (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6' and gs.winner_flag is not null) c6_ca,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb1 and gs.b_type='B1' and gs.winner_flag is not null) pxc1,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb2 and gs.b_type='B2' and gs.winner_flag is not null) pxc2,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb3 and gs.b_type='B3' and gs.winner_flag is not null) pxc3,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb4 and gs.b_type='B4' and gs.winner_flag is not null) pxc4,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb5 and gs.b_type='B5' and gs.winner_flag is not null) pxc5,
	     (select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=gambling_type and gs.drawing_id=gambling_id-1 and gs.digit=comb6 and gs.b_type='B6' and gs.winner_flag is not null) pxc6,
	     mod(comb1,2) pi1,
	     mod(comb2,2) pi2,
	     mod(comb3,2) pi3,
	     mod(comb4,2) pi4,
	     mod(comb5,2) pi5,
	     mod(comb6,2) pi6,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb1) pn1,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb2) pn2,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb3) pn3,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb4) pn4,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb5) pn5,
	     olap_sys.w_common_pkg.is_prime_number (pn_digit => comb6) pn6
	     FROM olap_sys.sl_gamblings
	   WHERE gambling_type = 'mrtr'
	     AND gambling_id = pn_gambling_id
	), in_drawings_dtl as (
	SELECT pv_gambling_type,
	       pn_seqno,
	       pv_type type,
	       dd.gambling_id id,
	       dd.comb1 c1,
	       dd.comb2 c2,
	       dd.comb3 c3,
	       dd.comb4 c4,
	       dd.comb5 c5,
	       dd.comb6 c6,
	       additional,
	       dd.sum_par_comb spar,
	       dd.sum_mod_comb smod,
	       global_index gi,
	       dd.clt1,
	       dd.clt2,
	       dd.clt3,
	       dd.clt4,
	       dd.clt5,
	       dd.clt6,
	       nvl(dd.c1_ca,0) c1_ca,
	       nvl(dd.c2_ca,0) c2_ca,
	       nvl(dd.c3_ca,0) c3_ca,
	       nvl(dd.c4_ca,0) c4_ca,
	       nvl(dd.c5_ca,0) c5_ca,
	       nvl(dd.c6_ca,0) c6_ca,
	       nvl(pxc1,0) pxc1,
	       nvl(pxc2,0) pxc2,
	       nvl(pxc3,0) pxc3,
	       nvl(pxc4,0) pxc4,
	       nvl(pxc5,0) pxc5,
	       nvl(pxc6,0) pxc6,
	       pi1,
	       pi2,
	       pi3,
	       pi4,
	       pi5,
	       pi6,
	       pn1,
	       pn2,
	       pn3,
	       pn4,
	       pn5,
	       pn6,
	       USER,
	       SYSDATE
	  FROM drawings_dtl dd
	  ) select *
	      from in_drawings_dtl
	  ;
     
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
      raise;        
  end ins_drawings_comparisons_IN;
        
  procedure ins_drawings_comparisons_OUT (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                        , pn_gambling_id                 number
                                        , pn_seqno                       number
                                        , pv_type                        varchar2
                                        , x_err_code       in out NOCOPY number
                                        ) is
     LV$PROCEDURE_NAME    constant varchar2(30) := 'ins_drawings_comparisons_OUT';
  
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     
	INSERT INTO OLAP_SYS.S_DRAWINGS_COMPARISONS(
	 DRAWING_TYPE,
	 SEQNO,
	 TYPE,
	 DRAWING_ID,
	 COMB1, 
	 COMB2, 
	 COMB3, 
	 COMB4, 
	 COMB5, 
	 COMB6, 
	 ADDITIONAL, 
	 SUM_PAR_COMB, 
	 SUM_MOD_COMB, 
	 GLOBAL_INDEX,
	 COLOR_LEY_TERCIO_C1,
	 COLOR_LEY_TERCIO_C2,
	 COLOR_LEY_TERCIO_C3,
	 COLOR_LEY_TERCIO_C4,
	 COLOR_LEY_TERCIO_C5,
	 COLOR_LEY_TERCIO_C6,
	 CICLO_APARICION_C1, 
	 CICLO_APARICION_C2, 
	 CICLO_APARICION_C3, 
	 CICLO_APARICION_C4, 
	 CICLO_APARICION_C5, 
	 CICLO_APARICION_C6, 
	 PRONOS_CICLO_C1, 
	 PRONOS_CICLO_C2, 
	 PRONOS_CICLO_C3, 
	 PRONOS_CICLO_C4, 
	 PRONOS_CICLO_C5, 
	 PRONOS_CICLO_C6, 
	 ON1, 
	 ON2, 
	 ON3, 
	 ON4, 
	 ON5, 
	 ON6, 
	 PN1, 
	 PN2, 
	 PN3, 
	 PN4, 
	 PN5, 
	 PN6,
	 CREATED_BY, 
	 CREATION_DATE 
	 )
	 select pv_gambling_type,
	        pn_seqno,	        
	        pv_type,
	        pn_gambling_id-1 id,
                comb1,
                comb2,
                comb3,
                comb4,
                comb5,
                comb6,    
                0 additional,
                sum_par_comb spar,
                sum_mod_comb smod,
                global_index,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb1 and gs.b_type='B1') clt1,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb2 and gs.b_type='B2') clt2,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb3 and gs.b_type='B3') clt3,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb4 and gs.b_type='B4') clt4,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb5 and gs.b_type='B5') clt5,
                (select decode(gs.color_ley_tercio,1,'R',2,'G',3,'B') from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb6 and gs.b_type='B6') clt6,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb1 and gs.b_type='B1') c1_ca,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb2 and gs.b_type='B2') c2_ca,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb3 and gs.b_type='B3') c3_ca,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb4 and gs.b_type='B4') c4_ca,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb5 and gs.b_type='B5') c5_ca,
                (select gs.ciclo_aparicion from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb6 and gs.b_type='B6') c6_ca,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb1 and gs.b_type='B1'),0) pxc1,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb2 and gs.b_type='B2'),0) pxc2,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb3 and gs.b_type='B3'),0) pxc3,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb4 and gs.b_type='B4'),0) pxc4,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb5 and gs.b_type='B5'),0) pxc5,
                nvl((select gs.pronos_ciclo from olap_sys.s_calculo_stats gs where gs.drawing_type=attribute3 and gs.drawing_id=next_drawing_id-1 and gs.digit=comb6 and gs.b_type='B6'),0) pxc6,
                mod(comb1,2) pi1,
                mod(comb2,2) pi2,
                mod(comb3,2) pi3,
                mod(comb4,2) pi4,
                mod(comb5,2) pi5,
                mod(comb6,2) pi6,
                olap_sys.w_common_pkg.is_prime_number (pn_digit => comb1) pn1,
	        olap_sys.w_common_pkg.is_prime_number (pn_digit => comb2) pn2,
	        olap_sys.w_common_pkg.is_prime_number (pn_digit => comb3) pn3,
	        olap_sys.w_common_pkg.is_prime_number (pn_digit => comb4) pn4,
	        olap_sys.w_common_pkg.is_prime_number (pn_digit => comb5) pn5,
	        olap_sys.w_common_pkg.is_prime_number (pn_digit => comb6) pn6,
	        USER,
	        SYSDATE   
           from olap_sys.w_combinations_picked_f
          where attribute3 = pv_gambling_type
            and next_drawing_id = pn_gambling_id;
     
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
      raise;        
  end ins_drawings_comparisons_OUT;
  
  
  procedure s_drawings_comparisons_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                          , pn_gambling_id                 number
                                          , x_err_code       in out NOCOPY number
                                            ) is
                                            
    LV$PROCEDURE_NAME    constant varchar2(30) := 's_drawings_comparisons_handler';
    ln$seqno             olap_sys.s_drawings_comparisons.seqno%type;
  begin
     dbms_output.enable(NULL);
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line(pn_gambling_id);
     
     --retrieving seqno
     select olap_sys.s_drawings_comparisons_seq.nextval
       into ln$seqno
       from dual;
dbms_output.put_line('ln$seqno: '||ln$seqno);

     --inserting drawing results coming from table sl_gamblings
     ins_drawings_comparisons_IN (pv_gambling_type => pv_gambling_type
	                        , pn_gambling_id   => pn_gambling_id
	                        , pn_seqno         => ln$seqno
	                        , pv_type          => 'IN'
	                        , x_err_code       => x_err_code
	                        );

     --inserting calculated drawing coming from table w_combinations_picked_f
     ins_drawings_comparisons_OUT (pv_gambling_type => pv_gambling_type
	                         , pn_gambling_id   => pn_gambling_id
	                         , pn_seqno         => ln$seqno
	                         , pv_type          => 'OUT'
	                         , x_err_code       => x_err_code
	                          );

	                        
     commit;	                        
     
  exception
    when others then
      x_err_code := sqlcode;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;        
  end s_drawings_comparisons_handler;                                              
*/    

  procedure repeated_numbers_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                    , pn_gambling_id                 number
                                    , pn_comb1                       number
                                    , pn_comb2                       number
                                    , pn_comb3                       number
                                    , pn_comb4                       number
                                    , pn_comb5                       number
                                    , pn_comb6                       number
                                    , x_err_code       in out NOCOPY number       
                                     ) is
     LV$PROCEDURE_NAME        CONSTANT VARCHAR2(30) := 'repeated_numbers_handler';
	 lv$previous_drawing      varchar2(30);
	 lv$current_drawing       varchar2(30);
	 ln$previous_drawing_id   number := pn_gambling_id -1;
	 lv$temp_dml              varchar2(1000);
	 CV$FIRST_GAMBLING        CONSTANT NUMBER := 1;
  begin
    dbms_output.put_line('----------------------------------');
    dbms_output.put_line(LV$PROCEDURE_NAME);
    if pn_gambling_id > CV$FIRST_GAMBLING then
	     --creating dynamic update
	     olap_sys.w_common_pkg.g_dml_stmt := 'update olap_sys.sl_gamblings set rep_comb# = 1, updated_date = sysdate, updated_by = user';
		 olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where gambling_type = '||chr(39)||':1'||chr(39)||' and gambling_id = :2 and comb# ';
		 olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' in (select regexp_substr('||chr(39)||':3'||chr(39)||','||chr(39)||chr(91)||chr(94)||chr(44)||chr(93)||chr(43)||chr(39)||',1,level) str';
		 olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from dual connect by level <= length('||chr(39)||':3'||chr(39)||')-length(replace('||chr(39)||':3'||chr(39)||','||chr(39)||chr(44)||chr(39)||','||chr(39)||chr(39)||'))+1)';
		 begin
		     --pulling previous drawing
			 select COMB1||','||COMB2||','||COMB3||','||COMB4||','||COMB5||','||COMB6 COMB
			   into lv$previous_drawing 
			   from olap_sys.sl_gamblings 
			  where gambling_id = ln$previous_drawing_id;
			  --dbms_output.put_line(ln$previous_drawing_id||' - '||lv$previous_drawing);
         exception
  		    when no_data_found then
		       lv$previous_drawing := null;
         end;		 

		 if lv$previous_drawing is not null then
		    olap_sys.w_common_pkg.g_index := 1; 
		    --updating current drawing
			for i in 1..6 loop
			    lv$temp_dml := olap_sys.w_common_pkg.g_dml_stmt;
			    --building update statement
				lv$temp_dml := replace(lv$temp_dml,'#',olap_sys.w_common_pkg.g_index);
				lv$temp_dml := replace(lv$temp_dml,':1',pv_gambling_type);
				lv$temp_dml := replace(lv$temp_dml,':2',pn_gambling_id);
				lv$temp_dml := replace(lv$temp_dml,':3',lv$previous_drawing);
                begin
                   --dbms_output.put_line(substr(lv$temp_dml,1,255));
				   --dbms_output.put_line(substr(lv$temp_dml,256,511));
				   execute immediate lv$temp_dml; 
				   
				   olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
				exception
                   when others then
				     dbms_output.put_line('unable to update repeated number. rep_comb'||olap_sys.w_common_pkg.g_index||' '||substr(sqlerrm,1,200));
					 x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
					 exit;
                end;				
			end loop;
/*			
			--updating previous drawing
			lv$current_drawing := pn_comb1||','||pn_comb2||','||pn_comb3||','||pn_comb4||','||pn_comb5||','||pn_comb6;

            olap_sys.w_common_pkg.g_index := 1;
			for i in 1..6 loop
			    lv$temp_dml := olap_sys.w_common_pkg.g_dml_stmt;
			    --building update statement
				lv$temp_dml := replace(lv$temp_dml,'#',olap_sys.w_common_pkg.g_index);
				lv$temp_dml := replace(lv$temp_dml,':1',pv_gambling_type);
				lv$temp_dml := replace(lv$temp_dml,':2',ln$previous_drawing_id);
				lv$temp_dml := replace(lv$temp_dml,':3',lv$current_drawing);
                begin
                   --dbms_output.put_line(substr(lv$temp_dml,1,255));
				   --dbms_output.put_line(substr(lv$temp_dml,256,511));
				   execute immediate lv$temp_dml; 

				   olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
				exception
                   when others then
				     dbms_output.put_line('unable to update repeated number. rep_comb'||olap_sys.w_common_pkg.g_index||' '||substr(sqlerrm,1,200));
					 x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
					 exit;
                end;				
			end loop;*/			
		 end if;
		 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
     end if;
  exception
    when others then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
  end repeated_numbers_handler;

  procedure prime_pairs_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                               , pn_gambling_id                 number
                               , pn_comb1                       number
                               , pn_comb2                       number
                               , pn_comb3                       number
                               , pn_comb4                       number
                               , pn_comb5                       number
                               , pn_comb6                       number
                               , x_err_code       in out NOCOPY number       
                                ) is
	 lv$current_drawing   varchar2(30);		
	 LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'prime_pairs_handler';
     CN$FIRST_POSITION    CONSTANT NUMBER := 1;
	 CN$SECOND_POSITION   CONSTANT NUMBER := 2;
	 ln$primo_ini         number := 0;
	 ln$primo_fin         number := 0;
	 lv$prev_drawing_list OLAP_SYS.S_GL_LEY_TERCIO_PATTERNS.CYCLES_LIST%type;
	 ln$ciclo_calculado   NUMBER := 0;
	 
     cursor c_prime_number (pv_current_drawing  varchar2) is
	 with full_list_tbl as (
     select regexp_substr(pv_current_drawing,'[^,]+',1,level) digits
       from dual 
     connect by level <= length(pv_current_drawing)-length(replace(pv_current_drawing,',',''))+1
     ) select digits
         from full_list_tbl
        where digits  in (1,2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101);    
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     lv$current_drawing := pn_comb1||','||pn_comb2||','||pn_comb3||','||pn_comb4||','||pn_comb5||','||pn_comb6;
	 olap_sys.w_common_pkg.g_index := 1;
	 olap_sys.w_common_pkg.g_rowcnt := 0;
	 --counting how many prime numbers are in the drawing
	 for i in c_prime_number (pv_current_drawing => lv$current_drawing) loop
	     if olap_sys.w_common_pkg.g_index = CN$FIRST_POSITION then
		    ln$primo_ini := i.digits;
			olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
		 elsif olap_sys.w_common_pkg.g_index = CN$SECOND_POSITION then
		    ln$primo_fin := i.digits;
			olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
		 elsif olap_sys.w_common_pkg.g_index > CN$SECOND_POSITION then		 
		 olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;
		 end if;
		 
		 olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
	 end loop;
	 dbms_output.put_line('primo_ini: '||ln$primo_ini||' primo_fin: '||ln$primo_fin||' g_rowcnt: '||olap_sys.w_common_pkg.g_rowcnt);
	 if ln$primo_ini = 0 and ln$primo_fin = 0 then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		dbms_output.put_line('There is not any prime number in the list.');
	 elsif ln$primo_ini != 0 and ln$primo_fin = 0 then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		dbms_output.put_line('There is one prime number in the list only.');
	 elsif ln$primo_ini != 0 and ln$primo_fin != 0 and olap_sys.w_common_pkg.g_rowcnt = CN$SECOND_POSITION then
	    update olap_sys.pm_parejas_primos
		   set estadistica = decode(olap_sys.w_common_pkg.g_rowcnt,2,estadistica + 1, estadistica)
			 , drawing_id = pn_gambling_id
			 , primo_cnt = olap_sys.w_common_pkg.g_rowcnt 
			 , drawing_list = drawing_list||pn_gambling_id||'|'
			 , updated_by = user
			 , updated_date = sysdate
			 , diferencia_sorteo = pn_gambling_id - drawing_id
		 where drawing_type = pv_gambling_type
		   and primo_ini = ln$primo_ini
		   and primo_fin = ln$primo_fin
		return drawing_list into lv$prev_drawing_list;
		
		dbms_output.put_line('lv$prev_drawing_list: '||lv$prev_drawing_list); 
        ln$ciclo_calculado := OLAP_SYS.W_COMMON_PKG.COMPUTE_CICLOS (PV_LISTA_ESTADISTICA=>lv$prev_drawing_list);
        dbms_output.put_line('ln$ciclo_calculado: '||ln$ciclo_calculado); 
		   
	    update olap_sys.pm_parejas_primos
		   set cycles_list = cycles_list||TO_CHAR(ln$ciclo_calculado)||'|'
			 , cycles_avg = OLAP_SYS.W_COMMON_PKG.COMPUTE_AVG_CICLOS (PV_LISTA_CICLOS=>CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|')
			 , updated_by = user
			 , updated_date = sysdate
		 where drawing_type = pv_gambling_type
		   and primo_ini = ln$primo_ini
		   and primo_fin = ln$primo_fin;
		   
		if sql%found then
		   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
		   dbms_output.put_line(sql%rowcount||' rows updated');
		else
	       x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		   dbms_output.put_line('pair of prime numbers was not found.');
        end if;	

		--!inicialzamos todos los play_status a N
		update olap_sys.pm_parejas_primos
		   set play_status = 'N';
		   
		--!solo los play_status que esten por debajo del rango alto se actualizaran a Y
		update olap_sys.pm_parejas_primos
		   set play_status = 'Y'
		 where diferencia_sorteo < (select round(avg((select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id) + stddev((select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id) * 0.75) from olap_sys.pm_parejas_primos);	
	 else
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
		dbms_output.put_line('No update statement executed.');
	 end if;
  exception
    when others then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;	 
  end prime_pairs_handler;  

  procedure ley_tercio_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                              , pn_gambling_id                 number
                              , x_err_code       in out NOCOPY number       
                               ) is
	 LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ley_tercio_handler';
	 lv$prev_drawing_list          OLAP_SYS.S_GL_LEY_TERCIO_PATTERNS.CYCLES_LIST%type;
	 ln$ciclo_calculado            NUMBER := 0;
	 
	 cursor c_main (pn_gambling_id number) is
	 select nvl(mr.clt1,'#') clt1, nvl(mr.clt2,'#') clt2, nvl(mr.clt3,'#') clt3, nvl(mr.clt4,'#') clt4, nvl(mr.clt5,'#') clt5, nvl(mr.clt6,'#') clt6
       from olap_sys.pm_mr_resultados_v2 mr
      where mr.gambling_id = pn_gambling_id;
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     for i in c_main (pn_gambling_id=> pn_gambling_id) loop     
		 if i.clt1='#' and i.clt2='#' and i.clt3='#' and i.clt4='#' and i.clt5='#' and i.clt6='#' then	
             dbms_output.put_line('No data found in table OLAP_SYS.S_CALCULO_STATS. Drawing_Id: '||pn_gambling_id);	
         else			 
			 update olap_sys.s_gl_ley_tercio_patterns lt
				 set lt.match_cnt = match_cnt + 1
				   , lt.drawing_list = drawing_list||pn_gambling_id||'|'
				   , updated_by = user
				   , updated_date = sysdate
			   where drawing_type = pv_gambling_type
				 and (lt.lt1,lt.lt2,lt.lt3,lt.lt4,lt.lt5,lt.lt6) in ((i.clt1, i.clt2, i.clt3, i.clt4, i.clt5, i.clt6))
			  return lt.drawing_list into lv$prev_drawing_list;

            dbms_output.put_line('lv$prev_drawing_list: '||lv$prev_drawing_list); 
            ln$ciclo_calculado := OLAP_SYS.W_COMMON_PKG.COMPUTE_CICLOS (PV_LISTA_ESTADISTICA=>lv$prev_drawing_list);
            dbms_output.put_line('ln$ciclo_calculado: '||ln$ciclo_calculado); 	 

            if ln$ciclo_calculado > 0 then
			 update olap_sys.s_gl_ley_tercio_patterns lt
				 set lt.cycles_list = cycles_list||TO_CHAR(ln$ciclo_calculado)||'|'
				   , lt.cycles_avg = OLAP_SYS.W_COMMON_PKG.COMPUTE_AVG_CICLOS (PV_LISTA_CICLOS=>CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|')
				   , updated_by = user
				   , updated_date = sysdate
			   where drawing_type = pv_gambling_type
				 and (lt.lt1,lt.lt2,lt.lt3,lt.lt4,lt.lt5,lt.lt6) in ((i.clt1, i.clt2, i.clt3, i.clt4, i.clt5, i.clt6));
			end if;
			
			 if sql%found then
				dbms_output.put_line(sql%rowcount||' rows updated');
				update olap_sys.s_gl_ley_tercio_patterns
				   set last_drawing_id = pn_gambling_id		     
				 where drawing_type = pv_gambling_type;		

				x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
				dbms_output.put_line(sql%rowcount||' rows updated');		 
			 end if;	
		 end if;
	 end loop;	
  exception
    when others then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;		 
  end ley_tercio_handler;

  procedure terminaciones_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                 , pn_gambling_id                 number
                                 , pn_comb1                       number
                                 , pn_comb2                       number
                                 , pn_comb3                       number
                                 , pn_comb4                       number
                                 , pn_comb5                       number
                                 , pn_comb6                       number								 
                                 , x_err_code       in out NOCOPY number       
                                  ) is 
 	 LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'terminaciones_handler'; 
	 ln$t1                NUMBER(1) := 0;
	 ln$t2                NUMBER(1) := 0;
	 ln$t3                NUMBER(1) := 0;
	 ln$t4                NUMBER(1) := 0;
	 ln$t5                NUMBER(1) := 0;
	 ln$t6                NUMBER(1) := 0;
	 lv$prev_drawing_list OLAP_SYS.S_TERMINACIONES.DRAWING_LIST%type;
	 ln$ciclo_calculado   NUMBER := 0;	 
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME); 
	 
	 IF pn_comb1 > 0 AND pn_comb2 > 0 AND pn_comb3 > 0 AND pn_comb4 >0 AND pn_comb5 > 0 AND pn_comb6 > 0 THEN
		 ln$t1 := SUBSTR(LPAD(pn_comb1,2,'0'),2);
		 ln$t2 := SUBSTR(LPAD(pn_comb2,2,'0'),2);
		 ln$t3 := SUBSTR(LPAD(pn_comb3,2,'0'),2);
		 ln$t4 := SUBSTR(LPAD(pn_comb4,2,'0'),2);
		 ln$t5 := SUBSTR(LPAD(pn_comb5,2,'0'),2);
		 ln$t6 := SUBSTR(LPAD(pn_comb6,2,'0'),2);
		 
		 UPDATE OLAP_SYS.S_TERMINACIONES
			SET MATCH_CNT = MATCH_CNT + 1
			  , DRAWING_LIST = DRAWING_LIST||TO_CHAR(PN_GAMBLING_ID)||'|'
			  , LAST_DRAWING_ID = pn_gambling_id
			  , UPDATED_BY = USER
			  , UPDATED_DATE = SYSDATE
		  WHERE DRAWING_TYPE = pv_gambling_type
			AND T1 = ln$t1
			AND T2 = ln$t2
			AND T3 = ln$t3
			AND T4 = ln$t4
			AND T5 = ln$t5
			AND T6 = ln$t6
		   RETURN DRAWING_LIST INTO lv$prev_drawing_list;

            dbms_output.put_line('lv$prev_drawing_list: '||lv$prev_drawing_list); 
            ln$ciclo_calculado := OLAP_SYS.W_COMMON_PKG.COMPUTE_CICLOS (PV_LISTA_ESTADISTICA=>lv$prev_drawing_list);
            dbms_output.put_line('ln$ciclo_calculado: '||ln$ciclo_calculado); 	

			IF NVL(ln$ciclo_calculado,0) > 0 THEN
				UPDATE OLAP_SYS.S_TERMINACIONES 
				   SET CYCLES_LIST = CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|'
				     , UPDATED_DATE = SYSDATE
				     , CYCLES_AVG = OLAP_SYS.W_COMMON_PKG.COMPUTE_AVG_CICLOS (PV_LISTA_CICLOS=>CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|')
				 WHERE DRAWING_TYPE = pv_gambling_type
				   AND T1 = ln$t1
				   AND T2 = ln$t2
				   AND T3 = ln$t3
				   AND T4 = ln$t4
				   AND T5 = ln$t5
				   AND T6 = ln$t6;
			END IF;	 
		dbms_output.put_line(sql%rowcount||' rows updated');				
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	 END IF;
  exception
    when others then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;		 
  end terminaciones_handler; 
  
  procedure panorama_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                            , pn_gambling_id                 number
                            , x_err_code       in out NOCOPY number
                             ) is
 	 LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'panorama_handler'; 
	 lv$prev_drawing_list          OLAP_SYS.PM_PANORAMA.DRAWING_LIST%type;
	 ln$ciclo_calculado            NUMBER := 0;
	 
     cursor c_panorama (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                      , pn_gambling_id                 number) is	 
	 WITH COMPUESTOS_TBL AS (
	 SELECT DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB1),1,NULL,COMB1) C1
	 	  , DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB2),1,NULL,COMB2) C2
		  , DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB3),1,NULL,COMB3) C3
		  , DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB4),1,NULL,COMB4) C4
		  , DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB5),1,NULL,COMB5) C5
		  , DECODE(OLAP_SYS.W_COMMON_PKG.IS_PRIME_NUMBER(COMB6),1,NULL,COMB6) C6
	   FROM olap_sys.pm_mr_resultados_v2
      WHERE PN_CNT      = 2
	    AND GAMBLING_ID = pn_gambling_id
	 ) , PANORAMA_TBL AS (
      SELECT DECODE(C1,NULL,NULL,C1||',')
		   || DECODE(C2,NULL,NULL,C2||',')
		   || DECODE(C3,NULL,NULL,C3||',')
		   || DECODE(C4,NULL,NULL,C4||',')
		   || DECODE(C5,NULL,NULL,C5||',')
		   || DECODE(C6,NULL,NULL,C6) STR 
		 FROM COMPUESTOS_TBL
     ) SELECT SUBSTR(STR,1,INSTR(STR,',',1,1)-1) P1_COMP
            , SUBSTR(STR,INSTR(STR,',',1,1)+1,INSTR(STR,',',1,2)-INSTR(STR,',',1,1)-1) P2_COMP
            , SUBSTR(STR,INSTR(STR,',',1,2)+1,INSTR(STR,',',1,3)-INSTR(STR,',',1,2)-1) P3_COMP
            , REPLACE(SUBSTR(STR,INSTR(STR,',',1,3)+1),',',NULL) P4_COMP
         FROM PANORAMA_TBL;			  
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME); 
	 dbms_output.put_line('ID: '||pn_gambling_id); 
     FOR i IN c_panorama (pv_gambling_type => pv_gambling_type
                        , pn_gambling_id   => pn_gambling_id) LOOP
         dbms_output.put_line('P1: '||i.P1_COMP||' P2: '||i.P2_COMP||' P3: '||i.P3_COMP||' P4: '||i.P4_COMP);
	        UPDATE OLAP_SYS.PM_PANORAMA 
			   SET DRAWING_LIST = DRAWING_LIST||TO_CHAR(PN_GAMBLING_ID)||'|'
			     , USE_FLAG = 'N'
				 , UPDATED_DATE = SYSDATE
	        WHERE DRAWING_TYPE = pv_gambling_type
			  AND P1_COMP = i.P1_COMP
			  AND P2_COMP = i.P2_COMP
			  AND P3_COMP = i.P3_COMP
			  AND P4_COMP = i.P4_COMP
		   RETURN DRAWING_LIST INTO lv$prev_drawing_list;

            dbms_output.put_line('lv$prev_drawing_list: '||lv$prev_drawing_list); 
            ln$ciclo_calculado := OLAP_SYS.W_COMMON_PKG.COMPUTE_CICLOS (PV_LISTA_ESTADISTICA=>lv$prev_drawing_list);
            dbms_output.put_line('ln$ciclo_calculado: '||ln$ciclo_calculado); 	

			IF NVL(ln$ciclo_calculado,0) > 0 THEN
				UPDATE OLAP_SYS.PM_PANORAMA 
				   SET CYCLES_LIST = CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|'
				     , UPDATED_DATE = SYSDATE
				     , CYCLES_AVG = OLAP_SYS.W_COMMON_PKG.COMPUTE_AVG_CICLOS (PV_LISTA_CICLOS=>CYCLES_LIST||TO_CHAR(ln$ciclo_calculado)||'|')
				 WHERE DRAWING_TYPE = pv_gambling_type
			       AND P1_COMP = i.P1_COMP
			       AND P2_COMP = i.P2_COMP
			       AND P3_COMP = i.P3_COMP
			       AND P4_COMP = i.P4_COMP;
			END IF;	 
     END LOOP;	
	 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
			
  EXCEPTION
     WHEN OTHERS THEN
	    DBMS_OUTPUT.PUT_LINE('Unable to update panorama for ID: '||pn_gambling_id);
		DBMS_OUTPUT.PUT_LINE(SQLERRM);
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;		 
  end panorama_handler;  
  
  procedure decenas_numeros_primos_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                          , pn_gambling_id                 number
                                          , pn_comb1                       number
                                          , pn_comb2                       number
                                          , pn_comb3                       number
                                          , pn_comb4                       number
                                          , pn_comb5                       number
                                          , pn_comb6                       number								 
                                          , x_err_code       in out NOCOPY number       
                                           ) is
     LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'decenas_numeros_primos_handler'; 
	 lv$prev_lista_estadistica     OLAP_SYS.PM_DECENAS_NUMEROS_PRIMOS.LISTA_ESTADISTICA%type;
	 lv$prev_lista_ciclos          OLAP_SYS.PM_DECENAS_NUMEROS_PRIMOS.LISTA_CICLOS%type; 
	 ln$ciclo_calculado            NUMBER := 0;

	 cursor c_decenas_primos (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                            , pn_comb1                       number
                            , pn_comb2                       number
                            , pn_comb3                       number
                            , pn_comb4                       number
                            , pn_comb5                       number
                            , pn_comb6                       number) is
	 SELECT d1
          , d2
          , d3
          , d4
          , d5
          , d6
          , pn1
          , pn2
          , pn3
          , pn4
          , pn5
          , pn6
       FROM olap_sys.w_combination_responses_fs
      WHERE attribute3 = pv_gambling_type
        AND comb1 =pn_comb1
        AND comb2 =pn_comb2
        AND comb3 =pn_comb3
        AND comb4 =pn_comb4
        AND comb5 =pn_comb5
        AND comb6 =pn_comb6;
  begin
     dbms_output.put_line('----------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME); 
	 dbms_output.put_line('ID: '||pn_gambling_id); 
    FOR i IN c_decenas_primos (pv_gambling_type => pv_gambling_type
                              , pn_comb1 => pn_comb1
                              , pn_comb2 => pn_comb2
                              , pn_comb3 => pn_comb3
                              , pn_comb4 => pn_comb4
                              , pn_comb5 => pn_comb5
                              , pn_comb6 => pn_comb6) LOOP
		 UPDATE OLAP_SYS.PM_DECENAS_NUMEROS_PRIMOS
            SET ESTADISTICA = NVL(ESTADISTICA,0) + 1
              , LISTA_ESTADISTICA = LISTA_ESTADISTICA ||TO_CHAR(pn_gambling_id)||'|'
          WHERE D1=i.D1
            AND D2=i.D2
            AND D3=i.D3
            AND D4=i.D4
            AND D5=i.D5
            AND D6=i.D6
            AND PN1=i.PN1
            AND PN2=i.PN2
            AND PN3=i.PN3
            AND PN4=i.PN4
            AND PN5=i.PN5
            AND PN6=i.PN6
			RETURN LISTA_ESTADISTICA into lv$prev_lista_estadistica;
            dbms_output.put_line('lv$prev_lista_estadistica: '||lv$prev_lista_estadistica); 
            ln$ciclo_calculado := OLAP_SYS.W_COMMON_PKG.COMPUTE_CICLOS (PV_LISTA_ESTADISTICA=>lv$prev_lista_estadistica);
            dbms_output.put_line('ln$ciclo_calculado: '||ln$ciclo_calculado); 	

		 IF NVL(ln$ciclo_calculado,0) > 0 THEN
			 UPDATE OLAP_SYS.PM_DECENAS_NUMEROS_PRIMOS
				SET LISTA_CICLOS = LISTA_CICLOS||TO_CHAR(ln$ciclo_calculado)||'|'
				  , PROMEDIO_CICLOS = OLAP_SYS.W_COMMON_PKG.COMPUTE_AVG_CICLOS (PV_LISTA_CICLOS=>LISTA_CICLOS||TO_CHAR(ln$ciclo_calculado)||'|')
				  , UPDATED_BY = USER
				  , UPDATED_DATE = SYSDATE
			  WHERE D1=i.D1
				AND D2=i.D2
				AND D3=i.D3
				AND D4=i.D4
				AND D5=i.D5
				AND D6=i.D6
				AND PN1=i.PN1
				AND PN2=i.PN2
				AND PN3=i.PN3
				AND PN4=i.PN4
				AND PN5=i.PN5
				AND PN6=i.PN6; 	
         END IF;		 
     END LOOP;
	 
	 UPDATE OLAP_SYS.PM_DECENAS_NUMEROS_PRIMOS
        SET LAST_DRAWING_ID = pn_gambling_id;	
     
     x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
dbms_output.put_line('x_err_code: '||x_err_code);	 
  exception
    when others then
	    dbms_output.put_line(sqlerrm);
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;		 
  end decenas_numeros_primos_handler; 


  --!proceso para insertar un nuevo registro en la tabla 
  procedure ins_pm_parejas_primos_log (pv_drawing_type	      		varchar2
                                     , pn_primo_ini					number
                                     , pn_primo_fin					number
                                     , pn_diferencia  				number
                                     , pn_drawing_id  				number
                                     , pn_diferencia_avg    		number
                                     , pn_diferencia_stddev 		number
                                     , pn_diferencia_ini    		number
                                     , pn_diferencia_end     		number
                                     , pn_factor    				number
									 , pv_drawing_list				varchar2
									 , pv_decena_id					varchar2								 
									 , x_err_code     in out NOCOPY number 									
									 ) is
     LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'ins_pm_parejas_primos_log'; 
	 pragma autonomous_transaction; 
  begin  
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	DBMS_OUTPUT.PUT_LINE('pn_primo_ini: '||pn_primo_ini);
	DBMS_OUTPUT.PUT_LINE('pn_primo_fin: '||pn_primo_fin);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia: '||pn_diferencia);
	DBMS_OUTPUT.PUT_LINE('pn_drawing_id: '||pn_drawing_id);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_avg: '||pn_diferencia_avg);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_stddev: '||pn_diferencia_stddev);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_ini: '||pn_diferencia_ini);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_end: '||pn_diferencia_end);
	DBMS_OUTPUT.PUT_LINE('pn_factor: '||pn_factor);
	DBMS_OUTPUT.PUT_LINE('pv_drawing_list: '||pv_drawing_list);
	DBMS_OUTPUT.PUT_LINE('pv_decena_id: '||pv_decena_id);
/*
for i in c_main loop
	DBMS_OUTPUT.PUT_LINE('pv_drawing_type: '||i.pv_drawing_type);
	DBMS_OUTPUT.PUT_LINE('pn_primo_ini: '||i.pn_primo_ini);
	DBMS_OUTPUT.PUT_LINE('pn_primo_fin: '||i.pn_primo_fin);
	DBMS_OUTPUT.PUT_LINE('seq_no: '||i.pn_seq_no);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia: '||i.pn_diferencia);
	DBMS_OUTPUT.PUT_LINE('drawing_cnt: '||i.pn_drawing_cnt);
	DBMS_OUTPUT.PUT_LINE('pn_drawing_id: '||i.pn_drawing_id);
	DBMS_OUTPUT.PUT_LINE('drawing_espacios: '||i.pn_drawing_espacios);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_avg: '||i.pn_diferencia_avg);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_stddev: '||i.pn_diferencia_stddev);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_ini: '||i.pn_diferencia_ini);
	DBMS_OUTPUT.PUT_LINE('pn_diferencia_end: '||i.pn_diferencia_end);
	DBMS_OUTPUT.PUT_LINE('pn_factor: '||i.pn_factor);
	DBMS_OUTPUT.PUT_LINE('bandera_in: '||i.pv_bandera_in);
	 insert into olap_sys.pm_parejas_primos_log (drawing_type
												, primo_ini
												, primo_fin
												, seq_no
												, diferencia
												, drawing_cnt
												, drawing_id
												, drawing_espacios
												, diferencia_avg
												, diferencia_stddev
												, diferencia_ini
												, diferencia_end
												, factor
												, bandera_in)
values(
i.pv_drawing_type
,i.pn_primo_ini
,i.pn_primo_fin
,i.pn_seq_no
,i.pn_diferencia
,i.pn_drawing_cnt
,i.pn_drawing_id
,i.pn_drawing_espacios
,i.pn_diferencia_avg
,i.pn_diferencia_stddev
,i.pn_diferencia_ini
,i.pn_diferencia_end
,i.pn_factor
,i.pv_bandera_in
);												
end loop;
*/
	 insert into olap_sys.pm_parejas_primos_log (drawing_type
												, primo_ini
												, primo_fin
												, seq_no
												, diferencia
												, drawing_cnt
												, drawing_id
												, drawing_espacios
												, diferencia_avg
												, diferencia_stddev
												, diferencia_ini
												, diferencia_end
												, factor
												, bandera_in
												, decena_id
												)
	 select pv_drawing_type
		  , pn_primo_ini
		  , pn_primo_fin
		  , (select nvl(max(seq_no),0) + 1 from olap_sys.pm_parejas_primos_log) seq_no
		  , pn_diferencia
		  , olap_sys.w_common_pkg.count_drawings_in_list (pv_lista_estadistica=> pv_drawing_list) drawing_cnt
		  , pn_drawing_id
		  , (select max(gambling_id) from olap_sys.sl_gamblings where gambling_type = pv_drawing_type) - pn_drawing_id drawing_espacios
		  , pn_diferencia_avg
		  , pn_diferencia_stddev
		  , pn_diferencia_ini
		  , pn_diferencia_end
		  , pn_factor	
          , case when pn_diferencia >= pn_diferencia_ini and pn_diferencia <= pn_diferencia_end then 'Y' else 'N' end bandera_in
		  , pv_decena_id
       from dual
      where not exists (select 1
						  from olap_sys.pm_parejas_primos_log
						 where drawing_type = pv_drawing_type
						   and primo_ini    = pn_primo_ini
						   and primo_fin    = pn_primo_fin	 
						   and drawing_id   = pn_drawing_id);
	 commit;	
	 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;						   
  exception
     when others then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end ins_pm_parejas_primos_log; 

  --!proceso para calcular el promedio, desviacion estandard y los rangos de la diferencia
  procedure calcular_diferencia_info (pv_gambling_type  	              varchar2
							        , xn_diferencia_avg		in out NOCOPY number
							        , xn_diferencia_stddev	in out NOCOPY number
							        , xn_diferencia_ini		in out NOCOPY number
							        , xn_diferencia_end		in out NOCOPY number
							        , xn_factor				in out NOCOPY number
									, x_err_code            in out NOCOPY number 									
									 ) is
     LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'calcular_diferencia_info'; 	
	 pragma autonomous_transaction;	 
  begin  
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	
	select to_number(ATTRIBUTE1)
	  into xn_factor
	  from olap_sys.w_lookups_fs
     where gambling_type = pv_gambling_type
	   and context= 'PRIMOS_DIFERENCIA'
       and code   = 'FACTOR'
	   and status = 'A';


	select round(avg(diferencia),2) diferencia_avg
		 , round(stddev(diferencia),2) diferencia_stddev
		 , round(avg(diferencia)-(stddev(diferencia)*xn_factor),2) diferencia_ini	 
		 , round(avg(diferencia)+(stddev(diferencia)*xn_factor),2) diferencia_end	
	  into xn_diferencia_avg
	     , xn_diferencia_stddev
		 , xn_diferencia_ini
		 , xn_diferencia_end
	  from olap_sys.pm_parejas_primos
	 where drawing_type = pv_gambling_type; 
	 
	 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
     when others then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end calcular_diferencia_info; 
  
  
  --!proceso para encontrar la decena que le corresponde a los numeros primos
  procedure encontrar_decena_info (pv_gambling_type  	        	varchar2
								 , pn_drawing_id					number
								 , xv_decena_id       in out NOCOPY varchar2      
								 , x_err_code         in out NOCOPY number 									
								  ) is
     LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'encontrar_decena_info'; 										  
	cursor c_sorteos (pv_gambling_type  	        varchar2
					, pn_drawing_id					number)is
	select comb1, comb2, comb3, comb4, comb5, comb6
	  from olap_sys.sl_gamblings
	 where gambling_type = pv_gambling_type
	   and gambling_id = pn_drawing_id; 
  begin  
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	
	for k in c_sorteos (pv_gambling_type => pv_gambling_type
					  , pn_drawing_id => pn_drawing_id) loop
	 xv_decena_id := olap_sys.w_common_pkg.get_dozen_sort (p_d1 => k.comb1
                                                         , p_d2 => k.comb2
                                                         , p_d3 => k.comb3
                                                         , p_d4 => k.comb4
                                                         , p_d5 => k.comb5
                                                         , p_d6 => k.comb6
                                                          );
	 end loop;
	 
	 if xv_decena_id is not null then
		 DBMS_OUTPUT.PUT_LINE('xv_decena_id: '||xv_decena_id);	
		 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	 else
		 x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	 end if;
  exception
     when others then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end encontrar_decena_info; 

  
  --!proceso principal para reunir informacion, realizar calculos e insertar nuevos registro en la tabla de log
  procedure pm_parejas_primos_log_handler (pv_gambling_type  	        varchar2
										 , pn_primo_ini					number
										 , pn_primo_fin					number
										 , pn_diferencia				number
										 , pn_drawing_id				number
										 , pv_drawing_list				varchar2								 
										  ) is
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'pm_parejas_primos_log_handler'; 
	ln$diferencia_avg				number := 0;
	ln$diferencia_stddev			number := 0;
	ln$diferencia_ini				number := 0;
	ln$diferencia_end				number := 0;
	ln$factor						number := 0;
	ln$err_code 	 				number := 0;
	ln$drawing_espacios				number := 0;
	lv$decena_id                    varchar2(2);
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);

	 --!proceso para calcular el promedio, desviacion estandard y los rangos de la diferencia
	 calcular_diferencia_info (pv_gambling_type  	 => pv_gambling_type
							 , xn_diferencia_avg	 => ln$diferencia_avg
							 , xn_diferencia_stddev	 => ln$diferencia_stddev
							 , xn_diferencia_ini	 => ln$diferencia_ini
							 , xn_diferencia_end	 => ln$diferencia_end
							 , xn_factor			 => ln$factor
							 , x_err_code       	 => ln$err_code 									
							  );
	 if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION	then
		  
		--!proceso para encontrar la decena que le corresponde a los numeros primos
		encontrar_decena_info (pv_gambling_type => pv_gambling_type
							 , pn_drawing_id	=> pn_drawing_id
							 , xv_decena_id     => lv$decena_id   
							 , x_err_code       => ln$err_code							
							  );
							  
		if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION	then					  
			  --!proceso para insertar un nuevo registro en la tabla 
			  ins_pm_parejas_primos_log (pv_drawing_type	  => pv_gambling_type
									   , pn_primo_ini		  => pn_primo_ini
									   , pn_primo_fin		  => pn_primo_fin
									   , pn_diferencia  	  => pn_diferencia
									   , pn_drawing_id  	  => pn_drawing_id
									   , pn_diferencia_avg    => ln$diferencia_avg
									   , pn_diferencia_stddev => ln$diferencia_stddev
									   , pn_diferencia_ini    => ln$diferencia_ini
									   , pn_diferencia_end    => ln$diferencia_end
									   , pn_factor    		  => ln$factor
									   , pv_drawing_list	  => pv_drawing_list
									   , pv_decena_id         => lv$decena_id  
									   , x_err_code           => ln$err_code 								
										);
		end if;							
	 end if;	
							  
  exception
     when others then
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end pm_parejas_primos_log_handler;  

  --!proceso para extraer info de los sorteos y llenar los arreglos
  procedure popular_arreglos (pv_gambling_type  	         	varchar2
                            , pn_gambling_id                 	number
							, pv_b_type                         varchar2
							, xtbl_prev_sorteo    in out NOCOPY gt$sorteo_tbl
						    , xtbl_curr_sorteo    in out NOCOPY gt$sorteo_tbl
							, x_err_code       in out NOCOPY number 								 
							 ) is
	   
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'popular_arreglos'; 	
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	
	xtbl_prev_sorteo.delete;
	xtbl_curr_sorteo.delete;

	gn$prev_gambling_id := pn_gambling_id-1;

    --!carga de la info del sorteo previo
	select rango_ley_tercio
	     , drawing_id + 1
	     , b_type
	     , digit
	     , color_ubicacion
	     , ubicacion
	     , color_ley_tercio
	     , ley_tercio
	     , ciclo_aparicion
	     , pronos_ciclo
	     , preferencia_flag 
         , 'N' copy_flag		 
      bulk collect into xtbl_prev_sorteo		 
	  from olap_sys.s_calculo_stats 
	 where drawing_type = pv_gambling_type
	   and drawing_id   = gn$prev_gambling_id
	   and b_type       = pv_b_type
	 order by b_type
         , drawing_id        
         , ubicacion desc
         , digit desc; 	

DBMS_OUTPUT.PUT_LINE(GV$SORTEO_PREVIO||'  pn_gambling_id: '||gn$prev_gambling_id||' count: '||xtbl_prev_sorteo.count);
--for m in xtbl_prev_sorteo.first..xtbl_prev_sorteo.last loop
--	DBMS_OUTPUT.PUT_LINE('ANTERIOR: '||'|'||xtbl_prev_sorteo(m).rango_ley_tercio||'|'||xtbl_prev_sorteo(m).drawing_id||'|'||xtbl_prev_sorteo(m).b_type||'|'||xtbl_prev_sorteo(m).digit||'|'||xtbl_prev_sorteo(m).color_ubicacion||'|'||xtbl_prev_sorteo(m).ubicacion||'|'||xtbl_prev_sorteo(m).color_ley_tercio||'|'||xtbl_prev_sorteo(m).ley_tercio||'|'||xtbl_prev_sorteo(m).ciclo_aparicion||'|'||xtbl_prev_sorteo(m).pronos_ciclo||'|'||xtbl_prev_sorteo(m).preferencia_flag||'|'||xtbl_prev_sorteo(m).copy_flag);
--	DBMS_OUTPUT.PUT_LINE('ANTERIOR: '||'|'||xtbl_prev_sorteo(m).rango_ley_tercio||'|'||xtbl_prev_sorteo(m).drawing_id||'|'||xtbl_prev_sorteo(m).b_type||'|'||xtbl_prev_sorteo(m).digit);
--end loop;

	gn$curr_gambling_id	:= pn_gambling_id;
    --!carga de la info del sorteo actual
	select rango_ley_tercio
	     , drawing_id + 1
	     , b_type
	     , digit
	     , color_ubicacion
	     , ubicacion
	     , color_ley_tercio
	     , ley_tercio
	     , ciclo_aparicion
	     , pronos_ciclo
	     , preferencia_flag 
         , 'N' copy_flag		 
      bulk collect into xtbl_curr_sorteo		 
	  from olap_sys.s_calculo_stats 
	 where drawing_type = pv_gambling_type
	   and drawing_id   = gn$curr_gambling_id
	   and b_type = pv_b_type
	 order by b_type
         , drawing_id        
         , ubicacion desc
         , digit desc;

DBMS_OUTPUT.PUT_LINE(GV$SORTEO_ACTUAL||'  pn_gambling_id: '||gn$curr_gambling_id||' count: '||xtbl_curr_sorteo.count);
--for m in xtbl_curr_sorteo.first..xtbl_curr_sorteo.last loop
--	DBMS_OUTPUT.PUT_LINE('ACTUAL: '||'|'||xtbl_curr_sorteo(m).rango_ley_tercio||'|'||xtbl_curr_sorteo(m).drawing_id||'|'||xtbl_curr_sorteo(m).b_type||'|'||xtbl_curr_sorteo(m).digit||'|'||xtbl_curr_sorteo(m).color_ubicacion||'|'||xtbl_curr_sorteo(m).ubicacion||'|'||xtbl_curr_sorteo(m).color_ley_tercio||'|'||xtbl_curr_sorteo(m).ley_tercio||'|'||xtbl_curr_sorteo(m).ciclo_aparicion||'|'||xtbl_curr_sorteo(m).pronos_ciclo||'|'||xtbl_curr_sorteo(m).preferencia_flag||'|'||xtbl_curr_sorteo(m).copy_flag);
--	DBMS_OUTPUT.PUT_LINE('ACTUAL: '||'|'||xtbl_curr_sorteo(m).rango_ley_tercio||'|'||xtbl_curr_sorteo(m).drawing_id||'|'||xtbl_curr_sorteo(m).b_type||'|'||xtbl_curr_sorteo(m).digit);
--end loop;

	if xtbl_prev_sorteo.count > 0 and xtbl_curr_sorteo.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;
	
  exception
     when others then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end popular_arreglos;  
  
  
  --!proceso para comparar la informacion de ambos arreglos
  procedure comparar_info_arreglos (ptbl_prev_sorteo                   gt$sorteo_tbl
								  , ptbl_curr_sorteo                   gt$sorteo_tbl
                                  , pn_gambling_id                     number
								  , pv_b_type                          varchar2 
								  , pv_actualizar_cambios			   varchar2
								  , x_err_code       		 	in out NOCOPY number 								 
								    ) is
	   
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'comparar_info_arreglos';
    ln$prev_index            number := 0; 
	ln$curr_index            number := 0;
	ln$index_ini             number := 1; 
	ln$index_end             number := 0;
	ln$gambling_id			 number := 0;	
	lv$prev_preferencia_flag varchar2(5);
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	DBMS_OUTPUT.PUT_LINE('pv_b_type: '||pv_b_type);
	DBMS_OUTPUT.PUT_LINE('<<< ptbl_prev_sorteo.count: '||ptbl_prev_sorteo.count||' first: '||ptbl_prev_sorteo.first||' last: '||ptbl_prev_sorteo.last);
	DBMS_OUTPUT.PUT_LINE('<<< ptbl_curr_sorteo.count: '||ptbl_curr_sorteo.count||' first: '||ptbl_curr_sorteo.first||' last: '||ptbl_curr_sorteo.last);

	if ptbl_prev_sorteo.count <= ptbl_curr_sorteo.count then
		ln$index_end  := ptbl_prev_sorteo.count;
--		ln$prev_index := ptbl_prev_sorteo.first; 
--		ln$curr_index := ptbl_curr_sorteo.first;
	elsif ptbl_prev_sorteo.count > ptbl_curr_sorteo.count then
		ln$index_end  := ptbl_curr_sorteo.count;
--		ln$prev_index := ptbl_prev_sorteo.first; 
--		ln$curr_index := ptbl_curr_sorteo.first;
--	elsif ptbl_prev_sorteo.count < ptbl_curr_sorteo.count then
--		ln$index_end  := ptbl_prev_sorteo.count;
--		ln$prev_index := ptbl_prev_sorteo.first; 
--		ln$curr_index := ptbl_curr_sorteo.first;	
	end if;
	ln$prev_index := ptbl_prev_sorteo.first; 
	ln$curr_index := ptbl_curr_sorteo.first;
/*
	DBMS_OUTPUT.PUT_LINE('ln$index_ini: '||ln$index_ini);
    DBMS_OUTPUT.PUT_LINE('ln$index_end: '||ln$index_end);
	DBMS_OUTPUT.PUT_LINE('ln$prev_index: '||ln$prev_index);
    DBMS_OUTPUT.PUT_LINE('ln$curr_index: '||ln$curr_index);

begin
for m in ln$index_ini..ln$index_end loop
	
	DBMS_OUTPUT.PUT_LINE('PREV: '||'|'||ptbl_prev_sorteo(ln$prev_index).rango_ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).drawing_id||'|'||ptbl_prev_sorteo(ln$prev_index).b_type||'|'||ptbl_prev_sorteo(ln$prev_index).digit||'|'||ptbl_prev_sorteo(ln$prev_index).color_ubicacion||'|'||ptbl_prev_sorteo(ln$prev_index).ubicacion||'|'||ptbl_prev_sorteo(ln$prev_index).color_ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).ciclo_aparicion||'|'||ptbl_prev_sorteo(ln$prev_index).pronos_ciclo||'|'||ptbl_prev_sorteo(ln$prev_index).preferencia_flag||'|'||ptbl_prev_sorteo(ln$prev_index).copy_flag||'|'||'CURR: '||'|'||ptbl_curr_sorteo(ln$curr_index).rango_ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).drawing_id||'|'||ptbl_curr_sorteo(ln$curr_index).b_type||'|'||ptbl_curr_sorteo(ln$curr_index).digit||'|'||ptbl_curr_sorteo(ln$curr_index).color_ubicacion||'|'||ptbl_curr_sorteo(ln$curr_index).ubicacion||'|'||ptbl_curr_sorteo(ln$curr_index).color_ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).ciclo_aparicion||'|'||ptbl_curr_sorteo(ln$curr_index).pronos_ciclo||'|'||ptbl_curr_sorteo(ln$curr_index).preferencia_flag||'|'||ptbl_curr_sorteo(ln$curr_index).copy_flag);
	ln$prev_index := ln$prev_index + 1;
	ln$curr_index := ln$curr_index + 1;
end loop;
exception
when others then null;
end;	
*/
		--!validando la posicion
		ln$prev_index := ptbl_prev_sorteo.first; 
		ln$curr_index := ptbl_curr_sorteo.first;
		for r in ln$index_ini..ln$index_end loop
DBMS_OUTPUT.PUT_LINE(r||'  prev ID:'||ptbl_prev_sorteo(ln$prev_index).drawing_id||' TYPE: '||ptbl_prev_sorteo(ln$prev_index).b_type||' DGT: '||ptbl_prev_sorteo(ln$prev_index).digit||' UB: '||ptbl_prev_sorteo(ln$prev_index).ubicacion);
DBMS_OUTPUT.PUT_LINE(r||'  curr ID:'||ptbl_curr_sorteo(ln$curr_index).drawing_id||' TYPE: '||ptbl_curr_sorteo(ln$curr_index).b_type||' DGT: '||ptbl_curr_sorteo(ln$curr_index).digit||' UB: '||ptbl_curr_sorteo(ln$curr_index).ubicacion);
--DBMS_OUTPUT.PUT_LINE('PREV: '||'|'||ptbl_prev_sorteo(ln$prev_index).rango_ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).drawing_id||'|'||ptbl_prev_sorteo(ln$prev_index).b_type||'|'||ptbl_prev_sorteo(ln$prev_index).digit||'|'||ptbl_prev_sorteo(ln$prev_index).color_ubicacion||'|'||ptbl_prev_sorteo(ln$prev_index).ubicacion||'|'||ptbl_prev_sorteo(ln$prev_index).color_ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).ley_tercio||'|'||ptbl_prev_sorteo(ln$prev_index).ciclo_aparicion||'|'||ptbl_prev_sorteo(ln$prev_index).pronos_ciclo||'|'||ptbl_prev_sorteo(ln$prev_index).preferencia_flag||'|'||ptbl_prev_sorteo(ln$prev_index).copy_flag||'|'||'CURR: '||'|'||ptbl_curr_sorteo(ln$curr_index).rango_ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).drawing_id||'|'||ptbl_curr_sorteo(ln$curr_index).b_type||'|'||ptbl_curr_sorteo(ln$curr_index).digit||'|'||ptbl_curr_sorteo(ln$curr_index).color_ubicacion||'|'||ptbl_curr_sorteo(ln$curr_index).ubicacion||'|'||ptbl_curr_sorteo(ln$curr_index).color_ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).ley_tercio||'|'||ptbl_curr_sorteo(ln$curr_index).ciclo_aparicion||'|'||ptbl_curr_sorteo(ln$curr_index).pronos_ciclo||'|'||ptbl_curr_sorteo(ln$curr_index).preferencia_flag||'|'||ptbl_curr_sorteo(ln$curr_index).copy_flag);

				--!validando la posicion				
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select count(1) cnt
				  into olap_sys.w_common_pkg.g_rowcnt
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
				   and rango_ley_tercio = ptbl_curr_sorteo(ln$curr_index).rango_ley_tercio;

				
				if olap_sys.w_common_pkg.g_rowcnt = 0 then
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_POSICION.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);
					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_posicion    = 'PO'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
						   and rango_ley_tercio	= ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO;	
					end if;														
				end if;


				--!validando ubicacion
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select count(1) cnt
				  into olap_sys.w_common_pkg.g_rowcnt
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
				   and color_ubicacion  = ptbl_curr_sorteo(ln$curr_index).color_ubicacion
				   and ubicacion 		= ptbl_curr_sorteo(ln$curr_index).ubicacion;
				
				if olap_sys.w_common_pkg.g_rowcnt = 0 then				
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_UBICACION.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);

					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_ubicacion   = 'UB'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
						   and color_ubicacion  = ptbl_curr_sorteo(ln$curr_index).color_ubicacion
						   and ubicacion 		= ptbl_curr_sorteo(ln$curr_index).ubicacion;	
					end if;	
				end if;			


				--!validando ley del tercio
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select count(1) cnt
				  into olap_sys.w_common_pkg.g_rowcnt
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
				   and color_ley_tercio = ptbl_curr_sorteo(ln$curr_index).color_ley_tercio
				   and ley_tercio 		= ptbl_curr_sorteo(ln$curr_index).ley_tercio;
				
				if olap_sys.w_common_pkg.g_rowcnt = 0 then				
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_LEY_TERCIO.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);

					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_ley_tercio   = 'LT'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
						   and color_ley_tercio = ptbl_curr_sorteo(ln$curr_index).color_ley_tercio
						   and ley_tercio 		= ptbl_curr_sorteo(ln$curr_index).ley_tercio;	
					end if;	
				end if;	


				--!validando ciclo aparicion		
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select count(1) cnt
				  into olap_sys.w_common_pkg.g_rowcnt
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
				   and ciclo_aparicion = ptbl_curr_sorteo(ln$curr_index).ciclo_aparicion;
				
				if olap_sys.w_common_pkg.g_rowcnt = 0 then				
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_CICLO_APARICION.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);

					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_ciclo_aparicion   = 'CA'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
						   and ciclo_aparicion = ptbl_curr_sorteo(ln$curr_index).ciclo_aparicion;
					end if;	
				end if;	

				--!validando pronostico x ciclo		
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select count(1) cnt
				  into olap_sys.w_common_pkg.g_rowcnt
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
				   and nvl(pronos_ciclo,00) = nvl(ptbl_curr_sorteo(ln$curr_index).pronos_ciclo,99);
				
				if olap_sys.w_common_pkg.g_rowcnt = 0 then				
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_PRONOS_CICLO.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);

					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_pronos_ciclo   = 'XC'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit
						   and nvl(pronos_ciclo,00) = nvl(ptbl_curr_sorteo(ln$curr_index).pronos_ciclo,99);
					end if;	
				end if;	


				--!validando preferencia flag		
				--!buscando los valores actuales en el registro del sorteo anterior
				olap_sys.w_common_pkg.g_rowcnt := 0;
				select preferencia_flag
				  into lv$prev_preferencia_flag
				  from olap_sys.s_calculo_stats
				 where drawing_id   	= pn_gambling_id-1
				   and b_type       	= pv_b_type
				   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit;
				
				if (lv$prev_preferencia_flag is null and ptbl_curr_sorteo(ln$curr_index).preferencia_flag is not null) or
				   (lv$prev_preferencia_flag is not null and ptbl_curr_sorteo(ln$curr_index).preferencia_flag is null) or				
				   lv$prev_preferencia_flag != ptbl_curr_sorteo(ln$curr_index).preferencia_flag then				
					--!se actualiza el drawing_id actual en la tabla para hacer match con la info de al hoja de excel
					DBMS_OUTPUT.PUT_LINE('CHNG_PREFERENCIA_FLAG.  drawing_id: '||pn_gambling_id||' b_type: '||pv_b_type||' digit: '||ptbl_curr_sorteo(ln$curr_index).digit||' rango_ley_tercio: '||ptbl_curr_sorteo(ln$curr_index).RANGO_LEY_TERCIO);

					if pv_actualizar_cambios = 'Y' then
						update olap_sys.s_calculo_stats 
						   set chng_preferencia_flag   = 'FL'
						 where drawing_id 		= pn_gambling_id
						   and b_type 			= pv_b_type 
						   and digit        	= ptbl_curr_sorteo(ln$curr_index).digit;
					end if;	
				end if;	

			ln$prev_index := ln$prev_index + 1;
			ln$curr_index := ln$curr_index + 1;			
		end loop;

		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;		
  exception
     when others then
	    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end comparar_info_arreglos;  	
 
 
  --!proceso principal para detectar cambios en la info del sorteo actual con respecto al anterior 
  procedure gl_comparar_sorteo_inf_handler (pv_gambling_type  	           varchar2
										  , pn_gambling_id                 number
										  , pv_actualizar_cambios          varchar2 default 'Y'
										  , x_err_code       in out NOCOPY number 								 
										   ) is
										   
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'gl_comparar_sorteo_inf_handler'; 
	ltbl$curr_sorteo              gt$sorteo_tbl;
	ltbl$prev_sorteo			  gt$sorteo_tbl;
	lv$curr_b_type				  VARCHAR2(2);
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	DBMS_OUTPUT.ENABLE(NULL);
								   
	for t in 1..6 loop
	lv$curr_b_type := 'B'||t;
	--!proceso principal para extraer info de los sorteos y llenar los arreglos
    popular_arreglos (pv_gambling_type => pv_gambling_type
                    , pn_gambling_id   => pn_gambling_id
					, pv_b_type		   => lv$curr_b_type
					, xtbl_prev_sorteo => ltbl$prev_sorteo
	                , xtbl_curr_sorteo => ltbl$curr_sorteo
					, x_err_code       => x_err_code								 
					 );
		
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!proceso para comparar la informacion de ambos arreglos
			comparar_info_arreglos (ptbl_prev_sorteo      => ltbl$prev_sorteo
								  , ptbl_curr_sorteo      => ltbl$curr_sorteo
								  , pn_gambling_id        => pn_gambling_id
								  , pv_b_type             => lv$curr_b_type
								  , pv_actualizar_cambios => pv_actualizar_cambios
								  , x_err_code            => x_err_code								 
								   );									
		end if;
	end loop;	
  exception
     when others then
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end gl_comparar_sorteo_inf_handler;  										   


  procedure pm_panorama_primos_handler (pv_gambling_type  	         	olap_sys.sl_gamblings.gambling_type%type
                                      , pn_gambling_id               	number
									  , x_err_code        in out NOCOPY number 	
									   ) is
    LV$PROCEDURE_NAME    CONSTANT VARCHAR2(30) := 'pm_panorama_primos_handler'; 	
  begin 
	DBMS_OUTPUT.PUT_LINE('-------------------------------');
	DBMS_OUTPUT.PUT_LINE(LV$PROCEDURE_NAME);
	
	update olap_sys.pm_panorama_primos pp
	   set pp.drawing_id = pn_gambling_id
	 where (attribute3, comb1, comb2, comb3, comb4, comb5, comb6) in (select pv_gambling_type, comb1, comb2, comb3, comb4, comb5, comb6
																		from olap_sys.pm_mr_resultados_v2
																	   where gambling_id = pn_gambling_id
																		 and pn_cnt = 2);
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
  exception
     when others then
		DBMS_OUTPUT.PUT_LINE(SQLERRM);		 
  end pm_panorama_primos_handler; 



  
END t_data_loader_pkg;  
/    
show errors;
     
     
     
     
     