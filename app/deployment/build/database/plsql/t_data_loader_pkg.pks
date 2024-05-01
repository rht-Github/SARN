rem purpose      : package used to load data from a flat file into database 
rem creation_date: 02/28/2010
rem change_desc  : adding counts for insert process
rem change date  : 03/31/2010

prompt creating package specification t_data_loader_pkg

create or replace package olap_sys.t_data_loader_pkg as

  GV$PACKAGE_NAME         constant varchar2(100) := 't_data_loader_pkg';
  GV$CONTEXT              constant varchar2(50)  := 'UPD_USR_DRAWING';
  g_package_name   constant varchar2(30) := 'olap_sys.t_data_loader_pkg';
  g_inscnt         number := 0;
  g_errorcnt       number := 0;
  g_rowcnt	       number := 0; 
  
  GN$DEFAULT_HIGH_DIGIT   constant number(2)   := 99; 
  GN$DEFAULT_LOW_DIGIT    constant number(2)   := -1;

  type typ_gamb is table of olap_sys.sl_gamblings%rowtype index by binary_integer;

  --plsql table used for computing inbound counts
  type gt$inbound_rec is record (gambling_id       number
                               , comb1             number
                               , comb2             number
                               , comb3             number
                               , comb4             number
                               , comb5             number
                               , comb6             number
                               , comb_sum          number
                               , sum_par_comb      number
                               , sum_mod_comb      number
                               , global_index      number
                               , prime_number_cnt  number
                                );
  type gt$inbound_tbl is table of gt$inbound_rec index by binary_integer;  

  type gt$inb_cnt_rec is record (attribute         varchar2(30)
                               , cur_value1        number
                               , cur_value2        number
                               , next_value1       number
                               , next_value2       number
                               , cnt               number
                               , attribute1        number
                               , attribute2        number
                               , attribute3        number
                               , attribute4        number
                                );
  type gt$inb_cnt_tbl is table of gt$inb_cnt_rec index by binary_integer; 
 
  type gt$sorteo_rec is record (rango_ley_tercio	number
							  , drawing_id			number
							  , b_type				varchar2(2)
							  , digit				number
							  , color_ubicacion		number
							  , ubicacion			number
							  , color_ley_tercio	number
							  , ley_tercio			number
							  , ciclo_aparicion		number
							  , pronos_ciclo		number
							  , preferencia_flag    varchar2(1)
                              , copy_flag           varchar2(1)							  
							   );
  type gt$sorteo_tbl is table of gt$sorteo_rec index by binary_integer; 
 
  procedure main_p (p_gambling_type                olap_sys.t_gambling_types.gambling_type%type
                  , x_err_code       in out NOCOPY number     
                  , x_err_msg        in out NOCOPY varchar2   
                   );
 
  type gt$b_type_rec is record (b_type					 varchar2(2)
							  , prev_type_index_ini      number
							  , prev_type_index_end      number
							  , prev_type_cnt            number
							  , curr_type_index_ini      number
							  , curr_type_index_end      number
							  , curr_type_cnt            number
							   );
							   
  type gt$b_type_tbl is table of gt$b_type_rec index by binary_integer; 

 
  procedure validate_gambling_type (p_gambling_type olap_sys.t_gambling_types.gambling_type%type);

  procedure ins_target_table (p_gambling_date             olap_sys.sl_gamblings.gambling_date%type
                            , p_gambling_id               olap_sys.sl_gamblings.gambling_id%type
                            , p_comb1                     olap_sys.sl_gamblings.comb1%type
                            , p_comb2                     olap_sys.sl_gamblings.comb2%type
                            , p_comb3                     olap_sys.sl_gamblings.comb3%type
                            , p_comb4                     olap_sys.sl_gamblings.comb4%type
                            , p_comb5                     olap_sys.sl_gamblings.comb5%type
                            , p_comb6                     olap_sys.sl_gamblings.comb6%type
                            , p_additional                olap_sys.sl_gamblings.additional%type
                            , p_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                            , p_comb_sum                  olap_sys.sl_gamblings.comb_sum%type
                            , p_price                     olap_sys.sl_gamblings.price%type default 0
                            , p_priority       					  olap_sys.sl_gamblings.priority%type
                            , p_inscnt         in out number 
                            , p_errorcnt       in out number
                            );

  procedure get_rowcnt_target_table (p_gambling_type olap_sys.t_gambling_types.gambling_type%type);

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
                     );
  
  procedure winner_vendor_drawing_handler (p_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , x_err_code       in out NOCOPY number     
                                         , x_err_msg        in out NOCOPY varchar2   
                                          );
  
  function get_max_gambling_id (p_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type) return number;
  
  procedure load_plsql_table (p_gambling_type  	             olap_sys.sl_gamblings.gambling_type%type
                            , x_err_code       in out NOCOPY number     
                            , x_err_msg        in out NOCOPY varchar2   
                             );

  procedure get_bingo_combs (x_GambTbl        in out NOCOPY typ_gamb
                           , x_err_code       in out NOCOPY number     
                           , x_err_msg        in out NOCOPY varchar2   
                            );

  procedure upd_sl_gamblings_bingo (p_GambTbl  	                   typ_gamb
                                  , x_err_code       in out NOCOPY number     
                                  , x_err_msg        in out NOCOPY varchar2   
                                   );
 
  --[procedure used for reading data from external table and inserting sorted data into stating table                                         
  procedure preview_gamblings_handler (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                     , x_err_code       in out NOCOPY number       
                                      );

  --[procedure used for setting up metadata headers on table s_metadata_select_headers in order to split the drawings among active players                                         
  procedure setup_metadata_headers_handler (pv_gambling_type  	      olap_sys.sl_gamblings.gambling_type%type
                                          , pv_gambling_day                varchar2
                                          , x_err_code       in out NOCOPY number       
                                           );

  --[procedure used for updating counts on gigaloteria patterns
  procedure gigaloterias_patterns_handler (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                 number
                                         , x_err_code       in out NOCOPY number       
                                        );  

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
                                          );

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
                                       );

  procedure ciclo_aparicion_stats_handler (pv_gambling_type  	          olap_sys.sl_gamblings.gambling_type%type
                                         , pn_gambling_id                     number
                                         , x_err_code           in out NOCOPY number);
  
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
                                       );
     
  procedure ins_calculo_stats_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                        , pn_gambling_id                 number
                                        , pn_comb1                       number
                                        , pn_comb2                       number
                                        , pn_comb3                       number
                                        , pn_comb4                       number
                                        , pn_comb5                       number
                                        , pn_comb6                       number
                                        , x_err_code       in out NOCOPY number       
                                         );       

  procedure repeated_numbers_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                    , pn_gambling_id                 number
                                    , pn_comb1                       number
                                    , pn_comb2                       number
                                    , pn_comb3                       number
                                    , pn_comb4                       number
                                    , pn_comb5                       number
                                    , pn_comb6                       number
                                    , x_err_code       in out NOCOPY number       
                                     );       

  procedure prime_pairs_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                               , pn_gambling_id                 number
                               , pn_comb1                       number
                               , pn_comb2                       number
                               , pn_comb3                       number
                               , pn_comb4                       number
                               , pn_comb5                       number
                               , pn_comb6                       number
                               , x_err_code       in out NOCOPY number       
                                );       

  procedure ley_tercio_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                              , pn_gambling_id                 number
                              , x_err_code       in out NOCOPY number       
                               );       

  procedure terminaciones_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                 , pn_gambling_id                 number
                                 , pn_comb1                       number
                                 , pn_comb2                       number
                                 , pn_comb3                       number
                                 , pn_comb4                       number
                                 , pn_comb5                       number
                                 , pn_comb6                       number								 
                                 , x_err_code       in out NOCOPY number       
                                  );  

/*                                         
  procedure s_drawings_comparisons_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                          , pn_gambling_id                 number
                                          , x_err_code       in out NOCOPY number
                                            );
*/                                                                                                                                                                 

  procedure panorama_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                            , pn_gambling_id                 number
                            , x_err_code       in out NOCOPY number
                             );

  procedure decenas_numeros_primos_handler (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                          , pn_gambling_id                 number
                                          , pn_comb1                       number
                                          , pn_comb2                       number
                                          , pn_comb3                       number
                                          , pn_comb4                       number
                                          , pn_comb5                       number
                                          , pn_comb6                       number								 
                                          , x_err_code       in out NOCOPY number       
                                           );  

  procedure pm_parejas_primos_log_handler (pv_gambling_type  	        varchar2
										 , pn_primo_ini					number
										 , pn_primo_fin					number
										 , pn_diferencia				number
										 , pn_drawing_id				number
										 , pv_drawing_list				varchar2								 
										  );

  procedure gl_comparar_sorteo_inf_handler (pv_gambling_type  	           varchar2
										  , pn_gambling_id                 number
										  , pv_actualizar_cambios          varchar2 default 'Y'
										  , x_err_code       in out NOCOPY number 								 
										   );
										   
  procedure pm_panorama_primos_handler (pv_gambling_type  	         	olap_sys.sl_gamblings.gambling_type%type
                                      , pn_gambling_id               	number
									  , x_err_code        in out NOCOPY number 	
									   );
  
end t_data_loader_pkg;
/
show errors;