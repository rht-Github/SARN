rem purpose      : package used to allocate common procedures might be used by other packages 
rem creation_date: 02/28/2010
rem change_desc  : adding g_delcnt global variable
rem change_date  : 03/16/2010
rem change_desc  : adding procedure get_sl_gamblings and type g_gam_rec
rem change_date  : 03/18/2010

prompt creating package specification olap_sys.w_common_pkg

create or replace package olap_sys.w_common_pkg as

GB$SHOW_PROC_NAME                boolean       := false;   


--[----------------------]--
--[ global pl/sql tables }--
--[----------------------]--

--[ plsql table used to allocate metadata for dynamic queries
type gt$qry_stmt_rec is record (query_id              number
                              , primary_select        varchar2(1000)
                              , primary_where         varchar2(4000)
                              , primary_detail        varchar2(4000)
                              , primary_group_by      varchar2(100)
                              , primary_order_by      varchar2(100)
                              , primary_category      varchar2(10)
                              , execution_order       number(2)
                              , secondary_select      varchar2(1000)
                              , secondary_detail      varchar2(4000)
                              , secondary_group_by    varchar2(1000)
                              , secondary_order_by    varchar2(1000)
                              , secondary_category    varchar2(10));
type gt$qry_stmt_tbl is table of gt$qry_stmt_rec index by binary_integer;   

--[ plsql table used to compute probabilities and saved them into temporal table olap_sys.tmp_computed_values
type gt$tmp_rec is table of olap_sys.tmp_computed_values%rowtype index by binary_integer; 

type gt$sort_by_rec is record (sort_by          number
                             , avg_sort_by      number
                             , stddev_sort_by   number
                             , lr_sort_by       number
                             , hr_sort_by       number);

type gt$sort_by_tbl is table of gt$sort_by_rec index by binary_integer;                             


type gt$pm_comp_by_tens_rec is record (ten_composition     varchar2(7)
                                     , accumulated  number);

type gt$pm_comp_by_tens_tbl is table of gt$pm_comp_by_tens_rec index by binary_integer;    

--!record para almacenar los metadatos del peso de las jugadas
type gt$gam_weight_rec is record (weight          	number
							    , pattern      	varchar2(10)
								, termination      varchar2(10));

type gt$gam_weight_tbl is table of gt$gam_weight_rec index by binary_integer;
            
  --|----------------------------|--
  --|   global constant values   |--
  --|----------------------------|--

  -- filtering only the top n combinations in a query
  g_top_n          	        constant number(2) := 10;
  
  -- constraint used on loop
  g_#_combinations 	        constant number(1) := 7;        -- combinations from 1 to 6 plus additional
  
  -- lowest value of a whole combination
  g_low_limit     	        constant number    := 1;
  
  -- highest value of a whole combination
  g_high_limit     	        constant number    := 49;

  -- number of days after last repetition of a combination
  g_days_after_last_repetition  constant number    := 120;
  
  -- number of repetitions of a combination
  g_#_repetitions               constant number    := 8;
  
  -- constant value for combinations based on two numbers   
  g_two_combinations 	        constant number(1) := 2;

  -- constant value for combinations based on three numbers   
  g_three_combinations          constant number(1) := 3;

  g_additional_combination   	constant number(1) := 7;

  -- date format
  g_date_format        		constant varchar2(10) := 'dd-mm-yyyy';

  -- date format using day - month
  g_short_date_format  		constant varchar2(10) := 'dd-mm';

  -- date format using month    
  g_month_date_format  		constant varchar2(10) := 'MM';

  -- date format using month    
  g_quarter_date_format  	constant varchar2(10) := 'Q';

  -- date format using year          
  g_year_date_format   		constant varchar2(10) := 'YYYY';   

  g_sysdate            		constant date         := sysdate;  

  g_username           		constant varchar2(30) := user;  
  
  g_number_format               constant varchar(7)   := '9999999';
 
  g_commit_rows                 constant number       := 500;
  
  --<05092010. begin>
  g_#_rep_comb_details		constant number       := 3;
  --<05092010. begin>

  --<07142010. begin>
  g_app_date_format        	constant varchar2(10) := 'mm/dd/yyyy';
  --<07142010. end>
  
  g_delimiter                   constant varchar2(1)  := '|';
  
  g_range_interval              constant number       := 20;

  gn_min_value                  constant number       := 1;

  gn_max_value                  constant number       := 6;
  
  --[ variable used for formating mail text in the body
  gv$enter                      constant varchar2(5) := chr(10);

  GV$PACKAGE_NAME         constant varchar2(100) := 'W_COMMOM_PKG';
  GV$CONTEXT              constant varchar2(50)  := 'COMMON_CONTEXT';

  GN$FAILED_EXECUTION     constant number(1)     := 1;
  GN$SUCCESSFUL_EXECUTION constant number(1)     := 0;
  GV$SUCCESSFUL_EXECUTION constant varchar2(100) := ' successfully completed.';
  
  --[ Contexts used for logging records
  GV_CONTEXT_ERROR        constant varchar2(100) := 'ERROR';
  GV_CONTEXT_INFO         constant varchar2(100) := 'INFO';
  GV_CONTEXT_WARNING      constant varchar2(100) := 'WARNING';

  --[ status for tale w_combination_responses_fs
--  GV$PICKED              constant varchar2(1) := 'P'; column not used anymore
  GV$ASSIGNED            constant varchar2(1) := 'A';
  GV$EXCLUDED            constant varchar2(1) := 'E';
--  GV$INCLUDED            constant varchar2(1) := 'I'; column not used anymore
  GV$FAILED              constant varchar2(1) := 'F';
  GV$WINNER              constant varchar2(1) := 'W';

  GN$RANKING_PRICE_ONE   constant number(1) := 1;

  --[ mail types
  GV$MAIL_TYPE_SUBMIT    constant varchar2(1) := 'S';
  GV$MAIL_TYPE_CONGRANTS constant varchar2(1) := 'C';
  GV$MAIL_TYPE_INFO      constant varchar2(1) := 'I';  
  GV$MAIL_TYPE_BALANCE   constant varchar2(1) := 'B';  
  
  --[ mail send flag
  GV$MAIL_SENT           constant varchar2(1) := 'Y';
  GV$MAIL_NOT_SENT       constant varchar2(1) := 'N';    
   
  --[ user type
  GV$USER_SPONSOR        constant varchar2(1) := 'S';
  GV$USER_OWNER          constant varchar2(1) := 'O';
  
  --[ mail from
  GV$MAIL_FROM           constant varchar2(100) := 'sarn.autosender@gmail.com';
  
  --[ pronosticos link
  GV$LINK1               constant varchar2(200) := 'http://www.pronosticos.gob.mx';
  GV$LINK2               constant varchar2(200) := 'http://www.tujugada.com.ar/mexico_melate_retro.asp';
  
  GN$DO_COMMIT           constant number := 1000;

  --[ sponsorship type
  GV$SPONSORSHIP_GAMBLING   constant varchar2(30) := 'GAM';
  GV$SPONSORSHIP_BUSINESS   constant varchar2(30) := 'BUS';
   
  GV$DOLLAR_FORMAT          constant varchar2(30) := 'L99G999G999G999D99MI';

  GN$AVG_GLOBAL_INDEX_LR   constant number := 689473.9;
  GN$AVG_GLOBAL_INDEX      constant number := 1631311.9;
  GN$AVG_GLOBAL_INDEX_HR   constant number := 2573149.9;  

  GN$AVG_COMB_SUM_LR       constant number := 93.9;
  GN$AVG_COMB_SUM          constant number := 119.9;
  GN$AVG_COMB_SUM_HR       constant number := 145.9;  

  CF$PERCENTILE_INI        constant float := 0.1;
  CF$PERCENTILE_END        constant float := 0.9;
   
  --|----------------------------|--
  --|      global variables      |--
  --|----------------------------|--

  -- used to build dynamic dml statements
  g_dml_stmt                	varchar2(10000);
       
  -- column name used for dynamic dml statements       
  g_column_name             	varchar2(30);
  
  -- column value used for dynamic dml statements       
  g_column_value            	number;
  
  -- flag used when query found data
  g_data_found         		number       := 0;  
  
  -- date when gambling took place  
  g_gambling_date      		date;  
  
  -- identifier for data load
  g_load_id            		number       := 0;
  
  -- flag to show dbms_output messages 
  g_show_message       		boolean := TRUE;
  
  -- flag used when a error is raised
  g_err_flag           		varchar2(100);

  -- used to count inserts done on a table  
  g_inscnt			number       := 0;  

  -- used to count updates done on a table  
  g_updcnt			number       := 0;  

  -- used to count deletes done on a table  
  g_delcnt			number       := 0;  

  -- used to count rows  
  g_rowcnt			number       := 0;  

  -- used to count error exceptions  
  g_errorcnt			number       := 0;  

  -- used to count how many rows were handled into a loop
  g_index                       number       := 0;
  
  -- global variable used for month using mm format
  g_month              		number(2);

  -- global variable used for quartes  
  g_quarter            		number(1);

  -- global variable used for half    
  g_half               		varchar2(1);

  -- global variable used for year
  g_year               		number(4);
  
  -- gobal variable for returning number data type in functions
  g_return_value                number := 0 ;
  
  --pattern from all 6 positions
  g_pattern			VARCHAR2(6);
  
  -- type for ref cursors
  type g_refcur        		is ref cursor;

  --TYPE g_refcur_dgtcnt IS REF CURSOR RETURN olap_sys.s_user_drawing_digit_counts%ROWTYPE;

  --<03182010. begin>
  
  -- global variable used for segment code
  g_segment_code                olap_sys.w_segments_d.segment_code%type;
  
  -- global variable used for segment order
  g_segment_order               olap_sys.w_segments_d.segment_order%type;
  
  type g_gam_rec is record  ( 
                             attribute1     date 	        --gambling_date
                           , attribute2     number 		--gambling_id
                           , attribute3     varchar2(10) 	--gambling_type
                           , attribute4     number 		--comb1
                           , attribute5     number 		--comb2
                           , attribute6     number 		--comb3
                           , attribute7     number 		--comb4
                           , attribute8     number 		--comb5
                           , attribute9     number 		--comb6
                           , attribute10    number 		--additional
                           , attribute11    number              --comb_sum
                           , attribute12    number              --priority
                           , attribute13    number              --sum_par_comb
                            );
                            
  type g_gam_tbl is table of g_gam_rec index by binary_integer;
  --<03182010. end>

  --!type usado para convertir un string separado por comas en un arreglo
  type gt$row_rec is table of varchar2(100);
  type gt$row_tbl is table of gt$row_rec index by binary_integer;


 --!type usado para almacenar todos los resultados 
 type g_resultado_rec is record  (id     number 		--gambling_id
								, pos1   number
								, pos2   number
								, pos3   number
								, pos4   number
								, pos5   number
								, pos6   number);
								
 type g_resultado_tbl is table of g_resultado_rec index by binary_integer;								

 --!type usado para contar las coincidencias de los digitos
 type g_contador_rec is record  (digito     number 		--gambling_id
								,cnt        number
								,ultimo_id  number);
								
 type g_contador_tbl is table of g_contador_rec index by binary_integer;	  

 --!type usado ubicar la suma de coincidencias de numeros primos
 type g_primos_sum_rec is record  (primo_sum		number
								 , bandera		    varchar2(1) default 'N');
								
 type g_primos_sum_tbl is table of g_primos_sum_rec index by binary_integer; 
 
  --[ variables used for handling dynamic queries
  gv$primary_qry_stmt              varchar2(10000);
  gv$secondary_qry_stmt            varchar2(10000);  
  gv$remaining_where_clause        varchar2(10000);


  --|----------------------------|--
  --|      global cursors        |--
  --|----------------------------|--
  CURSOR c_details (pv_drawing_type olap_sys.w_comb_setup_header_fs.attribute3%TYPE
                  , pv_gambling_day VARCHAR2) IS
  select dd.user_id
       , dd.drawings_per_day
    from olap_sys.c_users u
       , olap_sys.r_drawing_users du
       , olap_sys.s_metadata_select_details dd
   where u.id             = du.user_id
     and du.drawing_type  = dd.drawing_type
     and du.user_id       = dd.user_id
     and du.drawing_day   = dd.drawing_day   
     and du.drawing_type  = pv_drawing_type
     and du.drawing_day   = pv_gambling_day
     and du.status        = 'A'
     and dd.status        = 'A'
     and u.locked         = 'N'
     and u.stop_login_date is NULL
   ORDER BY dbms_random.RANDOM;
     
  --|----------------------------|--
  --|      procedures            |--
  --|----------------------------|--

  -- based on data allocated on w_segments_d table returns a segment code
  procedure get_segment_code (p_gambling_type     varchar2
                            , p_attribute         number
                            , p_no_segments       olap_sys.w_segments_d.no_segments%type
                            , p_segment_code  out olap_sys.w_segments_d.segment_code%type
                            , p_segment_order out olap_sys.w_segments_d.segment_order%type
                             );

  -- procedure used to retrieved data from sl_gamblings table
  procedure get_sl_gamblings (p_gambling_type        varchar2
                            , p_gambling_date        date 
                            , p_gam_tbl       in out g_gam_tbl
                             );
  --<03182010. end>
                   
  --<05092010. begin>
  -- function to get the total of counts on sl_gamblings
--  procedure main (p_gambling_type             varchar2);
                         
  function get_t_gambling_types_comb_no (p_gambling_type     olap_sys.t_gambling_types.gambling_type%type) return number;
  
  --[funtion used to return a numeric equivalent value based on a input combination value
  function get_equivalent_value (p_combination varchar2) return number;
  
  --[procedure used to retrieve min max values based on table t_gambling_types
  procedure get_t_gambling_types_min_max (p_gambling_type         olap_sys.t_gambling_types.gambling_type%type
                                        , p_min_value      in out olap_sys.t_gambling_types.min_value%type
                                        , p_max_value      in out olap_sys.t_gambling_types.max_value%type
                                         );	
                                         
  --[ function used to return an equivalent value based on high and low values logic                                       
  function convert_f (p_number          number
                    , p_convert_type    varchar2 default 'DIGIT'
                    , p_gambling_type   varchar2 ) return number;

  --[ function used to return an equivalent value based on low-low, low-high, high-low and high-high values logic                                       
  function convert_low_high_f (p_number          number
                             , p_convert_type    varchar2 default 'DIGIT'
                             , p_gambling_type   varchar2 default 'mrtr') return number;    

  --[ function used to return an detailed equivalent value based on low-low, low-high, high-low and high-high values logic                                       
  function convert_low_high_dtl_f (p_number          number) return number;      
                           
  --[ procedure used to save data into log table
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
					 
   --[ function that will return a number value
   function get_lookup_values_1_colnum (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                      , pv_context                     olap_sys.w_lookups_fs.context%type
                                      , pv_code                        olap_sys.w_lookups_fs.code%type
                                      , pv_column_name                 varchar2
                                        ) return number RESULT_CACHE;

   --[ function that will return a varchar2 value
   function get_lookup_values_1_colvar (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                      , pv_context                     olap_sys.w_lookups_fs.context%type
                                      , pv_code                        olap_sys.w_lookups_fs.code%type
                                      , pv_column_name                 varchar2
                                       ) return varchar2 RESULT_CACHE;
   
   --[ procedure that will return two varchar2 values                                   
   procedure get_lookup_values_2_colvar (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                       , pv_context                     olap_sys.w_lookups_fs.context%type
                                       , pv_code                        olap_sys.w_lookups_fs.code%type
                                       , xv_attribute3    in out NOCOPY olap_sys.w_lookups_fs.attribute3%type
                                       , xv_attribute4    in out NOCOPY olap_sys.w_lookups_fs.attribute4%type
                                        );        
										
					 
 /* 
   --[ function used to retrieve the host name used to send emails to outside world                                     
   function get_nls_charset return varchar2 RESULT_CACHE;                                                              

   --[ Write a MIME header
   procedure write_mime_header (p_conn   in out nocopy utl_smtp.connection
                              , pv_name                varchar2
                              , pv_value               varchar2
                               );
                                 
   --[ procedure used to send emails out to existing users inserted into table c_users                          
   procedure send_mail (pv_sender                             varchar2
                      , pv_recipient                          varchar2
                      , pv_subject                            varchar2
                      , pv_msg_text                           varchar2 
                      , pv_nls_charset                        varchar2 default 'WE8MSWIN1252'
                      , x_err_code              in out NOCOPY number
                      , x_err_msg               in out NOCOPY varchar2  
                      );
   
   --[ function used to get number of emails not sent while a process is executed
   function get_cnt_mails_not_sent (pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type 
                                  , pn_setup_id                         olap_sys.mails_sent_history_f.setup_id%type default null
                                  , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type default 'S'
                                  , pv_send_flag                        olap_sys.mails_sent_history_f.send_flag%type default 'N'
                                  , pv_assigned_to                      olap_sys.mails_sent_history_f.assigned_to%type default null 
                                  , pn_year                             olap_sys.mails_sent_history_f.year%type default null 
                                  , pn_quarter                          olap_sys.mails_sent_history_f.quarter%type default null
                                  , pn_month                            olap_sys.mails_sent_history_f.month%type default null
                                   ) return number RESULT_CACHE; 
   
   


                              
  --[ wrapper used to call procedure send_email
/*  procedure send_mail_p (pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type
                       , pn_setup_id                         olap_sys.mails_sent_history_f.setup_id%type
                       , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type
                       , x_err_code            in out NOCOPY number
                       , x_err_msg             in out NOCOPY varchar2                            
                        );*/


  --[ procedure used to save mail history
  procedure save_mail_history (pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type   
                             , pv_sender                           olap_sys.mails_sent_history_f.sender%type
                             , pv_recipient                        olap_sys.mails_sent_history_f.recipient%type
                             , pv_subject                          olap_sys.mails_sent_history_f.subject%type
                             , pv_msg                              olap_sys.mails_sent_history_f.msg%type
                             , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type
                             , pn_setup_id                         olap_sys.mails_sent_history_f.setup_id%type
                             , pn_next_drawing_id                  olap_sys.mails_sent_history_f.next_drawing_id%type
                             , pd_drawing_date                     olap_sys.mails_sent_history_f.drawing_date%type  
                             , pv_assigned_to                      olap_sys.mails_sent_history_f.assigned_to%type 
                             , pv_email_send_flag                  olap_sys.mails_sent_history_f.send_flag%type   
                             , x_err_code            in out NOCOPY number
                             , x_err_msg             in out NOCOPY varchar2                            
                              );
							  
  --[ procedure used to update send flag as Y                                 
  procedure upd_mails_sent_history_f(pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type
                                   , pn_setup_id                         olap_sys.w_combinations_picked_f.setup_id%type   
                                   , pv_subject                          olap_sys.mails_sent_history_f.subject%type
                                   , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type
                                   , pv_assigned_to                      olap_sys.mails_sent_history_f.assigned_to%type 
                                   , x_err_code            in out NOCOPY number
                                   , x_err_msg             in out NOCOPY varchar2                            
                                    ); 

  --[ function used to retrieve drawing description
  function get_drawing_desc (pv_drawing_type   olap_sys.w_combinations_picked_f.attribute3%type) return varchar result_cache;
  
  --[ function used to show owner bank account info
  function owner_bank_account_info (pv_drawing_type       olap_sys.w_combinations_picked_f.attribute3%type
                                  , pv_owner_full_name    varchar2
                                   ) return varchar result_cache;  
                                   
  --[ procedure used to update vendor drawing status every time a new vendor drawing is loaded or 
  --[ when a user drawing is released after matching the user drawings                                 
  procedure upd_status_vendor_drawing (pv_drawing_type                olap_sys.w_combination_responses_fs.attribute3%type
                                     , pn_seq_id                      olap_sys.w_combination_responses_fs.seq_id%type default null
                                     , pn_comb1                       olap_sys.w_combination_responses_fs.comb1%type default null
                                     , pn_comb2                       olap_sys.w_combination_responses_fs.comb2%type default null
                                     , pn_comb3                       olap_sys.w_combination_responses_fs.comb3%type default null
                                     , pn_comb4                       olap_sys.w_combination_responses_fs.comb4%type default null
                                     , pn_comb5                       olap_sys.w_combination_responses_fs.comb5%type default null
                                     , pn_comb6                       olap_sys.w_combination_responses_fs.comb6%type default null
                                     , pv_status                      olap_sys.w_combination_responses_fs.status%type default GV$EXCLUDED
                                     , x_err_code       in out NOCOPY number
                                     , x_err_msg        in out NOCOPY varchar2                                                                 
                                      );       
/*                                      
--[ function used to retrieve the range based on comb_sum of a drawing
function get_range_value (p_gambling_type          olap_sys.w_comb_setup_header_fs.attribute3%type
                        , p_comb_sum               number
                         ) return varchar2;
*/ 
--[ procedure used to get the next drawing id and date based on the latest gambling id inserted into table sl_gamblings                         
procedure get_next_drawing_id_date (p_gambling_type                    varchar2
                                  , xn_next_drawing_id   in out NOCOPY olap_sys.w_comb_setup_header_fs.next_drawing_id%type
                                  , xn_next_drawing_date in out NOCOPY olap_sys.w_comb_setup_header_fs.gambling_date%type 
                                  , x_err_code           in out NOCOPY number
                                  , x_err_msg            in out NOCOPY varchar2
                                   );                                                                 

--[ function used to return the median value from a set of comb_sum
function get_median_value (p_numerical_value_tbl  olap_sys.tbl_numerical_value) return number;

--[ function used to return the max value from an array of numbers
function get_max_value_from_tbl (p_numerical_value_tbl  olap_sys.tbl_numerical_value
                               , pn_rownum              number default 1) return number;

--[ function used to sum all digits of a number
function get_digit_sum (pn_number   number) return number;

--[ function used to return an detailed equivalent sum value based on low-low, low-high, high-low and high-high values logic                                       
function convert_sum_low_high_dtl_f (pn_comb_sum          number) return number;

--[ procedure used to load metadata for building dynamic queries
procedure load_plsql_qry_stmt (pv_drawing_type                 olap_sys.c_query_stmts.drawing_type%type
                             , pv_package_name                 olap_sys.c_query_stmts.package_name%type default NULL
                             , pv_procedure_name               olap_sys.c_query_stmts.procedure_name%type default NULL
                             , pv_type                         olap_sys.c_query_stmts.type%type
                             , x_gt$qry_stmt_tbl in out NOCOPY olap_sys.w_common_pkg.gt$qry_stmt_tbl
                             , x_err_code        in out NOCOPY number
                             , x_err_msg         in out NOCOPY varchar2  
                              );

--[ function used to find out if a digit is par or inpar
function get_par_f (pn_number  number) return number;

--[ function used to convert a row into column on a select statement
--function row_to_column_f (p_refcur   olap_sys.w_common_pkg.g_refcur_dgtcnt) return olap_sys.tbl_drawings_digit_counts pipelined;

--[ function use for returning a record from table c_day_range_factors based on input parameters
function get_day_range_factor (pv_drawing_type    olap_sys.c_day_range_factors.drawing_type%type
                             , pv_type            olap_sys.c_day_range_factors.type%type
                             , pn_day_range       number) return number;

procedure get_central_pos_measures (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                          , pn_days_interval            number
	                          , pv_column_name              varchar2
                                  , xn_avg        in out NOCOPY number 
                                  , xn_median     in out NOCOPY number
                                  , xn_count      in out NOCOPY number
                                   );

procedure get_no_central_pos_measures (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                             , pn_days_interval            number
	                             , pv_column_name              varchar2
                                     , xn_mode       in out NOCOPY number 
                                     , xn_rowcount   in out NOCOPY number 
                                      );
                                
procedure get_dispersion_measures (pv_drawing_type              olap_sys.sl_gamblings.gambling_type%type 
	                         , pn_days_interval             number
	                         , pv_column_name               varchar2
                                 , xn_path        in out NOCOPY number 
                                 , xn_max         in out NOCOPY number 
                                 , xn_min         in out NOCOPY number 
                                 , xn_var         in out NOCOPY number 
                                 , xn_stddev      in out NOCOPY number 
                                  );                                

procedure get_shape_measures (pv_drawing_type                     olap_sys.sl_gamblings.gambling_type%type 
	                    , pn_days_interval                    number
	                    , pv_column_name                      varchar2
	                    , pn_avg                              number
                            , pn_count                            number
                            , pn_stddev                           number
                            , xn_custosis           in out NOCOPY number
                            , xv_custosis_desc      in out NOCOPY varchar2 
                            , xn_asymm_coefficient  in out NOCOPY number 
                            , xv_asymm_desc         in out NOCOPY varchar2 
                             );

procedure get_range_base (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                , pn_days_interval            number
	                , pn_rownum                   number default 7
	                , pv_column_name              varchar2
	                 );

function get_range_base_3 (pn_comb_sum         number) return varchar2;

function get_range_base_4 (pn_comb_sum         number) return varchar2;

function get_range_base_5 (pn_comb_sum         number) return varchar2;

--[ function used to get digit count on table sl_gamblings based on digit number
function get_vendor_digit_count (pv_drawing_type                    olap_sys.sl_gamblings.gambling_type%type
                               , pn_days_interval                   number
                               , pv_column_name                     varchar2
                               , pn_digit_value                     number
                               ) return number;                                                

--[ function used to get digit count on table sl_gamblings based on digit number
function get_user_digit_count (pv_drawing_type                    olap_sys.sl_gamblings.gambling_type%type
                             , pv_column_name                     varchar2
                             , pn_digit_value                     number
                             ) return number; 

function sum_digit (pn_digit  number) return number;

--[ function used for retrieving sort_by value from w_combination_responses_fs based on seq_id
function get_user_sort_by (pn_seq_id    number) return number;

--[ get counts from table olap_sys.sl_gamblings based on comb_sum
function get_vendor_comb_sum_cnt (pv_drawing_type          varchar2
                                , pn_comb_sum              number
                                , pv_read_full_table       varchar2 default 'N'
                                , pn_last_n_drawings       number default 20
                                , pn_sum_par_comb          number default null
                                , pn_sum_mod_comb          number default null
                                , pn_sort_by_ini           number default null
                                , pn_sort_by_end           number default null
                                , pv_exclude_drawings_flag varchar2 default 'N'
                                , pv_comb1_list            varchar2 default null
                                , pv_comb2_list            varchar2 default null
                                , pv_comb3_list            varchar2 default null
                                , pv_comb4_list            varchar2 default null
                                , pv_comb5_list            varchar2 default null
                                , pv_comb6_list            varchar2 default null
                                ) return number;
                                   
--[ get counts from table olap_sys.w_combination_responses_fs based on comb_sum                                   
function get_user_comb_sum_cnt (pv_drawing_type    varchar2
                              , pn_comb_sum        number
                              , pn_sum_par_comb    number default null
                              , pn_sum_mod_comb    number default null
                              , pn_sort_by_ini     number default null
                              , pn_sort_by_end     number default null
                              , pv_comb1_list      varchar2 default null
                              , pv_comb2_list      varchar2 default null
                              , pv_comb3_list      varchar2 default null
                              , pv_comb4_list      varchar2 default null
                              , pv_comb5_list      varchar2 default null
                              , pv_comb6_list      varchar2 default null
                                ) return number;

--[ procedure used for inserting computed values into temporal table
procedure ins_computed_value(pn_attribute1        number
                           , pn_attribute2        number default null
                           , pn_attribute3        number default null
                           , pn_attribute4        number default null
                           , pn_attribute5        number default null
                           , pn_attribute6        number default null
                           , pn_attribute7        number default null
                           , pn_attribute8        number default null
                           , pn_attribute9        number default null
                           , pn_attribute10       number default null
                           , pn_attribute11       number default null
                           , pn_attribute12       number default null
                           , pn_attribute13       number default null
                           , pn_attribute14       number default null
                           , pn_attribute15       number default null
                           , pn_attribute16       number default null
                           , pn_attribute17       number default null
                           , pn_attribute18       number default null
                           , pn_attribute19       number default null
                           , pn_attribute20       number default null 
                           , pv_attribute95       varchar2 default null
                           , pv_attribute96       varchar2 default null                                                     
                           , pd_attribute97       date   default null
                           , pd_attribute98       date   default null                           
                           , pv_attribute99       varchar2 default null
                            );

--[ procedure used for inserting computed values into temporal table based on a plsq table 
procedure ins_computed_value(p_tmp_tbl    olap_sys.w_common_pkg.gt$tmp_rec); 

--[ funtion used to retrieve current value from sequence olap_sys.tmp_computed_value_seq in order to save data into table olap_sys.tmp_computed_values
function tmp_computed_value_seq return number;

--[ function used to compute low/high range for sort_by
function get_sort_by_range (pv_drawing_type    varchar2
                          , pn_comb_sum        number
                          , pn_last_n_drawings number default 214
                          , pn_pct             number default 0.01
                          , pn_quarter         number) return varchar2;

--[ function used to compute low/high range for sort_by
function get_sort_by_month (pv_drawing_type    varchar2
                          , pn_comb_sum        number
                          , pn_pct             number default 0.01
                          , pn_mon_ini         number
                          , pn_mon_end         number) return varchar2;

function get_next_drawing_id (p_gambling_type      olap_sys.w_comb_setup_header_fs.attribute3%type
                            , p_setup_id           olap_sys.w_comb_setup_header_fs.setup_id%type) return number;
                                                 

--[ function used to return user global index based on seq_id
function get_usr_global_index (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                             , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number;
                          
--[ function used to return user global index pct based on seq_id
function get_usr_global_index_pct (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                                 , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number;
                                 
--[ function used to return user elegible flag based on seq_id
function get_usr_elegible_flag (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                              , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return varchar2;
                              
--[ function used to return user elegible counter based on seq_id
function get_usr_elegible_cnt (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                             , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number;


--[ function used to randomly retrieve an ID based on a sum_par_comb input parameter
function get_random_usr_mod_comb_filter (pv_drawing_type   olap_sys.c_usr_mod_comb_filters.drawing_type%type
                                       , pn_sum_par_comb   olap_sys.c_usr_mod_comb_filters.sum_par_comb%type
                                        ) return number;                                                                                            

function convert_sum_par_comb (pv_cond_sum_par_comb  varchar2) return number;

--[ procedure to do commit
procedure do_commit(pn_index   number default 0);

--[function used for verifying if the drawing exists on table s_gigamelate_stats
--[will return Y if the drawing is found. Otherwise, will return N
function get_existing_gigamelate (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                , pn_gambling_id                 number
                                , pn_comb1                       number
                                , pn_comb2                       number
                                , pn_comb3                       number
                                , pn_comb4                       number
                                , pn_comb5                       number
                                , pn_comb6                       number) return number;

--[ function used to retrieve total drawings per day based on all active players by gambling day
FUNCTION get_sum_drawings_per_day (pv_drawing_type  	       olap_sys.sl_gamblings.gambling_type%TYPE
                                 , pv_gambling_day             VARCHAR2
                                  ) RETURN NUMBER;

--[ function used to return user m4_comb1..m4_comb6 d on seq_id
function get_usr_m4_comb (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                        , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type
                        , pv_column_name     varchar2) return number;
                             
--[ function used to return user level_comb1..level_comb6 based on seq_id
function get_usr_level_comb (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                           , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type
                           , pv_column_name     varchar2) return varchar2;  
                           
function get_main_range (pn_comb_sum         number) return varchar2;

function get_gigamelate_range (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type) return number;

--[procedure used for sorting in an ascending way all digits tied to a drawing
procedure sort_inbound_comb (x_comb1  in out number
                           , x_comb2  in out number
                           , x_comb3  in out number
                           , x_comb4  in out number
                           , x_comb5  in out number
                           , x_comb6  in out number                           
                            );

--[function used for finding out if a digit is a prime number                            
function is_prime_number (pn_digit           olap_sys.w_combination_responses_fs.comb1%type) return number;                            

function is_valid_module_criteria (pv_gambling_type                  olap_sys.s_preview_gamblings.drawing_type%type
                                 , pn_comb1                          olap_sys.s_preview_gamblings.comb1%type
                                 , pn_comb2                          olap_sys.s_preview_gamblings.comb2%type
                                 , pn_comb3                          olap_sys.s_preview_gamblings.comb3%type
                                 , pn_comb4                          olap_sys.s_preview_gamblings.comb4%type
                                 , pn_comb5                          olap_sys.s_preview_gamblings.comb5%type
                                 , pn_comb6                          olap_sys.s_preview_gamblings.comb6%type
                                 , pn_module_value                   number
                                 , pn_primer_number                  number
                                 ) return varchar2;  
                                   
function is_valid_prime_number_criteria (pv_gambling_type                  olap_sys.s_preview_gamblings.drawing_type%type
                                       , pn_comb1                          olap_sys.s_preview_gamblings.comb1%type
                                       , pn_comb2                          olap_sys.s_preview_gamblings.comb2%type
                                       , pn_comb3                          olap_sys.s_preview_gamblings.comb3%type
                                       , pn_comb4                          olap_sys.s_preview_gamblings.comb4%type
                                       , pn_comb5                          olap_sys.s_preview_gamblings.comb5%type
                                       , pn_comb6                          olap_sys.s_preview_gamblings.comb6%type
                                       , pn_primer_number                  number
                                       ) return varchar2;

--[function used for verifying if the drawing exists on table s_gigamelate_stats
--[will return > 0 if the drawing is found. Otherwise, will return 0
function get_gigaloterias_count (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                               , pn_gambling_id                 number
                               , pn_comb1                       number
                               , pn_comb2                       number
                               , pn_comb3                       number
                               , pn_comb4                       number
                               , pn_comb5                       number
                               , pn_comb6                       number) return number;

--[function used for retrieving last drawing_id to be used as criteria in order to get drawing data                                
function get_last_gigaloterias_id (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                 , pn_gambling_id               number default null
                                 , pn_gambling_range            number default 99) return number;                                

--[function used for retrieving constant values used for computing average patterns
function get_avg_pattern_constant (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                 , pv_column_name               varchar2) return number;                                                                                                                                                           

--[function used for getting count for digit that are above corresponding digit average
function get_average_pattern_count (pv_gambling_type  	     olap_sys.sl_gamblings.gambling_type%type default 'mrtr'
                                  , pn_comb1                 number default null
                                  , pn_comb2                 number default null
                                  , pn_comb3                 number default null
                                  , pn_comb4                 number default null
                                  , pn_comb5                 number default null
                                  , pn_comb6                 number default null
                                  , pv_type                  varchar2 default 'AVG'
                                  ) return number;

--[function used for returning average on global index based on type
function get_avg_global_index (pv_type   varchar2) return number;

--[function used for returning average on comb sum based on type
function get_avg_comb_sum (pv_type   varchar2) return number;

--[ function used for returning master count from table olap_sys.c_usr_dtl_sum_par_comb_count   
/*function get_usr_dtl_sum_par_comb_cnt (pv_drawing_type    olap_sys.c_usr_dtl_sum_par_comb_count.drawing_type%type                                     
                                     , pn_sum_par_comb    olap_sys.c_usr_dtl_sum_par_comb_count.sum_par_comb%type default -1
                                     , pn_comb_sum        olap_sys.c_usr_dtl_sum_par_comb_count.comb_sum%type
                                     , pv_column          varchar2 default 'XROWCOUNT'
                                      ) return number;*/

--[ function used for returning master count from table olap_sys.c_usr_drawings_master_count   
function get_usr_drawings_master_cnt (pv_drawing_type    olap_sys.c_usr_drawings_master_count.drawing_type%type) return number;
                                      
--[procedure used for updating winner flag on table olap_sys.s_calculo_stats
procedure upd_calculo_stats_winner_flag (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                       , pn_gambling_id                 number
                                        );
                                        
--[function used for counting winner drawings allocated on table s_gl_abril_combinations
function count_win_drawings (pv_match_history varchar2) return number;

--[function used for identifying termination numbers in a whole combination
function find_terminations (pn_comn1  number
                          , pn_comn2  number
                          , pn_comn3  number
                          , pn_comn4  number
                          , pn_comn5  number
                          , pn_comn6  number
                           ) return number;   
                           
function show_prime_number(pn_digit  number) return number;       

function show_multiple_number(pn_digit   number
                            , pn_base    number) return number;   

function hide_prime_number(pn_digit  number) return number;           

function compute_multiplos(pn_comn1  number
                         , pn_comn2  number
                         , pn_comn3  number
                         , pn_comn4  number
                         , pn_comn5  number
                         , pn_comn6  number) return varchar2;
    
function get_dozen_sort  (p_d1   varchar2
                        , p_d2   varchar2
                        , p_d3   varchar2
                        , p_d4   varchar2
                        , p_d5   varchar2
                        , p_d6   varchar2
                         ) return number;   

function get_dozen_rank  (p_d1   varchar2
                        , p_d2   varchar2
                        , p_d3   varchar2
                        , p_d4   varchar2
                        , p_d5   varchar2
                        , p_d6   varchar2
                         ) return varchar2;   

function descomponer_numero(pn_comb  number) return number;

function is_real_none (pn_digit  number) return number;  

function is_real_par (pn_digit  number) return number;

function compute_ciclos (pv_lista_estadistica   varchar2) return number;   

function compute_avg_ciclos (pv_lista_ciclos   varchar2) return number;   

--!cuenta el numeros de sorteos incluidos en la lista
function count_drawings_in_list (pv_lista_estadistica   varchar2) return number;

--!regresa el ultimo numero de sorteo de la lista
function get_last_drawing_from_list (pv_lista_estadistica   varchar2) return number;

function compute_composition_by_tens (pv_ten_list   varchar2) return varchar2;
 
function count_multiple_terminations (pn_comn1  number
                                    , pn_comn2  number
                                    , pn_comn3  number
                                    , pn_comn4  number
                                    , pn_comn5  number
                                    , pn_comn6  number
									, pn_base_number number default 2
									, pv_exe_flag varchar2 default 'TERM_ONLY'
									) return number;
									
--!convertir un string separado por comas en renglones de un query
procedure translate_string_to_rows (pv_string              VARCHAR2
                                  , xtbl_row  IN OUT NOCOPY dbms_sql.varchar2_table
								  , x_err_code IN OUT NOCOPY NUMBER
								   );

--!recuperar los valores distinctos de un string separado por comas
procedure get_distinct_values_from_list (pv_string                             VARCHAR2
                                       , pv_data_type                          VARCHAR2 DEFAULT 'STRING'
									   , pv_data_sort						   VARCHAR2 DEFAULT 'DESC'
									   , xv_distinct_value_list  IN OUT NOCOPY VARCHAR2
									    );

--!construye el select list final
procedure get_final_select_list (pv_string                             VARCHAR2
							   , xv_select_list          IN OUT NOCOPY VARCHAR2
							   , x_err_code              IN OUT NOCOPY NUMBER
							    );

--!recupera el peso de una jugada en base a metadatos
function get_drawing_weight (pv_string varchar2) return number;

--!recupera el ultimo elemento de una lista separada por |
function get_last_list_item (pv_string varchar2) return number;


--!recupera un elemnto especifico de una lista separada por comas
function get_n_list_item (pv_string 	varchar2
						, pn_position	number) return number;

--!recupera la longitud de la columna de una tabla
function get_column_length (pv_owner 		varchar2 default 'OLAP_SYS'
						  , pv_table_name	varchar2
						  , pv_column_name	varchar2) return number;

--!procedimiento para extraer el valor de cada posicion de config_ppn_description con este formato PR-%-%-PR-%-%
procedure read_config_primos_string (pv_config_ppn_description VARCHAR2
								   , xv_pos1	IN OUT NOCOPY VARCHAR2
								   , xv_pos2	IN OUT NOCOPY VARCHAR2
								   , xv_pos3	IN OUT NOCOPY VARCHAR2
								   , xv_pos4	IN OUT NOCOPY VARCHAR2
								   , xv_pos5	IN OUT NOCOPY VARCHAR2
								   , xv_pos6	IN OUT NOCOPY VARCHAR2);

--!contador de terminaciones en base a un numero base
function termination_counter (pv_termination_list	VARCHAR2
							, pn_base_number		NUMBER DEFAULT 2) return number;

--!funcion para identificar si se usuara una nueva secuencia para un nuevo registro
function is_new_gl_mapa_seq (pv_gl_type		varchar2
						   , pn_xrownum		number
						   , pn_seq_no		number
						   , pn_drawing_id	number) return number;

--!funcion para recuperar el master_id de la tabla olap_sys.s_gl_mapas
function get_gl_mapa_master_id (pn_xrownum		number
						      , pn_seq_no		number
						      , pn_drawing_id	number
							  , pv_gl_type		varchar2 DEFAULT 'LT') return number;							  

--!funcion para averiguar si una jugada tiene terminaciones repetidas
function is_jugada_term (pn_seq_id      number
                       , pn_digito      number) return number;
	
--!insertar registros de PM Panorama en tabla temporal para usarlo en la validacion de las jugadas
procedure ins_pm_panorama(pv_str       varchar2);

--!filtrar las parejas de numeros primos dependiendo del operador
function pm_filtrar_pareja_primos(pn_comb1  number
							    , pn_comb2  number
							    , pn_comb3  number
							    , pn_comb4  number
							    , pn_comb5  number
							    , pn_comb6  number
							    , pv_operador varchar2 default '>'
								, pv_dis_filtrar_pareja_primos varchar2 default 'N') return varchar2;

--!cuentador de las ocurrencias de los digitos siguientes a digito indicado
procedure cuenta_sig_digito(pn_digito		number
						  , pn_posicion     number
						  , pn_anio_ini     number default 2010);

--!insertar los numeros extremos para usarlos despues como filtro para generar las jugadas
procedure ins_numeros_extremos_handler;

--!filtrando jugadas en base a los numeros extremos
function pm_filtrar_extremos(pn_drawing_case number
                           , pn_comb1        number	
                           , pn_comb6        number
						   , pv_dis_filtrar_extremos varchar2) return varchar2;	

--!filtrando jugadas en base a rangos de ciclos de aparicion
function gl_filtrar_ca_handler(pn_drawing_case  	number
                             , pn_ca1				number
						     , pn_ca2				number 
						     , pn_ca3				number
						     , pn_ca4				number
						     , pn_ca5				number
						     , pn_ca6				number
						     , pn_sum_ca			number
							 --!bandera para habilitar que se validen los ca por posicion
							 , pv_look_ca_pos       	varchar2 default 'Y'
							 --!contador que indica hasta cuantos ca no pueden hacer match
							 , pn_ca_no_match_cnt   number default 1
							 ) return varchar2;	

--!recuperar un patron numerico en base al parametro de entrada y a la posicion de los digitos
function get_string_pattern(pn_comb1  number
						  , pn_comb2  number
						  , pn_comb3  number
						  , pn_comb4  number
						  , pn_comb5  number
						  , pn_comb6  number
						  --!PR=Primo, I=Inpar, P=Par 
						  , pv_type varchar2 default 'PR') return varchar2;	
						  					  
						  
--!filtrar jugadas en base a posiciones de numeros primos						  
function pm_filtrar_primos_por_posicion(pn_drawing_case  	number
								      , pn_comb1  			number
									  , pn_comb2  			number
									  , pn_comb3  			number
									  , pn_comb4  			number
									  , pn_comb5  			number
									  , pn_comb6  			number
									  , pv_filtrar_primos_por_posicion varchar2 default 'N'
								      , pn_percentile		number default 0.25
									  , pn_primos_cnt		number default 2) return varchar2;						  

					 
--!validar que el ciclo de aparicion de cada posicion se encuentre dentro del rango de la tabla 					 
function is_ciclo_aparicion_valido(pn_drawing_case  	number
							     , pn_ca1				number
								 , pn_ca2				number 
								 , pn_ca3				number
								 , pn_ca4				number
								 , pn_ca5				number
								 , pn_ca6				number
								 , pn_ca_no_match_cnt  	number) return boolean;	

--!valida si el numero de ocurrencias de numeros favorables	de 1 a 4 esta en la configuracion de la tabla					  
function gl_is_nf_config_valid(pn_drawing_case		number
						     , pn_sum_nf			number
							 , pn_pre_sum_nf		number
						     , pv_pre1  			varchar2
						     , pv_pre2  			varchar2
						     , pv_pre3  			varchar2
						     , pv_pre4  			varchar2
						     , pv_pre5  			varchar2
						     , pv_pre6  			varchar2
							 , pv_dis_nf_config_valid varchar2) return varchar2; 

--!recupera el grupo establecido en la tabla plan_jugada_details.
--!funcion usada por la vista pm_mr_resultados_v2					  
function gl_get_nf_group(pv_pre1  			varchar2
					   , pv_pre2  			varchar2
					   , pv_pre3  			varchar2
					   , pv_pre4  			varchar2
					   , pv_pre5  			varchar2
					   , pv_pre6  			varchar2) return number; 	

--!insertar en la tabla w_lookups_fs los pares de numeros primos a jugar
procedure ins_numeros_primos_handler;


--!insertar en la tabla plan_jugada_ca_stats las etadisticas de los ciclos de aparicion
procedure ins_ca_stats_handler;

--!validacion del conteo de inpares y pares por decena
function valida_conteo_inpar_par(pn_drawing_case	number
							   , pn_none_cnt		number
							   , pn_par_cnt			number
							   , pv_val_inpar_par   varchar2 default 'N') return varchar2;	

--!validacion de terminaciones por decena
function valida_conteo_terminaciones(pn_drawing_case		number
								   , pn_term1_cnt			number
								   , pn_term2_cnt			number
								   , pv_terminacion_doble   varchar2
								   , pv_val_terminaciones   varchar2 default 'N') return varchar2;	

--!devuelve el conteo de los resultados en base a b1, b4, b6 y decena
function get_conteo_b1_b4_b6(pn_drawing_case	number
						   , pn_comb1  			number
						   , pn_comb4  			number
						   , pn_comb6  			number) return number;

--!regresa el conteo de las terminaciones en funcion de los resultados
function get_conteo_favorables(pn_drawing_case		number
						     , pv_pre1  			varchar2
						     , pv_pre2  			varchar2
						     , pv_pre3  			varchar2
						     , pv_pre4  			varchar2
						     , pv_pre5  			varchar2
						     , pv_pre6  			varchar2) return number;

--!validar que el ca_sum y comb_sum esten dentro del rango calculado
function valida_ca_sum_comb_sum(pn_comb1  		number
							   , pn_comb2  		number
							   , pn_comb3  		number
							   , pn_comb4  		number
							   , pn_comb5  		number
							   , pn_comb6  		number
							   , pv_val_type	varchar2 default 'ALL') return varchar2;

--!validar que el ca_sum y comb_sum en base a las resultados ganadores de 5 y 6 aciertos
function valida_ca_sum_comb_sum(pn_comb_sum  		number
							  , pn_ca_sum 		number
							  , pv_val_type	varchar2 default 'ALL') return varchar2;

--!regresa si el numero es numero primo, inpar o par
function get_position_type(pn_comb      number) return varchar2;

--!regresa el ID del drawing_case
function get_plan_jugada_id(pn_drawing_case		number)	return number;	

--!regresa la bandera del mapa de numeros primos guardado en la tabla pm_mapa_numeros_primos 					  
function get_mapa_numeros_primos(pn_drawing_id	number
							   , pn_comb1  		number
							   , pn_comb2  		number
							   , pn_comb3  		number
							   , pn_comb4  		number
							   , pn_comb5  		number
							   , pn_comb6  		number)	return varchar2;				  

--!cuenta las igualdades de valores entre dos cadenas de valores separadas por comas
function contar_igualdades (pv_string1		varchar2
						  , pv_string2		varchar2) return number;

--!comparar loa valores de los 2 parametros de entrada y regresa un valor equivalente para cada caso
--!dependiendo de id del sorteo
function get_favorito(pn_drawing_id		number
				    , pn_pxc				number
				    , pv_preferido		varchar2) return number;	

--!convertir el string de la decena a numero
function get_decena_to_numero(pv_decena		varchar2) return number;

--!convertir numero a string de decena 
function get_numero_to_decena(pv_numero		varchar2) return varchar2;

--!convertir el digito a numero one-hot encoded
function get_digito_to_numero(pn_numero		number) return number;

--!transformar los valores que vienen de la predicciones
function transformar_valor_posicion(pv_tipo		varchar2
								  , pv_pred		varchar2) return varchar2;
								  
--!convertir digito a string de decena 
function get_digito_to_decena(pn_numero		number) return varchar2;
								  
--!regresar el rank de la combinacion de C1, C5 y C6								  
function get_c1_c5_c6_rank(pn_comb1     number
                         , pn_comb5     number
                         , pn_comb6     number) return number ; 	

--!regresar el tipo de numero. 1:primo, 2:par, 3:impar
function get_digit_type(pn_comb		number) return number;	
					 
end w_common_pkg;
/
show errors;