create or replace package olap_sys.w_new_pick_panorama_pkg as

GV$PACKAGE_NAME         constant varchar2(100) := 'W_NEW_PICK_PANORAMA_PKG';
GV$CONTEXT              constant varchar2(50)  := 'PICK_PANORAMA';
GB$SHOW_PROC_NAME                boolean       := FALSE;    

--!constantes de la validaciones que se aplican a las jugadas
CV$VAL_DISTANCIA_EXTREMOS	    constant varchar2(30) := 'VAL_DIST_EXTREMOS';
CV$VAL_SUMA_DIGITOS             constant varchar2(30) := 'VAL_COMB_SUM';
CV$VAL_DIFERENCIA_TIPO          constant varchar2(30) := 'VAL_DIF_TIPO';
CV$VAL_POSICION_PRIMOS          constant varchar2(30) := 'VAL_POS_PRIMOS';
CV$VAL_SUMA_CICLO_APARICION     constant varchar2(30) := 'VAL_GL_CA_SUM';
CV$VAL_POSICION_SIN_CAMBIO      constant varchar2(30) := 'VAL_POS_SIN_CAMBIO';
CV$VAL_GL_LEY_TERCIO     		constant varchar2(30) := 'VAL_GL_LEY_TERCIO';

--!percentile usado para los calculos usados en las validaciones
CN$YEAR_END						constant number(4) := 2022;
CN$YEARS_BACK					constant number(2) := 10;
CN$PRIMO_CNT					constant number(1) := 2;
CF$PERCENTILE					constant float := 0.75;
CN$MIN_RCNT_DIF					constant number(1) := 2;
CN$NUMEROS_PARES_INPARES 		constant number(1) := 0;
CN$NUMEROS_PRIMOS		 		constant number(1) := 1;

  --!valores constantes del proceso
   CV$NUMERO_PRIMO         constant varchar2(3) := 'PR';
   CV$NUMERO_PAR           constant varchar2(3) := 'PAR';
   CV$NUMERO_NON           constant varchar2(3) := 'NON';  
   CV$NUMERO_COMODIN       constant varchar2(3) := '%';
   CV$SIN_VALOR            constant varchar2(5) := '1=1';   
   CN$MULTIPLO_3           constant number(1) := 3;
   CN$MULTIPLO_4           constant number(1) := 4;
   CN$MULTIPLO_5           constant number(1) := 5;
   CN$MULTIPLO_7           constant number(1) := 7;
   CV$ENABLE               constant varchar2(1) := 'Y';  
   CV$DISABLE              constant varchar2(1) := 'N';  
   CV$PRINT_ONLY           constant varchar2(1) := 'P';
   CN$DECENAS_TODAS		   constant number(1) := 0;	
   CV$PANORAMA			   constant varchar2(1) := 'P';	
   CV$MAPAS_LT			   constant varchar2(1) := 'M';	   
   CN$MIN_GL_DRAWING_ID	   constant number := 594;
   CN$DOS_NUMEROS_PRIMOS   constant number := 2;
   CN$HOLGURA_GL_CA        constant number := 2;
   CV$GL_NULL			   constant varchar2(1) := 'X';	   
   CV$STATUS_ACTIVO        constant varchar2(1) := 'A';
   CV$STATUS_ERROR         constant varchar2(1) := 'E';
   CV$PATRON_TRES          constant varchar2(1) := '3';   
   CN$DIFERENCIA		   constant number(3) := 99;
   CN$BASE_DRAWING_ID      constant number(3) := 595;
   CV$POSICION_SIN_CAMBIO  constant varchar2(10) := 'SIN CAMBIO';			
   
--!tabla para almacenar resultados de info recuperados por query basados en gl
type gt$gl_rec is record (drawing_id    number
                        , b_type        varchar2(2)
						, digit         number
						, frec          varchar2(1)
						, lt            varchar2(1) 
					    , rlt           varchar2(1) 
                        , ca			number
                        , pxc			number
                        , pr			number
						, non			number
                        , preferencia_flag varchar2(1)  
                        , change_flag    varchar2(1)  
                        , numero_primo_flag number						
						 );

type gt$gl_tbl is table of gt$gl_rec index by binary_integer;    

--!tabla para almacenar resultados de info recuperados por query basados en panorama
type gt$panorama_rec is record (pos1    number
							  , pos2    number
							  , pos3    number
							  , pos4    number
							  , pos5    number
							  , pos6	number
							  , panorama_seq_no  number
							   );

type gt$panorama_tbl is table of gt$panorama_rec index by binary_integer; 

--!tabla para almacenar los datos guardados en la tabla plan de jugadas
type gt$plan_rec is record (pos1    varchar2(200)
						  , pos2    varchar2(200)
						  , pos3    varchar2(200)
						  , pos4    varchar2(200)
						  , pos5    varchar2(200)
						  , pos6	varchar2(200)
						   );

type gt$plan_tbl is table of gt$plan_rec index by binary_integer; 					   

--!tabla para almacenar los numeros primos para el plan de jugadas
type gt$np_rec is record (numero_primo1    varchar2(2)
						, numero_primo2    varchar2(2)
						 );

type gt$np_tbl is table of gt$np_rec index by binary_integer; 						   

--!tabla para almacenar los digitos y su posicion del ultimo sorteo 
type gt$posicion_rec is record (digito		number(2)
							  , posicion    varchar2(100)
						   );

type gt$posicion_tbl is table of gt$posicion_rec index by binary_integer; 


--!tabla para almacenar el conteo de los lt b_types para un sorteo dado
type gt$lt_rec is record (gl_type		  varchar2(2)
						, drawing_id	  number
						, b_type		  varchar2(5)	
                        , attribute       varchar2(20)
					    , value		  	  varchar2(20)
						 );

type gt$lt_tbl is table of gt$lt_rec index by binary_integer; 


--!tabla para calcular el procentaje acumulado de los pares de numeros primos
type gt$primo_ptg_rec is record (drawing_case    	number
							  , primo_pos1    		number
							  , primo_pos2    		number
							  , pn_cnt    			number
							  , percentage    		number
							  , percentage_accum	number
							  , row_id				varchar2(50)
							   );

type gt$primo_ptg_tbl is table of gt$primo_ptg_rec index by binary_integer;


--!tabla para almacenar los resultados de frec/lt
type gt$fre_lt_rec is record (drawing_id    number
						    , gl1    		varchar2(1)
						    , gl2    		varchar2(1)
						    , gl3    		varchar2(1)
						    , gl4    		varchar2(1)
						    , gl5    		varchar2(1)
							, gl6    		varchar2(1)						
						     );

type gt$fre_lt_tbl is table of gt$fre_lt_rec index by binary_integer;

--!tabla para almacenar los counts de frec/lt
type gt$fre_lt_cnt_rec is record (drawing_id    number
						    , gl1    		varchar2(1)
						    , gl2    		varchar2(1)
						    , gl3    		varchar2(1)
						    , gl4    		varchar2(1)
						    , gl5    		varchar2(1)
							, gl6    		varchar2(1)	
						    , wf1    		varchar2(1)
						    , wf2    		varchar2(1)
						    , wf3    		varchar2(1)
						    , wf4    		varchar2(1)
						    , wf5    		varchar2(1)
							, wf6    		varchar2(1)								
						     );

type gt$fre_lt_cnt_tbl is table of gt$fre_lt_cnt_rec index by binary_integer;

--!tabla para almacenar el conteo de frec/lt
type gt$gl_cnt_rec is record (gl    	varchar2(1)
							, cnt    	number
							, flag      varchar2(1) 
							 );

type gt$gl_cnt_tbl is table of gt$gl_cnt_rec index by binary_integer;


--!tabla para almacenar los counts de frec/lt
type gt$history_cnt_rec is record (drawing_id    number
								, pos1    		number(1)
								, pos2    		number(1)
								, pos3    		number(1)
								, pos4    		number(1)
								, pos5    		number(1)
								, pos6    		number(1));

type gt$history_cnt_tbl is table of gt$history_cnt_rec index by binary_integer;								

--!tabla para almacenar los counts de frec/lt
type gt$color_rec is record (color_ultimo    		number(1)
						   , color_previo    		number(1));

type gt$color_tbl is table of gt$color_rec index by binary_integer;	


--!tabla para almacenar el drawing_case y su prioridad
type gt$jugada_rec is record (jugada_prioridad    		number(1)
						    , drawing_case    		    number(1));

type gt$jugada_tbl is table of gt$jugada_rec index by binary_integer;


--!tabla para almacenar el drawing_case y su prioridad
type gt$jugada_tipos_rec is record (consecutivo    		varchar2(1)
								  , terminacion_doble	varchar2(1));

type gt$jugada_tipos_tbl is table of gt$jugada_tipos_rec index by binary_integer;	


--!tabla para almacenar el drawing_case y su prioridad
type gt$plan_jugada_ca_rec is record (drawing_case    		number(1)
								    , b_type	            varchar2(2)
									, ca_ini				number(2)
									, ca_end				number(2)									
									);

type gt$plan_jugada_ca_tbl is table of gt$plan_jugada_ca_rec index by binary_integer;	

--!tabla para almacenar los resultados de los sorteos
type gt$resultado_rec is record (id           		number
								, pos1    			number(2)
								, pos2    			number(2)
								, pos3    			number(2)
								, pos4    			number(2)
								, pos5    			number(2)
								, pos6    			number(2)
								, flag          	varchar2(1));

type gt$resultado_tbl is table of gt$resultado_rec index by binary_integer;	


--!tabla para almacenar los resultados de los sorteos
type gt$resultado_next_rec is record (pos    			number(2)
                                    , match_cnt         number
							        , history_ids   	varchar2(1000)
							        , history_duration  varchar2(1000));

type gt$resultado_next_tbl is table of gt$resultado_next_rec index by binary_integer;	

--!cursor para obtener el ID de cada decena
cursor c_decenas (pv_drawing_type           VARCHAR2 DEFAULT 'mrtr'
				, pn_drawing_case			 NUMBER) is
SELECT ID
  FROM OLAP_SYS.PLAN_JUGADAS
 WHERE DRAWING_TYPE = pv_drawing_type
   AND DESCRIPTION  = 'DECENAS'
   AND STATUS       = 'A'
   AND DRAWING_CASE = pn_drawing_case; 	

	
/*
--!proceso principal para generar jugadas para los sorteos
procedure main_handler(pv_drawing_type             VARCHAR2
					 , pn_drawing_case             NUMBER				
					 --!indica si de agrega el filtro en base a frecuencia. Valor valido Y
					 , pv_frec_enable         		VARCHAR2 DEFAULT 'Y'
					 --!indica si de agrega el filtro en base a lt. Valor valido Y
					 , pv_lt_enable    				VARCHAR2 DEFAULT 'Y'				 
					 --!indica si de agrega el filtro en base a ca. Valor valido Y
					 , pv_ca_enable                VARCHAR2 DEFAULT 'Y'
					 --!cuando haya una condicion invalida en el panorama query con esta opcion
					 --!habilitada se sustituira la condicion por una condicion por default. Valor valido Y
					 , pv_final_list_enable        VARCHAR2 DEFAULT 'N'	
                     --!habilita el filtro de terminaciones
                     , pv_terminaciones_enable 	   VARCHAR2 DEFAULT 'Y'					 
					 --!para los patrones de numeros se agregan a la lista final de numeros o la reemplazan
					 , pv_replace_add_flag         VARCHAR2 DEFAULT 'R'
			         --!habilita el filtro de cambios en GL
					 , pv_cambios_gl_enable       VARCHAR2 DEFAULT 'Y'
					 --!indica si se guarda el query en la tabla
					 , pv_save_qry_enable			  VARCHAR2 DEFAULT 'N'
					--!indica si se generaran las combinaciones basadas en el panorama
					 , pv_panorama_enable			  VARCHAR2 DEFAULT 'Y'					 
  				     , x_err_code    IN OUT NOCOPY NUMBER
                      );
*/					  
						
--!proceso principal para validar la reglas de juego en la tabla						
procedure table_validator_handler (pn_case                     NUMBER
                                 , pv_description              VARCHAR2
								 , pv_pos1					   VARCHAR2
								 , pv_pos2					   VARCHAR2
								 , pv_pos3					   VARCHAR2
								 , pv_pos4					   VARCHAR2
								 , pv_pos5					   VARCHAR2
								 , pv_pos6					   VARCHAR2
								 , x_err_code    IN OUT NOCOPY NUMBER
								  );						

/*
--!proceso principal para obtener los digitos iniciales a ser usados en jugadas para los sorteos
procedure initial_digits_handler(pv_drawing_type              VARCHAR2
						       , pn_drawing_case              NUMBER
							   --!indica si de agrega el filtro en base a lt. Valor valido Y
							   , pv_lt_enable                 VARCHAR2 DEFAULT 'Y'
							   --!indica si de agrega el filtro en base a lt. Valor valido Y
							   , pv_ca_enable                 VARCHAR2 DEFAULT 'Y'
							   --!indica si de agrega el filtro en base conf de numero primo, par y non. Valor valido Y
							   , pv_conf_ppn_enable           VARCHAR2 DEFAULT 'Y'
							   --!habilita el filtro de cambios en GL
							   , pv_cambios_gl_enable       VARCHAR2 DEFAULT 'Y'
							   --!indica si se guarda el query en la tabla
							   , pv_save_qry_enable			  VARCHAR2 DEFAULT 'N' 
  							   --!indica si de agrega el filtro en base a frecuencia. Valor valido Y
							   , pv_frec_enable         		VARCHAR2 DEFAULT 'Y'	
						       , x_err_code    IN OUT NOCOPY NUMBER
						        );
*/
--!recuperar el ID ultimo sorteo
function get_max_drawing_id (pv_drawing_type             VARCHAR2 DEFAULT 'mrtr') return number;

--!proceso principal para obtener los digitos iniciales a ser usados en jugadas para los sorteos
procedure main_posicion_IN_handler(pv_drawing_type              VARCHAR2 DEFAULT 'mrtr'
						         , pn_drawing_case              NUMBER
								 --!P:Panorama, M:Mapas LT
								 , pv_execution_type            VARCHAR2 DEFAULT 'P'  
						         , pv_show_init_values          VARCHAR2 DEFAULT 'Y'
								 --! 0:todos, 1:bajos, 2:altos
								 , pn_diferencia_tipo			NUMBER DEFAULT 2
								 , pv_add_primo_enable			VARCHAR2 DEFAULT 'N'
								 , pn_lt_red_cnt				NUMBER DEFAULT 2
								 --!validacion comb_sum
								 , pv_val_sum_enable  		    VARCHAR2 DEFAULT 'N'
								 --!validacion gl ca
								 , pv_val_ca_enable  		    VARCHAR2 DEFAULT 'N'
								 --!validacion numeros primos
								 , pv_val_primos_enable  		VARCHAR2 DEFAULT 'N'
								 --!validacion extremos
								 , pv_val_extremos_enable  		VARCHAR2 DEFAULT 'N'
								 --!contador de numeros consecutivos
								 , pn_consecutivos_cnt  		NUMBER DEFAULT 0	
								 --!validacion de posiciones sin cambio
                                 , pv_val_pos_sin_cambio_enable VARCHAR2 DEFAULT 'N'								 
								 --!indicador de terminaciones repetidas en la jugadas. Default 2
								 , pn_term_cnt					NUMBER DEFAULT 2
								 --!bandera de numeros favorables
								 , pv_favorable_flag_1			VARCHAR2 DEFAULT NULL
								 , pv_favorable_flag_2			VARCHAR2 DEFAULT NULL
								 , pv_favorable_flag_3			VARCHAR2 DEFAULT NULL
								 , pv_favorable_flag_4			VARCHAR2 DEFAULT NULL
								 , pv_favorable_flag_5			VARCHAR2 DEFAULT NULL
								 , pv_favorable_flag_6			VARCHAR2 DEFAULT NULL
								 --!bandera de posiciones sin cambio
								 , pv_change_flag_1				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO
								 , pv_change_flag_2				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO
								 , pv_change_flag_3				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO
								 , pv_change_flag_4				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO
								 , pv_change_flag_5				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO
								 , pv_change_flag_6				VARCHAR2 DEFAULT CV$POSICION_SIN_CAMBIO		
								 --!bandera de pronostico por ciclo
								 , pv_pxc_flag_1				VARCHAR2 DEFAULT CV$DISABLE
								 , pv_pxc_flag_2				VARCHAR2 DEFAULT CV$DISABLE
								 , pv_pxc_flag_3				VARCHAR2 DEFAULT CV$DISABLE
								 , pv_pxc_flag_4				VARCHAR2 DEFAULT CV$DISABLE
								 , pv_pxc_flag_5				VARCHAR2 DEFAULT CV$DISABLE
								 , pv_pxc_flag_6				VARCHAR2 DEFAULT CV$DISABLE										 
								 , x_err_code     IN OUT NOCOPY NUMBER								 
								  );

--!proceso principal para configurar el patron del ultimo sorteo
procedure main_ultimo_sorteo_handler(pv_drawing_type              VARCHAR2 DEFAULT 'mrtr'
						           , pn_drawing_case              NUMBER
								   , pv_add_enable           	  VARCHAR2 DEFAULT 'Y'  
						           , pv_remove_enable          	  VARCHAR2 DEFAULT 'N'  
								   , x_err_code     IN OUT NOCOPY NUMBER								 
								    );


--!proceso principal para copiar metadatos entre cases ID en base a un case ID
--procedure copy_metadata_handler(pn_drawing_case              NUMBER DEFAULT 1);	

--!proceso principal para calcular los ciclos de aparicion de una decena
procedure main_decena_ciclos_handler (pn_year					   NUMBER
									, pn_primos_cnt		 		   NUMBER DEFAULT 2
									, x_err_code     IN OUT NOCOPY NUMBER);


--!proceso principal para limpiar las configuraciones del plan de jugadas
procedure limpiar_plan_jugadas_handler (pn_drawing_case              NUMBER DEFAULT 0);
			  
--!!proceso para actualizar tabla plan_jugadas J_DECENA_PRIMO_CNT en base las jugadas activas en la tabla W_COMBINATION_RESPONSES_FS
procedure plan_jugadas_trg_handler (pv_drawing_type			       VARCHAR2 DEFAULT 'mrtr'
							      , pn_drawing_case                NUMBER 
								  , pn_id					       NUMBER
							      , x_err_code       IN OUT NOCOPY NUMBER);	

--!proceso para desactivar/activar CONFIG_PRIMOS_PARES_NONES en base a las decenas
procedure plan_jugadas_conf_pr_handler (pv_drawing_type			           VARCHAR2 DEFAULT 'mrtr'
									  , pn_drawing_case                    NUMBER 
									  , pv_config_primos_list			   VARCHAR2
									  , pv_just_disable					   VARCHAR2 DEFAULT 'N'	
									  , x_err_code           IN OUT NOCOPY NUMBER);	


--!proceso para validar que la config LEY_TERCIO_IN sea encontrada como valida en tabla S_GL_LEY_TERCIO_PATTERNS
procedure valida_ley_tercio_in_handler (pv_drawing_type		           VARCHAR2
									  , pn_drawing_case                NUMBER 
									  , pv_description				   VARCHAR2
									  , pv_pos1						   VARCHAR2
									  , pv_pos2						   VARCHAR2
									  , pv_pos3						   VARCHAR2
									  , pv_pos4						   VARCHAR2
									  , pv_pos5						   VARCHAR2
									  , pv_pos6						   VARCHAR2									  
									  , xn_record_cnt	 IN OUT NOCOPY NUMBER	
									  , x_err_code       IN OUT NOCOPY NUMBER);	

--!proceso para realizar conteos de LT y FR de los ultimoas 100 jugadas en base a la ultima jugadas
procedure get_frec_lt_count_handler(pv_gl_type                     VARCHAR2 DEFAULT 'LT'
								  , pn_rownum					   NUMBER
								  , pn_drawing_id				   NUMBER DEFAULT NULL
								  , pv_auto_commit				   VARCHAR2 DEFAULT 'N'
								  , pv_get_resultado			   VARCHAR2 DEFAULT 'Y'	
								  , pv_insert_pattern			   VARCHAR2 DEFAULT 'Y'
								  , pv_resultado_type			   VARCHAR2 DEFAULT 'PREV'
								  , x_err_code       IN OUT NOCOPY NUMBER);


--!proceso para generar conteos de lt types para los dos ultimos sorteos
procedure generar_lt_counts_handler (pv_drawing_type			  VARCHAR2 DEFAULT 'mrtr'
								   , pn_drawing_id				  NUMBER
								   , pv_gl_type					  VARCHAR2 DEFAULT 'LT'
								   , pv_resultado_type			   VARCHAR2 DEFAULT 'PREV'
								   , pv_insert_allowed_flag		  VARCHAR2 DEFAULT 'Y'
								   , x_err_code     IN OUT NOCOPY NUMBER);						   


--!proceso general para realizar conteos de LT y FR de los ultimoas 100 jugadas en base a la ultima jugadas
procedure get_frec_lt_count_wrapper(pn_drawing_id				   NUMBER DEFAULT NULL
								  , pv_auto_commit				   VARCHAR2 DEFAULT 'N'	
								  , pv_insert_pattern			   VARCHAR2 DEFAULT 'Y'	
								  , x_err_code       IN OUT NOCOPY NUMBER	
									);

--!proceso para buscar patrones en el historico de mapas de conteo
procedure search_pattern_handler(pv_drawing_type			    VARCHAR2 DEFAULT 'mrtr'
							   , pn_drawing_id				    NUMBER DEFAULT 0
							   , pv_gl_type					    VARCHAR2 DEFAULT 'LT'
							   , pn_rownum						NUMBER DEFAULT 2
							   , pv_full_scan					VARCHAR2 DEFAULT 'N'
							   , x_err_code       IN OUT NOCOPY NUMBER	
								);

--!proceso para validar la repeticion de los patrones de la ley del tercio
procedure validate_lt_pattern_history;	
							
							
--!procedimiento para contar numeros inpares, pares y primos en base al anio
procedure par_inpar_primo_cnt_handler (pn_year_end		number default 2022
									 , pn_years_back	number default 5
									 , pn_primo_cnt		number default 2
									 , pn_is_primo		number default 0
									 , pn_percentile    number default 0.5
									 , pn_min_rcnt_dif  number default 2);	


--!procedimiento para crear las combinaciones de datos a ser insertados en la tabla plan_jugada_details
procedure plan_jugada_dtl_handler(pv_drawing_type           VARCHAR2
								, pn_drawing_case			NUMBER
								, ptbl$pos1 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos2 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos3 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos4 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos5 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos6 				DBMS_SQL.VARCHAR2_TABLE
								, pn_seq_ini				NUMBER
								, pn_seq_end				NUMBER
								, x_err_code  IN OUT NOCOPY NUMBER	);
 
--!procedimiento para crear patrones de jugadas basadas en tres numeros ser insertados en la tabla plan_jugada_details
procedure ins_plan_jugada_tres_handler(pn_drawing_case		NUMBER
									 , pn_pos1 				NUMBER
									 , pn_pos2 				NUMBER
									 , pn_pos3 				NUMBER
									 , pn_pos4 				NUMBER
									 , pn_pos5 				NUMBER
									 , pn_pos6 				NUMBER
									 , pv_drawing_type      VARCHAR2 DEFAULT 'mrtr'
									  );


--!funciona valida si la jugada contiene alguno de los patrones de tres numeros
function is_plan_jugada_tres(pv_drawing_type		VARCHAR2
						   , pn_drawing_case		NUMBER
						   , pn_pos1 				NUMBER
						   , pn_pos2 				NUMBER
						   , pn_pos3 				NUMBER
						   , pn_pos4 				NUMBER
						   , pn_pos5 				NUMBER
						   , pn_pos6 				NUMBER) return boolean;		

--!procedimiento para crear patrones de jugadas basadas en distancia de extremos a ser insertados en la tabla plan_jugada_details
procedure ins_plan_jugada_extr_handler(pn_drawing_case		NUMBER
									 , pn_pos1 				NUMBER
									 , pv_drawing_type      VARCHAR2 DEFAULT 'mrtr'
									  );	

--!funciona valida si la jugada contiene alguno de los patrones de basado en la distancia de extremos
function is_plan_jugada_extremo(pv_drawing_type		VARCHAR2
						      , pn_drawing_case		NUMBER
						      , pn_pos1 			NUMBER) return boolean;	


--!construye las condiciones dinammicas de las columnas con cambio y sin cambio 
procedure get_dinamic_where_clause(pn_drawing_case			  		 NUMBER
								 , xv_not_null_columns IN OUT NOCOPY VARCHAR2
								 , xv_null_columns 	   IN OUT NOCOPY VARCHAR2
								 , x_err_code    	   IN OUT NOCOPY NUMBER
								  );

--!proceso para insertar los numeros resultantes en la tabla de salida
procedure ins_s_template_outputs(pn_process_id			 NUMBER
							   , pn_pos1				 NUMBER
							   , pn_pos2				 NUMBER
							   , pn_pos3				 NUMBER
							   , pn_pos4				 NUMBER
							   , pn_pos5				 NUMBER
							   , pn_pos6				 NUMBER
							   , pv_drawing_type		 VARCHAR2					  
							   , pn_drawing_id		     NUMBER
							   , pn_consecutivo_index    NUMBER DEFAULT 0
							   , pv_get_gl_info			 BOOLEAN DEFAULT TRUE
							   , xn_seq_no IN OUT NOCOPY NUMBER
							    );

--!recupera el ID global de las jugadas listas
function get_jugadas_listas_seq return number;					   			  
						   
--!proceso para actualizar contadores, estados y mensajes en los templates
procedure upd_s_templates_error (pn_process_id			  NUMBER
						       , pn_seq_no				  NUMBER
						       , pv_validation_message	  VARCHAR2
                               , pv_type                  VARCHAR2
						        );

--!valida la posicion de los numeros primos en base a metadata en tabla plan_jugadas
procedure valida_posicion_primos(pv_drawing_type			  VARCHAR2
							   , pn_drawing_case			  NUMBER
							   , pn_pos1					  NUMBER
							   , pn_pos2					  NUMBER
							   , pn_pos3			          NUMBER
							   , pn_pos4			          NUMBER
							   , pn_pos5			          NUMBER
							   , pn_pos6			          NUMBER
							   , xv_primo_flag	IN OUT NOCOPY VARCHAR2
							   , xv_pr1		IN OUT NOCOPY VARCHAR2
						       , xv_pr2		IN OUT NOCOPY VARCHAR2							  
							   , xv_pr3		IN OUT NOCOPY VARCHAR2
							   , xv_pr4		IN OUT NOCOPY VARCHAR2
							   , xv_pr5		IN OUT NOCOPY VARCHAR2
							   , xv_pr6		IN OUT NOCOPY VARCHAR2);

--!validacion de posiciones sin cambio
procedure valida_posiciones_sin_cambio (pv_drawing_type			  VARCHAR2
								      , pn_drawing_case			  NUMBER
                                      , pv_not_null_columns       VARCHAR2
                                      , pv_null_columns           VARCHAR2
									  , x_err_code  IN OUT NOCOPY NUMBER);

--!procedimiento para contar las jugadas que contengan los numeros favorables calculados por el proceso par_inpar_primo_cnt_handler
procedure valida_jugada_matches_cnt;

--!sumatoria de todos los campos con error
procedure actualiza_status_counter;	

--!proceso usado para insertar valores para hacer debug
procedure ins_tmp_testing (pv_valor		VARCHAR2);
							
--!proceso para generar las jugadas previas para el sorteo							
procedure pm_gl_genera_jugadas_handler(--!bandera para habilitar que se validen los ca por posicion
							           pv_look_ca_pos       	varchar2 default 'Y'
									   --!contador que indica hasta cuantos ca no pueden hacer match
									 , pn_ca_no_match_cnt  number default 1									 
									 , pn_seq_ini          number
									 , pn_seq_end          number
									 , pn_drawing_case     number default NULL
									 , pv_not_in_lt1       varchar2 default '&'
									 , pv_not_in_lt2       varchar2 default '&'
									 , pv_not_in_lt3       varchar2 default '&'
									 , pv_not_in_lt4       varchar2 default '&'
									 , pv_not_in_lt5       varchar2 default '&'
									 , pv_not_in_lt6       varchar2 default '&'
									 , pn_sum_nf           number default 3
									 , pn_cursor_option    number default 1
									 , pv_ca2_flag		   varchar2 default 'Y'			
									 , pv_ca3_flag		   varchar2 default 'Y'
									 , pv_ca4_flag		   varchar2 default 'Y'
									 , pv_ca5_flag		   varchar2 default 'Y'
									 --!deshabilitar funciones
									 , pv_dis_nf_config_valid 		varchar2 default 'N'
									 , pv_dis_filtrar_pareja_primos varchar2 default 'N'
									 , pv_dis_filtrar_extremos		varchar2 default 'N'
									 , pv_filtrar_primos_por_posicion varchar2 default 'N'
									  );	
									 
--!proceso para insertar rangos de ciclos de aparicion en la tabla plan_jugada_ciclos_aparicion
procedure plan_jugada_ca_handler (ptbl$plan_jugada_ca_tbl		gt$plan_jugada_ca_tbl);


--!guardar las estadisticas de la suma de los numeros primos en base a su posicion						  
procedure pm_primos_por_posicion_handler(pv_drawing_type    varchar2 default 'mrtr'
								       , pn_percentile		number default 0.25
									   , pn_primos_cnt		number default 2);		

--!procedimiento para crear patrones basados en numerosfavorables para ser insertados en la tabla plan_jugada_details
--!ejecutado manualmente por medio de la informacion del tab FAVORABLES
procedure gl_numeros_favobles_handler(pn_drawing_case			NUMBER
									, pv_pos1 				VARCHAR2
									, pv_pos2 				VARCHAR2
									, pv_pos3 				VARCHAR2
									, pv_pos4 				VARCHAR2
									, pv_pos5 				VARCHAR2
									, pv_pos6 				VARCHAR2
									, pn_resultados_cnt     NUMBER
									, pn_priority_flag      NUMBER DEFAULT NULL
									, pv_drawing_type       VARCHAR2 DEFAULT 'mrtr'
									 );	

--!insert de de conteo de numeros inpares y pares, terminaciones por decena en la tabla plan_jugada_details
--!este insert se crea en tomando el top 3 de los conteos por decena que arroja 
--!query para analizar las pares e inpares por decena 
procedure ins_plan_jugada_dtl_handler(pv_drawing_type      	VARCHAR2 DEFAULT 'mrtr'
								    , pv_description       	VARCHAR2
								    , pn_drawing_case		NUMBER
								    , pn_attribute1_cnt		NUMBER
								    , pn_attribute2_cnt		NUMBER DEFAULT NULL
									, pn_attribute3_cnt		NUMBER DEFAULT NULL
									, pn_attribute4_cnt		NUMBER DEFAULT NULL
									, pn_attribute5_cnt		NUMBER DEFAULT NULL
									, pn_attribute6_cnt		NUMBER DEFAULT NULL
								    , pn_jugadas_cnt		NUMBER DEFAULT NULL
								    , pn_resultados_cnt    	NUMBER DEFAULT NULL
									, pv_flag1				VARCHAR2 DEFAULT NULL
								     ); 

--!recupera el patron de inpares y pares de la posicion b3 y b4 por decena
procedure ins_b3_b4_inpar_par_handler (pv_drawing_type      VARCHAR2 DEFAULT 'mrtr');

--!recupera el patron de inpares y pares de la posicion b1, b4 y b6 por decena
procedure ins_b1_b4_b6_inpar_par_handler (pv_drawing_type      VARCHAR2 DEFAULT 'mrtr');

--!recupera el patron de inpares y pares de la posicion b1, b3, b4 y b6 por decena
procedure ins_b1_b3_b4_b6_inpar_par_hand (pv_drawing_type      VARCHAR2 DEFAULT 'mrtr');

--!recuperar los digitos en base al rango del percentile y al historico de resultados
procedure b1_b4_b6_ca_percentile_handler(pf_percentile_ini	FLOAT DEFAULT 0.1
									   , pf_percentile_end	FLOAT DEFAULT 0.8);

--!generar las jugadas para insertarlas en la tabla de interface
procedure jugadas_interface_handler (pv_val_pr_pa_in_c1_c6  varchar2 default 'Y'
								   , pv_val_centros_c3_c4   varchar2 default 'Y');
									  
--!insertar registros en la tabla plan_jugadas_details las combinaciones de primos, inpares y pares
--!tomando como base las columnas c1 y c6									  
procedure pr_pa_in_c1_c6_handler;
									  
--!insertar metadatos del mapa de numeros basados en 2 primos
procedure mapa_numeros_primos_handler;	
								  
--!mostrar el conteo de las repeticiones de numeros en funcion del digito base 								  
procedure listado_numeros_handler (pn_comb1   number
                                 , pn_historico number default 100);
								 
--!ejecuta el proceso numero_siguiente_wrapper para cada posicion del resultado
procedure numero_siguiente_handler(pn_drawing_id	number
                                 , pn_historico    number default 0);

--!buscar el patron del numero siguiente en base a un numero dado
procedure numero_siguiente_wrapper (pn_comb   		number
								  , pn_posicion		number
								  , pn_historico    number);

--!procedimiento para insertar registros en la tabla plan_jugada_details
procedure ins_plan_jugada_details(pv_drawing_type		VARCHAR2 DEFAULT 'mrtr'
								, pn_plan_jugada_id		NUMBER
								, pv_pos1				VARCHAR2
								, pv_pos2				VARCHAR2 DEFAULT NULL
								, pv_pos3				VARCHAR2 DEFAULT NULL
								, pv_pos4				VARCHAR2 DEFAULT NULL
								, pv_pos5				VARCHAR2 DEFAULT NULL
								, pv_pos6				VARCHAR2 DEFAULT NULL
								, pv_seq_no             VARCHAR2 DEFAULT NULL
								, pv_descripcion        VARCHAR2 DEFAULT 'LEY_TERCIO_IN'
								, pv_comments			VARCHAR2 DEFAULT NULL
								, pv_sort_execution     VARCHAR2 DEFAULT NULL
								, pv_flag1              VARCHAR2 DEFAULT NULL
								, pn_jugadas_cnt        NUMBER DEFAULT NULL
								, pn_resultados_cnt     NUMBER DEFAULT NULL
							 	 );
end w_new_pick_panorama_pkg;
/
show errors;