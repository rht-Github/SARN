create or replace package body olap_sys.w_pick_panorama_pkg as

--!variables globales
   --!decenas
   gv$d1			varchar2(100);
   gv$d2			varchar2(100);
   gv$d3			varchar2(100);
   gv$d4			varchar2(100);
   gv$d5			varchar2(100);
   gv$d6			varchar2(100);
   gv$decena_rank	varchar2(100);
   --!ciclo aparicion
   gv$ca1			varchar2(100);
   gv$ca2			varchar2(100);
   gv$ca3			varchar2(100);
   gv$ca4			varchar2(100);
   gv$ca5			varchar2(100);
   gv$ca6			varchar2(100);
   --!contador de primos, pares y nones
   gn$primo_cnt    	number := 0;
   gn$par_cnt      	number := 0;
   gn$non_cnt       number := 0;  
   gn$ins_cnt		number := 0;  
   --!multiplos de 3
   gv$conf_m3_1		varchar2(100);
   gv$conf_m3_2		varchar2(100);
   gv$conf_m3_3		varchar2(100);
   gv$conf_m3_4		varchar2(100);
   gv$conf_m3_5		varchar2(100);
   gv$conf_m3_6		varchar2(100);
   --!multiplos de 4
   gv$conf_m4_1		varchar2(100);
   gv$conf_m4_2		varchar2(100);
   gv$conf_m4_3		varchar2(100);
   gv$conf_m4_4		varchar2(100);
   gv$conf_m4_5		varchar2(100);
   gv$conf_m4_6		varchar2(100);
   --!multiplos de 5
   gv$conf_m5_1		varchar2(100);
   gv$conf_m5_2		varchar2(100);
   gv$conf_m5_3		varchar2(100);
   gv$conf_m5_4		varchar2(100);
   gv$conf_m5_5		varchar2(100);
   gv$conf_m5_6		varchar2(100);
   --!multiplos de 7
   gv$conf_m7_1		varchar2(100);
   gv$conf_m7_2		varchar2(100);
   gv$conf_m7_3		varchar2(100);
   gv$conf_m7_4		varchar2(100);
   gv$conf_m7_5		varchar2(100);
   gv$conf_m7_6		varchar2(100);
   --!ley del tercio
   gv$conf_lt1		varchar2(100);
   gv$conf_lt2		varchar2(100);
   gv$conf_lt3		varchar2(100);
   gv$conf_lt4		varchar2(100);
   gv$conf_lt5		varchar2(100);
   gv$conf_lt6		varchar2(100);
   --!ley del tercio log
   gv$conf_ltl1		varchar2(100);
   gv$conf_ltl2		varchar2(100);
   gv$conf_ltl3		varchar2(100);
   gv$conf_ltl4		varchar2(100);
   gv$conf_ltl5		varchar2(100);
   gv$conf_ltl6		varchar2(100);
   --!patrones de numeros
   gv$patron1		varchar2(100);
   gv$patron2		varchar2(100);
   gv$patron3		varchar2(100);
   gv$patron4		varchar2(100);
   gv$patron5		varchar2(100);
   gv$patron6		varchar2(100);
   --!variables para hacer fetch de cursores
   gv$fetch_pos1    varchar2(100);
   gv$fetch_pos2    varchar2(100);
   gv$fetch_pos3    varchar2(100);
   gv$fetch_pos4    varchar2(100);
   gv$fetch_pos5    varchar2(100);
   gv$fetch_pos6    varchar2(100);
   --!variables para remover la ultima coma en un string
   gv$valor_pos1    varchar2(100);
   gv$valor_pos2    varchar2(100);
   gv$valor_pos3    varchar2(100);
   gv$valor_pos4    varchar2(100);
   gv$valor_pos5    varchar2(100);
   gv$valor_pos6    varchar2(100);
    --!patrones de cambios de GL
   gv$chng_criteria_pos1 	varchar2(100);
   gv$chng_criteria_pos2 	varchar2(100);
   gv$chng_criteria_pos3 	varchar2(100);
   gv$chng_criteria_pos4 	varchar2(100);
   gv$chng_criteria_pos5 	varchar2(100);
   gv$chng_criteria_pos6 	varchar2(100);
   --!frecuencia	
   gv$conf_frec1			varchar2(100);
   gv$conf_frec2			varchar2(100);
   gv$conf_frec3			varchar2(100);
   gv$conf_frec4			varchar2(100);
   gv$conf_frec5			varchar2(100);
   gv$conf_frec6			varchar2(100);
   --!rango de frecuencias
   gv$frec1  				varchar2(100); 
   gv$frec2  				varchar2(100); 
   gv$frec3  				varchar2(100); 
   gv$frec4  				varchar2(100); 
   gv$frec5  				varchar2(100); 
   gv$frec6  				varchar2(100); 
   --!variables globales numericas para fetch de datos
   gn$value1				number := 0;	
   gn$value2				number := 0;
   gn$value3				number := 0;
   gn$value4				number := 0;
   gn$value5				number := 0;   
   --!variable para recuperar el ID del ultimo sorteo
   gn$drawing_id				number := 0;
   gn$jugadas_finales_cnt		number := 0;
   gn$jugadas_presentadas_cnt	number := 0;
   
   --!variables para le manejo de queries dinamicos
   gv$qry_stmt              		varchar2(1000);
   gv$qry_where_stmt        		varchar2(1000);
   gv$qry_order_stmt        		varchar2(1000);

   --!arreglo para convertir un string separado por comas en renglones de un query   
   gtbl$row_source                  dbms_sql.varchar2_table;
   gtbl$row_target                  dbms_sql.varchar2_table;

   --!variable global para armar string separado por comar para convertirlo en un arreglo	
   gv$tmp_list                      varchar2(200); 
   
   --!excepciones del proceso
   ge$select_list_len               exception;
   ge$numeros_primos_no_match       exception;
   ge$pares_nones_match             exception;
   ge$numero_primo_invalido         exception;
   ge$numero_par_invalido           exception;
   ge$numero_non_invalido           exception;  
   ge$cant_numero_primo_invalido    exception;
   ge$cant_num_par_non_invalido	    exception;
   ge$no_numeros_primos             exception;
   ge$numeros_listas_imcompletas	exception;
   
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
   CN$DIFERENCIA		   constant number(3) := 99;
   CN$BASE_DRAWING_ID      constant number(3) := 595;
   
   --!cursor global para recuperar la configuracion de numero primo, par y non
   cursor c_conf_ppn (pv_drawing_type              VARCHAR2
				    , pn_drawing_case              NUMBER) is
	SELECT ID
	     , NVL(POS1,CV$NUMERO_COMODIN) POS1
		 , NVL(POS2,CV$NUMERO_COMODIN) POS2
		 , NVL(POS3,CV$NUMERO_COMODIN) POS3
		 , NVL(POS4,CV$NUMERO_COMODIN) POS4
		 , NVL(POS5,CV$NUMERO_COMODIN) POS5
		 , NVL(POS6,CV$NUMERO_COMODIN) POS6
		 , SEQ_NO
		 , COMMENTS
		 , SORT_EXECUTION
	  FROM OLAP_SYS.PLAN_JUGADAS
	 WHERE DRAWING_TYPE = pv_drawing_type
	   AND DESCRIPTION  = 'CONFIG_PRIMOS_PARES_NONES'
	   AND STATUS       = 'A'
	   AND DRAWING_CASE = pn_drawing_case
	 ORDER BY SORT_EXECUTION;

   --!cursor global para recuperar la configuracion de numero primo, par y non
/*   cursor c_lt_in (pv_drawing_type              VARCHAR2
				 , pn_plan_jugada				NUMBER) is
	SELECT CASE WHEN POS1 IS NULL THEN 'LEY_TERCIO <= 2' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS1,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 2)' END POS1
		 , CASE WHEN POS2 IS NULL THEN 'LEY_TERCIO <= 2' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS2,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 2)' END POS2
		 , CASE WHEN POS3 IS NULL THEN 'LEY_TERCIO <= 2' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS3,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 2)' END POS3
		 , CASE WHEN POS4 IS NULL THEN 'LEY_TERCIO <= 2' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS4,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 2)' END POS4
		 , CASE WHEN POS5 IS NULL THEN 'LEY_TERCIO <= 2' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS5,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 2)' END POS5
		 , CASE WHEN POS6 IS NULL THEN 'LEY_TERCIO <= 3' ELSE '(COLOR_LEY_TERCIO IN ('||REPLACE(REPLACE(REPLACE(POS6,'R',1),'G',2),'B',3)||') AND LEY_TERCIO <= 3)' END POS6
		 , COMMENTS
		 , SORT_EXECUTION
	  FROM OLAP_SYS.PLAN_JUGADA_DETAILS
	 WHERE DRAWING_TYPE   = pv_drawing_type
	   AND DESCRIPTION    = 'LEY_TERCIO_IN'
	   AND STATUS         = 'A'
	   AND PLAN_JUGADA_ID = pn_plan_jugada
	 ORDER BY SORT_EXECUTION; */
	 
--!proceso usado para insertar valores para hacer debug
procedure ins_tmp_testing (pv_valor		VARCHAR2) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_tmp_testing';
begin
/*
	insert into olap_sys.tmp_testing(valor)
	select pv_valor
	  from dual
	 where not exists (select 1
						 from olap_sys.tmp_testing
						where valor = pv_valor) ;
	
	if sql%found then
		commit;
	end if;	
*/
	insert into olap_sys.tmp_testing(valor)	
	values (pv_valor);
	commit;
exception
	when others then
		dbms_output.put_line(LV$PROCEDURE_NAME||' '||sqlerrm);
end;


procedure initialize_global_variables is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'initialize_global_variables';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

   --!decenas
   gv$d1			:= null;
   gv$d2			:= null;
   gv$d3			:= null;
   gv$d4			:= null;
   gv$d5			:= null;
   gv$d6			:= null;
   --!ciclo aparicion
   gv$ca1			:= null;
   gv$ca2			:= null;
   gv$ca3			:= null;
   gv$ca4			:= null;
   gv$ca5			:= null;
   gv$ca6			:= null;
   --!contador de primos, pares y nones
   gn$primo_cnt    	:= 0;
   gn$par_cnt      	:= 0;
   gn$non_cnt       := 0;   
   --!multiplos de 3
   gv$conf_m3_1		:= null;
   gv$conf_m3_2		:= null;
   gv$conf_m3_3		:= null;
   gv$conf_m3_4		:= null;
   gv$conf_m3_5		:= null;
   gv$conf_m3_6		:= null;
   --!multiplos de 4
   gv$conf_m4_1		:= null;
   gv$conf_m4_2		:= null;
   gv$conf_m4_3		:= null;
   gv$conf_m4_4		:= null;
   gv$conf_m4_5		:= null;
   gv$conf_m4_6		:= null;
   --!multiplos de 5
   gv$conf_m5_1		:= null;
   gv$conf_m5_2		:= null;
   gv$conf_m5_3		:= null;
   gv$conf_m5_4		:= null;
   gv$conf_m5_5		:= null;
   gv$conf_m5_6		:= null;
   --!multiplos de 7
   gv$conf_m7_1		:= null;
   gv$conf_m7_2		:= null;
   gv$conf_m7_3		:= null;
   gv$conf_m7_4		:= null;
   gv$conf_m7_5		:= null;
   gv$conf_m7_6		:= null;
   --!ley del tercio
   gv$conf_lt1		:= null;
   gv$conf_lt2		:= null;
   gv$conf_lt3		:= null;
   gv$conf_lt4		:= null;
   gv$conf_lt5		:= null;
   gv$conf_lt6		:= null;
   --!ley del tercio log
   gv$conf_ltl1		:= null;
   gv$conf_ltl2		:= null;
   gv$conf_ltl3		:= null;
   gv$conf_ltl4		:= null;
   gv$conf_ltl5		:= null;
   gv$conf_ltl6		:= null;
   --!patrones de numeros
   gv$patron1		:= null;
   gv$patron2		:= null;
   gv$patron3		:= null;
   gv$patron4		:= null;
   gv$patron5		:= null;
   gv$patron6		:= null;
   --!variables para hacer fetch de cursores
   gv$fetch_pos1    := null;
   gv$fetch_pos2    := null;
   gv$fetch_pos3    := null;
   gv$fetch_pos4    := null;
   gv$fetch_pos5    := null;
   gv$fetch_pos6    := null;
   --!variables para remover la ultima coma en un string
   gv$valor_pos1    := null;
   gv$valor_pos2    := null;
   gv$valor_pos3    := null;
   gv$valor_pos4    := null;
   gv$valor_pos5    := null;
   gv$valor_pos6    := null;
   --!patrones de cambios de GL
   gv$chng_criteria_pos1 := null;
   gv$chng_criteria_pos2 := null;
   gv$chng_criteria_pos3 := null;
   gv$chng_criteria_pos4 := null;
   gv$chng_criteria_pos5 := null;
   gv$chng_criteria_pos6 := null;
   --!frecuencia
   gv$conf_frec1 := null;
   gv$conf_frec2 := null;
   gv$conf_frec3 := null;
   gv$conf_frec4 := null;
   gv$conf_frec5 := null;
   gv$conf_frec6 := null;
   --!rango de frecuencias
   gv$frec1 := null;
   gv$frec2 := null;
   gv$frec3 := null;
   gv$frec4 := null;
   gv$frec5 := null;
   gv$frec6 := null;
   
   --!variables para le manejo de queries dinamicos
   gv$qry_stmt        := null;
   gv$qry_where_stmt  := null;
   gv$qry_order_stmt  := null;
end initialize_global_variables;


--!funcion para contar los numeros pares e impares de la jugada
function par_inpar_contador (pv_drawing_ready		varchar2
						   , pv_digit_type			varchar2) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'par_inpar_contador';
	ln$inpar_cnt				   number :=0;	
	ln$par_cnt				   	   number :=0;
	ln$err_code					   number :=0;	
begin
--	dbms_output.put_line('pv_drawing_ready: '||pv_drawing_ready);
--	dbms_output.put_line('pv_digit_type: '||pv_digit_type);
	gtbl$row_source	.delete;
	
	--!validando la posicion de los numeros primos en el select list
	olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => pv_drawing_ready
												 , xtbl_row   => gtbl$row_source 
												 , x_err_code => ln$err_code
												  );    
	if gtbl$row_source.count > 0 then
		for t in gtbl$row_source.first..gtbl$row_source.last loop	
			--!contador de pares e impares
			if olap_sys.w_common_pkg.is_prime_number (pn_digit => to_number(gtbl$row_source(t))) = 0 then
--				dbms_output.put_line('digit: '||to_number(gtbl$row_source(t)));
				if mod(to_number(gtbl$row_source(t)),2) > 0 then
					ln$inpar_cnt := ln$inpar_cnt + 1;	
				else
					ln$par_cnt   := ln$par_cnt + 1;
				end if;	
			end if;		
		end loop;
	end if;	
--	dbms_output.put_line('ln$inpar_cnt: '||ln$inpar_cnt);
--	dbms_output.put_line('ln$par_cnt: '||ln$par_cnt);
	if pv_digit_type = 'PAR' then
		return ln$par_cnt;
	else
		return ln$inpar_cnt;
	end if;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    return 0;   	
end par_inpar_contador;



							
--!recuperar el ID ultimo sorteo
function get_max_drawing_id (pv_drawing_type             VARCHAR2) return number is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_max_drawing_id';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

   SELECT MAX(GAMBLING_ID) 
     INTO olap_sys.w_common_pkg.g_data_found
	 FROM OLAP_SYS.SL_GAMBLINGS 
	WHERE GAMBLING_TYPE = pv_drawing_type;

   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('ultimo sorteo ID: '||olap_sys.w_common_pkg.g_data_found);
   end if;		
   return olap_sys.w_common_pkg.g_data_found;	
end get_max_drawing_id;


procedure build_query (pv_drawing_type              		  VARCHAR2
				     , pn_select_id                 		  NUMBER
				     , pn_where_id                 		  NUMBER
					 , xv_qry_stmt   		IN OUT NOCOPY VARCHAR2
					 , xv_qry_where_stmt   	IN OUT NOCOPY VARCHAR2
					 , xv_qry_order_stmt   	IN OUT NOCOPY VARCHAR2
					 , x_err_code    		IN OUT NOCOPY NUMBER
  					  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'build_query';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);		
		dbms_output.put_line('pn_select_id: '||pn_select_id);
		dbms_output.put_line('pn_where_id: '||pn_where_id);
   end if;
    --!inicializando parametros de salida
	xv_qry_stmt   	  := null; 	
	xv_qry_where_stmt := null;
	xv_qry_order_stmt := null;
	
	SELECT SELECT_LIST
         , ORDER_BY
      INTO xv_qry_stmt
	     , xv_qry_order_stmt
	  FROM OLAP_SYS.C_SELECT_STMTS
     WHERE DRAWING_TYPE = 'mrtr'
       AND STATUS = 'A'
       AND ID = pn_select_id;--7;
 
	SELECT DML_STMT
  	  INTO xv_qry_where_stmt
	  FROM OLAP_SYS.C_MASTER_WHERE_STMTS
     WHERE DRAWING_TYPE = 'mrtr'
       AND STATUS = 'A'
       AND ID = pn_where_id;--9;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end build_query;	

--!configurar el query de gl con los filtros de cada posicion
procedure set_gl_query_rules(pv_drawing_type             	   VARCHAR2
						   , pn_digit_pos                      NUMBER
						   , pn_drawing_id                     NUMBER
						   , pv_decena          			   VARCHAR2
						   , pv_ciclo_aparicion 			   VARCHAR2
					       , pv_conf_lt                        VARCHAR2
						   , pv_conf_ppn                       VARCHAR2 
						   , pv_chng_criteria_pos			   VARCHAR2
						   , pv_cambios_gl_enable 			   VARCHAR2
						   , pv_conf_frec					   VARCHAR2
						   , pv_frec_enable					   VARCHAR2
						   , pv_frec						   VARCHAR2
						   , pv_ca_enable					   VARCHAR2
						   , pv_lt_enable					  VARCHAR2
						   , xv_qry_where_stmt   IN OUT NOCOPY VARCHAR2
						   , x_err_code          IN OUT NOCOPY NUMBER
                            ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'set_gl_query_rules';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_digit_pos: '||pn_digit_pos);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_decena: '||pv_decena);
		dbms_output.put_line('pv_ciclo_aparicion: '||pv_ciclo_aparicion);
		dbms_output.put_line('pv_conf_lt: '||pv_conf_lt);
		dbms_output.put_line('pv_conf_ppn: '||pv_conf_ppn);
		dbms_output.put_line('pv_chng_criteria_pos: '||pv_chng_criteria_pos);
		dbms_output.put_line('pv_cambios_gl_enable: '||pv_cambios_gl_enable);
		dbms_output.put_line('pv_conf_frec: '||pv_conf_frec);
		dbms_output.put_line('pv_frec_enable: '||pv_frec_enable);
		dbms_output.put_line('pv_frec: '||pv_frec);
		dbms_output.put_line('pv_ca_enable: '||pv_ca_enable);
		dbms_output.put_line('pv_lt_enable: '||pv_lt_enable);
   end if;
   
    --!filtros comunes
	xv_qry_where_stmt := replace(xv_qry_where_stmt,':1',CHR(39)||pv_drawing_type||CHR(39));
	xv_qry_where_stmt := replace(xv_qry_where_stmt,':2',pn_drawing_id);
    
	if pv_ca_enable = CV$ENABLE then
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<CICLO_APARICION>',pv_ciclo_aparicion);
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<DIGIT>',pv_decena);
	else
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<CICLO_APARICION>',CV$SIN_VALOR);
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<DIGIT>',CV$SIN_VALOR);	
	end if;
	
	if pv_cambios_gl_enable = CV$ENABLE then
		if pv_chng_criteria_pos is not null then
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<CAMBIOS_GL>',pv_chng_criteria_pos);
		else
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<CAMBIOS_GL>',CV$SIN_VALOR);
		end if;	
	else
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<CAMBIOS_GL>',CV$SIN_VALOR);
	end if;
		
	--!reemplazando valores para patron de frecuencias
	if pv_frec_enable = CV$ENABLE then
		if pv_conf_frec is not null then
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<FRECUENCIA>','COLOR_UBICACION IN ('||pv_conf_frec||')');
		else
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<FRECUENCIA>',CV$SIN_VALOR);
		end if;
		if pv_frec is not null then
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<RANGO_FRECUENCIA>',pv_frec);
		else
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<RANGO_FRECUENCIA>',CV$SIN_VALOR);		
		end if;
	else
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<FRECUENCIA>',CV$SIN_VALOR);
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<RANGO_FRECUENCIA>',CV$SIN_VALOR);		
	end if;
	
	--!reemplazando valores para ley del tercio
	if pv_lt_enable = CV$ENABLE then
		if pv_conf_lt is not null then
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<LEY_TERCIO>','COLOR_LEY_TERCIO IN ('||pv_conf_lt||')');
		else
			xv_qry_where_stmt := replace(xv_qry_where_stmt,'<LEY_TERCIO>',CV$SIN_VALOR);
		end if;
	else
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<LEY_TERCIO>',CV$SIN_VALOR);
	end if;

    if GB$SHOW_PROC_NAME then	
		dbms_output.put_line('pos: '||pn_digit_pos||' pv_conf_ppn: '||pv_conf_ppn);
		dbms_output.put_line('10 xv_qry_where_stmt: '||xv_qry_where_stmt);
	end if;

	--!reemplazando valores para numeros primos
	if instr(pv_conf_ppn,CV$NUMERO_PRIMO) > 0 then 
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 1');
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<PAR_NON>',CV$SIN_VALOR);		
	--!reemplazando valores para numeros pares y nones
	elsif instr(pv_conf_ppn,CV$NUMERO_PAR) > 0 then
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<PAR_NON>','INPAR_NUMBER_FLAG = 0');		
	elsif instr(pv_conf_ppn,CV$NUMERO_NON) > 0 then
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<PAR_NON>','INPAR_NUMBER_FLAG = 1');		
	elsif instr(pv_conf_ppn,CV$NUMERO_COMODIN) > 0 then
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		xv_qry_where_stmt := replace(xv_qry_where_stmt,'<PAR_NON>','INPAR_NUMBER_FLAG IN (0,1)');		
	end if;

    if GB$SHOW_PROC_NAME then
		dbms_output.put_line('20.1: '||substr(xv_qry_where_stmt,1,256));
		dbms_output.put_line('20.2: '||substr(xv_qry_where_stmt,256,256));
	end if;
	
	--1er posicion
	if pn_digit_pos = 1 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B1'||CHR(39));
	end if;

	--2da posicion
	if pn_digit_pos = 2 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B2'||CHR(39));
	end if;

	--3er posicion
	if pn_digit_pos = 3 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B3'||CHR(39));
	end if;

	--4ta posicion
	if pn_digit_pos = 4 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B4'||CHR(39));
	end if;

	--5ta posicion
	if pn_digit_pos = 5 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B5'||CHR(39));
	end if;

	--6ta posicion
	if pn_digit_pos = 6 then
	   xv_qry_where_stmt := replace(xv_qry_where_stmt,':3',CHR(39)||'B6'||CHR(39));
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
    if GB$SHOW_PROC_NAME then
		dbms_output.put_line('30.1: '||substr(xv_qry_where_stmt,1,256));
		dbms_output.put_line('30.2: '||substr(xv_qry_where_stmt,256,256));
	end if;
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end set_gl_query_rules;	

--!ejecutar el query y retornar la lista de digitos candidatos
procedure run_gl_query_rules(pn_digit_pos                  NUMBER
                           , pv_qry_stmt                   VARCHAR2
						   , pv_save_qry_enable			   VARCHAR2
						   , xtbl_qry_output IN OUT NOCOPY gt$gl_tbl
						   , x_err_code      IN OUT NOCOPY NUMBER
                            ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'run_gl_query_rules'; 
  le$no_data_found                 exception;  
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
   
	--!limpiando arreglo
	xtbl_qry_output.delete;
	
	if pv_save_qry_enable = 'Y' then
		ins_tmp_testing (pv_valor => pv_qry_stmt);    
	end if;
	
	--!ejecucion del query dinamico
	execute immediate pv_qry_stmt bulk collect into xtbl_qry_output;

    if GB$SHOW_PROC_NAME then
		dbms_output.put_line('xtbl_qry_output.count: '||xtbl_qry_output.count);
    end if;
   
	if xtbl_qry_output.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
--		raise le$no_data_found;
	end if;
exception
  when le$no_data_found then
	dbms_output.put_line('[gl] Valores nos encontrados en posicion B'||pn_digit_pos);
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;
end run_gl_query_rules;	


--!proceso para actualizar contadores, estados y mensajes en los templates
procedure upd_s_templates_error (pn_process_id			  NUMBER
						       , pn_seq_no				  NUMBER
						       , pv_validation_message	  VARCHAR2
						        ) is 
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_s_templates_error';

begin
	update olap_sys.s_template_hdr
	   set error_cnt = error_cnt + 1
	 where process_id = pn_process_id;
	 
	update olap_sys.s_template_outputs
	   set status = CV$STATUS_ERROR
	     , validation_message = validation_message||' '||pv_validation_message||'|'
	 where process_id = pn_process_id
	   and seq_no = pn_seq_no;

end upd_s_templates_error;

--!proceso para validar que el par de numeros primos corresponda a la diferencia tipo
procedure validate_diferencia_tipo (pn_process_id			  NUMBER
								  , pn_seq_no				  NUMBER
								  , pn_diferencia_tipo		  NUMBER
								  , pn_pos1             	  NUMBER
								  , pn_pos2             	  NUMBER
								  , pn_pos3             	  NUMBER
								  , pn_pos4             	  NUMBER
								  , pn_pos5             	  NUMBER
								  , pn_pos6             	  NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'validate_diferencia_tipo';
	ln$primo_ini	number := 0;
	ln$primo_end	number := 0;
	lv_pos_list		varchar(100);
	ln$err_code		number := -1;
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_process_id: '||pn_process_id);
		dbms_output.put_line('pn_seq_no: '||pn_seq_no);
		dbms_output.put_line('pn_diferencia_tipo: '||pn_diferencia_tipo);
		dbms_output.put_line('pn_pos1: '||pn_pos1);
		dbms_output.put_line('pn_pos2: '||pn_pos2);
		dbms_output.put_line('pn_pos3: '||pn_pos3);
		dbms_output.put_line('pn_pos4: '||pn_pos4);
		dbms_output.put_line('pn_pos5: '||pn_pos5);
		dbms_output.put_line('pn_pos6: '||pn_pos6);
	end if;
	
	lv_pos_list := 	to_char(pn_pos1)||','||to_char(pn_pos2)||','||to_char(pn_pos3)||','||to_char(pn_pos4)||','||to_char(pn_pos5)||','||to_char(pn_pos6);
	olap_sys.w_common_pkg.g_data_found := -1;
	
	--!convertir un string separado por comas en renglones de un query
	olap_sys.w_common_pkg.translate_string_to_rows (pv_string  => lv_pos_list
												  , xtbl_row   => gtbl$row_source
												  , x_err_code => ln$err_code
												   );
	
	if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		--!recuperando la pareja de numeros primos
		if gtbl$row_source.count > 0 then
			for r in gtbl$row_source.first..gtbl$row_source.last loop
				if olap_sys.w_common_pkg.is_prime_number (pn_digit => to_number(gtbl$row_source(r))) = 1 then
					if GB$SHOW_PROC_NAME then dbms_output.put_line('numero primo: '||gtbl$row_source(r)); end if;
					if ln$primo_ini = 0 then
						ln$primo_ini := to_number(gtbl$row_source(r));
						if GB$SHOW_PROC_NAME then dbms_output.put_line('primo_ini: '||ln$primo_ini); end if;					
					else
						if ln$primo_end = 0 then
							ln$primo_end := to_number(gtbl$row_source(r));
							if GB$SHOW_PROC_NAME then dbms_output.put_line('primo_end: '||ln$primo_end); end if;
							exit;
						end if;					
					end if;				
				end if;
			end loop;		
		end if;

		dbms_output.put_line('ln$primo_ini: '||ln$primo_ini);
		dbms_output.put_line('ln$primo_end: '||ln$primo_end);

		--!inicializando variable de salida
		olap_sys.w_common_pkg.g_data_found := -1;
		
		--!numeros primos con diferenciq < 0
		if pn_diferencia_tipo = 1 then
			select count(1) cnt
			  into olap_sys.w_common_pkg.g_data_found
			  from olap_sys.pm_parejas_primos
			 where play_status = CV$ENABLE
			   and diferencia < 0
			   and primo_ini = ln$primo_ini
			   and primo_fin = ln$primo_end;  	

dbms_output.put_line('diferencia < 0 g_data_found: '||olap_sys.w_common_pkg.g_data_found);
			if 	olap_sys.w_common_pkg.g_data_found = 0 then
dbms_output.put_line('upd_s_templates_error');			
				--!proceso para actualizar contadores, estados y mensajes en los templates
				upd_s_templates_error (pn_process_id		 => pn_process_id
									 , pn_seq_no			 => pn_seq_no
									 , pv_validation_message => LV$PROCEDURE_NAME);			   
			end if;	

		end if;

		
		if pn_diferencia_tipo = 2 then
			--!inicializando variable de salida
			olap_sys.w_common_pkg.g_data_found := -1;

			--!numeros primos con diferenciq > 0
			select count(1) cnt
			  into olap_sys.w_common_pkg.g_data_found
			  from olap_sys.pm_parejas_primos
			 where play_status = CV$ENABLE
			   and diferencia > 0
			   and primo_ini = ln$primo_ini
			   and primo_fin = ln$primo_end; 			

dbms_output.put_line('diferencia > 0 g_data_found: '||olap_sys.w_common_pkg.g_data_found);
			if 	olap_sys.w_common_pkg.g_data_found = 0 then
dbms_output.put_line('upd_s_templates_error');			
				--!proceso para actualizar contadores, estados y mensajes en los templates
				upd_s_templates_error (pn_process_id		 => pn_process_id
									 , pn_seq_no			 => pn_seq_no
									 , pv_validation_message => LV$PROCEDURE_NAME);			   
			end if;	
		end if;
	end if;
end validate_diferencia_tipo;

--!procedimiento para recuperar info de GL
procedure get_gl_info(pv_drawing_type				  VARCHAR2
                    , pn_drawing_id        			  NUMBER 
				    , pv_b_type						  VARCHAR2
					, pn_digit						  NUMBER
					, xn_gl_ca		    IN OUT NOCOPY NUMBER
			        , xv_change    	    IN OUT NOCOPY VARCHAR2
			        , xv_favorable      IN OUT NOCOPY VARCHAR2
					, x_err_code      	IN OUT NOCOPY NUMBER
				     ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_gl_info';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_b_type: '||pv_b_type);
		dbms_output.put_line('pn_digit: '||pn_digit);	
   end if;
	select ciclo_aparicion
	     , substr(chng_posicion,1,1)
	     , preferencia_flag
	  into xn_gl_ca
	     , xv_change
         , xv_favorable 		 
	  from olap_sys.s_calculo_stats 
	 where drawing_type = pv_drawing_type 
	   and drawing_id = pn_drawing_id 
	   and b_type = pv_b_type 
	   and digit = pn_digit;
	
exception
  when no_data_found then
	xn_gl_ca     := -1;
	xv_change    := '?';
	xv_favorable := '?';
  when others then
	xn_gl_ca     := -1;
	xv_change    := '?';
	xv_favorable := '?';
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());      
end get_gl_info;


--!procedimiento para recuperar info de GL
procedure get_gl_info_handler(pv_drawing_type				  VARCHAR2
							, pn_drawing_id        			  NUMBER 
							, pn_b1_digit					  NUMBER
							, pn_b2_digit					  NUMBER
							, pn_b3_digit					  NUMBER
							, pn_b4_digit					  NUMBER
							, pn_b5_digit					  NUMBER
							, pn_b6_digit					  NUMBER							
							, xn_gl_ca_pos1     IN OUT NOCOPY NUMBER
							, xn_gl_ca_pos2     IN OUT NOCOPY NUMBER
							, xn_gl_ca_pos3     IN OUT NOCOPY NUMBER
							, xn_gl_ca_pos4     IN OUT NOCOPY NUMBER
							, xn_gl_ca_pos5     IN OUT NOCOPY NUMBER
							, xn_gl_ca_pos6     IN OUT NOCOPY NUMBER
							, xv_change_pos1    IN OUT NOCOPY VARCHAR2
							, xv_change_pos2    IN OUT NOCOPY VARCHAR2
							, xv_change_pos3    IN OUT NOCOPY VARCHAR2
							, xv_change_pos4    IN OUT NOCOPY VARCHAR2
							, xv_change_pos5    IN OUT NOCOPY VARCHAR2
							, xv_change_pos6    IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos1 IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos2 IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos3 IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos4 IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos5 IN OUT NOCOPY VARCHAR2
							, xv_favorable_pos6 IN OUT NOCOPY VARCHAR2
							, x_err_code      	IN OUT NOCOPY NUMBER
							 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_gl_info_handler';
begin

	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B1'
			  , pn_digit		  => pn_b1_digit
			  , xn_gl_ca		  => xn_gl_ca_pos1
			  , xv_change    	  => xv_change_pos1
			  , xv_favorable	  => xv_favorable_pos1
			  , x_err_code        => x_err_code); 

	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B2'
			  , pn_digit		  => pn_b2_digit
			  , xn_gl_ca		  => xn_gl_ca_pos2
			  , xv_change    	  => xv_change_pos2
			  , xv_favorable	  => xv_favorable_pos2
			  , x_err_code        => x_err_code); 

	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B3'
			  , pn_digit		  => pn_b3_digit
			  , xn_gl_ca		  => xn_gl_ca_pos3
			  , xv_change    	  => xv_change_pos3
			  , xv_favorable	  => xv_favorable_pos3
			  , x_err_code        => x_err_code); 

	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B4'
			  , pn_digit		  => pn_b4_digit
			  , xn_gl_ca		  => xn_gl_ca_pos4
			  , xv_change    	  => xv_change_pos4
			  , xv_favorable	  => xv_favorable_pos4
			  , x_err_code        => x_err_code); 
			  
	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B5'
			  , pn_digit		  => pn_b5_digit
			  , xn_gl_ca		  => xn_gl_ca_pos5
			  , xv_change    	  => xv_change_pos5
			  , xv_favorable	  => xv_favorable_pos5
			  , x_err_code        => x_err_code); 

			  	--!procedimiento para recuperar info de GL
	get_gl_info(pv_drawing_type	  => pv_drawing_type
			  , pn_drawing_id     => pn_drawing_id
			  , pv_b_type		  => 'B6'
			  , pn_digit		  => pn_b6_digit
			  , xn_gl_ca		  => xn_gl_ca_pos6
			  , xv_change    	  => xv_change_pos6
			  , xv_favorable	  => xv_favorable_pos6
			  , x_err_code        => x_err_code); 

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());      
end get_gl_info_handler;


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
							   , xn_seq_no IN OUT NOCOPY NUMBER
							    ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_s_template_outputs';
	lv$drawing_ready				varchar2(50);
	ln$inpar_cnt					number := 0;
	ln$par_cnt						number := 0;
	ln$pos_sum					    number := 0;
	ln$gl_ca_pos1					number := 0;
	ln$gl_ca_pos2					number := 0;
	ln$gl_ca_pos3					number := 0;
	ln$gl_ca_pos4					number := 0;
	ln$gl_ca_pos5					number := 0;
	ln$gl_ca_pos6					number := 0;
	ln$gl_ca_sum					number := 0;
	lv$change_pos1				    varchar2(1);
	lv$change_pos2				    varchar2(1);
	lv$change_pos3				    varchar2(1);
	lv$change_pos4				    varchar2(1);
	lv$change_pos5				    varchar2(1);
	lv$change_pos6				    varchar2(1);
	lv$favorable_pos1				varchar2(1);
	lv$favorable_pos2				varchar2(1);
	lv$favorable_pos3				varchar2(1);
	lv$favorable_pos4				varchar2(1);
	lv$favorable_pos5				varchar2(1);
	lv$favorable_pos6				varchar2(1);
	ln$err_code						number := 0;
begin
	
	--!armando lista de numeros separadas por comas
	lv$drawing_ready := lpad(pn_pos1,2,0)
				 ||','||lpad(pn_pos2,2,0) 
				 ||','||lpad(pn_pos3,2,0) 
				 ||','||lpad(pn_pos4,2,0) 
				 ||','||lpad(pn_pos5,2,0) 
				 ||','||lpad(pn_pos6,2,0);
				 
	--!funcion para contar los numeros pares e impares de la jugada
    ln$inpar_cnt := par_inpar_contador (pv_drawing_ready => lv$drawing_ready, pv_digit_type => 'INPAR');
	ln$par_cnt	 := par_inpar_contador (pv_drawing_ready => lv$drawing_ready, pv_digit_type => 'PAR');
	ln$pos_sum   := pn_pos1 + pn_pos2 + pn_pos3 + pn_pos4 + pn_pos5 + pn_pos6;


	--!procedimiento para recuperar info de GL
	get_gl_info_handler(pv_drawing_type	  => pv_drawing_type
					  , pn_drawing_id     => pn_drawing_id
					  , pn_b1_digit		  => pn_pos1
					  , pn_b2_digit		  => pn_pos2
					  , pn_b3_digit		  => pn_pos3
					  , pn_b4_digit		  => pn_pos4
					  , pn_b5_digit		  => pn_pos5
					  , pn_b6_digit		  => pn_pos6							
					  , xn_gl_ca_pos1     => ln$gl_ca_pos1
					  , xn_gl_ca_pos2     => ln$gl_ca_pos2
					  , xn_gl_ca_pos3     => ln$gl_ca_pos3
					  , xn_gl_ca_pos4     => ln$gl_ca_pos4
					  , xn_gl_ca_pos5     => ln$gl_ca_pos5
					  , xn_gl_ca_pos6     => ln$gl_ca_pos6
					  , xv_change_pos1    => lv$change_pos1
					  , xv_change_pos2    => lv$change_pos2
					  , xv_change_pos3    => lv$change_pos3
					  , xv_change_pos4    => lv$change_pos4
					  , xv_change_pos5    => lv$change_pos5
					  , xv_change_pos6    => lv$change_pos6
					  , xv_favorable_pos1 => lv$favorable_pos1
					  , xv_favorable_pos2 => lv$favorable_pos2
					  , xv_favorable_pos3 => lv$favorable_pos3
					  , xv_favorable_pos4 => lv$favorable_pos4
					  , xv_favorable_pos5 => lv$favorable_pos5
					  , xv_favorable_pos6 => lv$favorable_pos6
					  , x_err_code        => ln$err_code
					   );

	if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		--!funcion que regresa la sum del gl ciclo de aparicion de un jugada
		ln$gl_ca_sum := ln$gl_ca_pos1 + ln$gl_ca_pos2 + ln$gl_ca_pos3 + ln$gl_ca_pos4 + ln$gl_ca_pos5 + ln$gl_ca_pos6;
				
		insert into olap_sys.s_template_outputs (process_id
											   , seq_no
											   , pos1
											   , pos2
											   , pos3
											   , pos4
											   , pos5
											   , pos6
											   , inpar_cnt
											   , par_cnt
											   , pos_sum
											   , gl_ca_pos1
											   , gl_ca_pos2
											   , gl_ca_pos3
											   , gl_ca_pos4
											   , gl_ca_pos5
											   , gl_ca_pos6
											   , gl_ca_sum
											   , change_pos1
											   , change_pos2
											   , change_pos3
											   , change_pos4
											   , change_pos5
											   , change_pos6
											   , favorable_pos1
											   , favorable_pos2
											   , favorable_pos3
											   , favorable_pos4
											   , favorable_pos5
											   , favorable_pos6
											   , status
											   , created_by
											   , creation_date)
		values (pn_process_id
			  , (select nvl(max(seq_no),0)+1 from olap_sys.s_template_outputs)
			  , pn_pos1
			  , pn_pos2
			  , pn_pos3
			  , pn_pos4
			  , pn_pos5
			  , pn_pos6
			  , ln$inpar_cnt
			  , ln$par_cnt
			  , ln$pos_sum
			  , ln$gl_ca_pos1
			  , ln$gl_ca_pos2
			  , ln$gl_ca_pos3
			  , ln$gl_ca_pos4
			  , ln$gl_ca_pos5
			  , ln$gl_ca_pos6
			  , ln$gl_ca_sum
			  , lv$change_pos1
			  , lv$change_pos2
			  , lv$change_pos3
			  , lv$change_pos4
			  , lv$change_pos5
			  , lv$change_pos6
			  , lv$favorable_pos1
			  , lv$favorable_pos2
			  , lv$favorable_pos3
			  , lv$favorable_pos4
			  , lv$favorable_pos5
			  , lv$favorable_pos6
			  , CV$STATUS_ACTIVO
			  , user
			  , sysdate) returning seq_no into xn_seq_no;
		
		--!contador de jugadas
		gn$jugadas_presentadas_cnt := gn$jugadas_presentadas_cnt + 1;	

	end if;
	
exception
  when dup_val_on_index then
	--!no accion cuando hay una jugada duplicada
	null;
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());      
end ins_s_template_outputs;


--!ejecutar el panorama query para obtener los numeros a jugar
procedure run_panorama_query_rules(pv_drawing_type					  VARCHAR2
                                 , pn_drawing_id        			  NUMBER 
							     , pn_drawing_case             	  	  NUMBER
								 , pn_term_cnt						  NUMBER
								 , pv_qry_stmt                   	  VARCHAR2
								 , pv_save_qry_enable			 	  VARCHAR2
								 , pn_diferencia_tipo				  NUMBER
								 , pv_digit_list_pos1 		 	   	  VARCHAR2
								 , pv_digit_list_pos2 		 	  	  VARCHAR2
								 , pv_digit_list_pos3 		 	      VARCHAR2
								 , pv_digit_list_pos4 		 	      VARCHAR2
								 , pv_digit_list_pos5 		 	      VARCHAR2
								 , pv_digit_list_pos6 		 	      VARCHAR2
								 , pn_process_id					  NUMBER
								 , x_err_code      		IN OUT NOCOPY NUMBER
								  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'run_panorama_query_rules';
  lv$qry_stmt			           varchar2(2000);
  ln$active_cnt  				   number := 0;  
  ln$seq_no						   number := 0; 
  ltbl$qry_output 				   gt$panorama_tbl; 				   	 
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_qry_stmt.len: '||length(pv_qry_stmt));
		dbms_output.put_line('pv_save_qry_enable: '||pv_save_qry_enable);
   end if;
    
	--!multiples terminaciones
	if pn_term_cnt = 1 then
		lv$qry_stmt := replace(pv_qry_stmt,'<1>','(T1_CNT = '||pn_term_cnt||' or '||'T2_CNT = '||pn_term_cnt||')');
	elsif pn_term_cnt = 2 then
		lv$qry_stmt := replace(pv_qry_stmt,'<1>','T2_CNT = '||pn_term_cnt);
	end if;
	
	--!pos1
	lv$qry_stmt := replace(lv$qry_stmt,'<2>',pv_digit_list_pos1);

	--!pos2
	lv$qry_stmt := replace(lv$qry_stmt,'<3>',pv_digit_list_pos2);

	--!pos3
	lv$qry_stmt := replace(lv$qry_stmt,'<4>',pv_digit_list_pos3);

	--!pos4
	lv$qry_stmt := replace(lv$qry_stmt,'<5>',pv_digit_list_pos4);

	--!pos5
	lv$qry_stmt := replace(lv$qry_stmt,'<6>',pv_digit_list_pos5);

	--!pos6
	lv$qry_stmt := replace(lv$qry_stmt,'<7>',pv_digit_list_pos6);
	
	ins_tmp_testing (pv_valor => lv$qry_stmt); 
	
	begin
		--!limpiando arreglo
		ltbl$qry_output.delete;
		
		execute immediate lv$qry_stmt bulk collect into ltbl$qry_output;																										
		dbms_output.put_line('Jugadas encontradas: '||ltbl$qry_output.count);
		if ltbl$qry_output.count > 0 then
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line(ltbl$qry_output.count||' jugadas encontradas en el Panorama');
			end if;
			
			--!actualizando el contador de registros template_hdr
			ln$active_cnt := ltbl$qry_output.count;
			update olap_sys.s_template_hdr
			   set active_cnt = ln$active_cnt
			 where process_id = pn_process_id;  
			
			for t in ltbl$qry_output.first..ltbl$qry_output.last loop						  
				--!proceso para insertar los numeros resultantes en la tabla de salida
				ins_s_template_outputs(pn_process_id     => pn_process_id
									 , pn_pos1		     => ltbl$qry_output(t).pos1
									 , pn_pos2	         => ltbl$qry_output(t).pos2
									 , pn_pos3		     => ltbl$qry_output(t).pos3
									 , pn_pos4		     => ltbl$qry_output(t).pos4
									 , pn_pos5		     => ltbl$qry_output(t).pos5
									 , pn_pos6		     => ltbl$qry_output(t).pos6
									 , pv_drawing_type   => pv_drawing_type					  
							         , pn_drawing_id     => pn_drawing_id 
									 , xn_seq_no         => ln$seq_no
									  ); 
				
				--!funcion para validar que el par de numeros primos corresponda a la diferencia tipo				
				validate_diferencia_tipo (pn_process_id      => pn_process_id
				                        , pn_seq_no 	     => ln$seq_no
										, pn_diferencia_tipo => pn_diferencia_tipo
									    , pn_pos1		     => ltbl$qry_output(t).pos1
										, pn_pos2	         => ltbl$qry_output(t).pos2
										, pn_pos3		     => ltbl$qry_output(t).pos3
										, pn_pos4		     => ltbl$qry_output(t).pos4
										, pn_pos5		     => ltbl$qry_output(t).pos5
										, pn_pos6		     => ltbl$qry_output(t).pos6); 
																					   
			end loop;																
		end if;																	
	exception
	when no_data_found then
		dbms_output.put_line('Consulta del PANORAMA no recupero datos. criterio: '||pv_digit_list_pos1||'-'||pv_digit_list_pos2||'-'||pv_digit_list_pos3||'-'||pv_digit_list_pos4||'-'||pv_digit_list_pos5||'-'||pv_digit_list_pos6);
	end;	

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||' ~ '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
end run_panorama_query_rules;	


--!si las decenas continuas son iguales y el digito de la 1a decena es 9 regresa false
--!de lo contrario regresa true y el numero se agrega a la lista de numeros
procedure valida_ultimo_digito_decena (pv_decena                        VARCHAR2
									 , pv_next_decena                   VARCHAR2 
									 , xtbl_qry_output    IN OUT NOCOPY gt$gl_tbl
									 , x_err_code    	  IN OUT NOCOPY NUMBER									
                                      ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'valida_ultimo_digito_decena';
begin
	if pv_decena != CV$SIN_VALOR then
	   if GB$SHOW_PROC_NAME then
			dbms_output.put_line('--------------------------------');
			dbms_output.put_line(LV$PROCEDURE_NAME);
			dbms_output.put_line('ini xtbl_qry_output.cnt: '||xtbl_qry_output.count);
			dbms_output.put_line('pv_decena: '||pv_decena);
			dbms_output.put_line('pv_next_decena: '||pv_next_decena);
	   end if;
		

		if xtbl_qry_output.count > 0 then
			--!si la decena inicial es igual a la siguiente y la decena inicial el numero termina en 9 entonces se remueve ese numero
			--!terminado en 9 de la lista. Ejemplo: el numero 19 se removera de la lista de la decena inicial cuando ambas decenas sean 10-19
			for i in xtbl_qry_output.first..xtbl_qry_output.last loop
				if substr(lpad(xtbl_qry_output(i).digit,2,0),2,1) = '9' and pv_decena = pv_next_decena then
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('HEY borrando row: '||i);
					end if;	
					xtbl_qry_output(i).change_flag := 'N';
				end if;	
			end loop;
		end if;
		
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('end xtbl_qry_output.cnt: '||xtbl_qry_output.count);
		end if;
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    x_err_code := sqlcode;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
end valida_ultimo_digito_decena;  

--!regresar lista de numeros en base a GL
procedure get_gl_handler(pv_drawing_type             	  VARCHAR2
                       , pn_digit_pos      			 	  NUMBER
                       , pv_conf_lt     			 	  VARCHAR2
                       , pv_decena                        VARCHAR2
					   , pv_ciclo_aparicion               VARCHAR2
					   , pn_drawing_id               	  NUMBER
					   , pv_qry_stmt   					  VARCHAR2
					   , pv_qry_where_stmt                VARCHAR2
					   , pv_qry_order_stmt                VARCHAR2
					   , pv_conf_ppn                      VARCHAR2 
                       , pv_next_decena			          VARCHAR2
					   , pv_save_qry_enable				  VARCHAR2
					   , pv_chng_criteria_pos			  VARCHAR2
					   , pv_cambios_gl_enable			  VARCHAR2
					   , pv_conf_frec					  VARCHAR2
					   , pv_frec_enable					  VARCHAR2
					   , pv_frec						  VARCHAR2
					   , pv_ca_enable					  VARCHAR2
					   , pv_lt_enable					  VARCHAR2
					   , xtbl_qry_output    IN OUT NOCOPY gt$gl_tbl 
					   , x_err_code    		IN OUT NOCOPY NUMBER
                        ) is
  LV$PROCEDURE_NAME       constant  varchar2(30) := 'get_gl_handler';
  gv$qry_where_stmt                 varchar2(1000) := pv_qry_where_stmt;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
  
   --!configurar el query de gl con los filtros de cada posicion
   set_gl_query_rules(pv_drawing_type    => pv_drawing_type
				    , pn_digit_pos       => pn_digit_pos
				    , pn_drawing_id      => pn_drawing_id
				    , pv_decena          => pv_decena
				    , pv_ciclo_aparicion => pv_ciclo_aparicion
					, pv_conf_lt         => pv_conf_lt
					, pv_conf_ppn        => pv_conf_ppn
					, pv_chng_criteria_pos => pv_chng_criteria_pos
					, pv_cambios_gl_enable => pv_cambios_gl_enable
					, pv_conf_frec		 => pv_conf_frec
					, pv_frec_enable	 => pv_frec_enable
					, pv_frec			 => pv_frec
					, pv_ca_enable		 => pv_ca_enable
					, pv_lt_enable   	 => pv_lt_enable
				    , xv_qry_where_stmt  => gv$qry_where_stmt
				    , x_err_code         => x_err_code
				     ); 

	if GB$SHOW_PROC_NAME then
        dbms_output.put_line('--------------------------------');
		dbms_output.put_line(substr(gv$qry_where_stmt,1,255));
--		dbms_output.put_line(substr(gv$qry_where_stmt,256,255));
--		dbms_output.put_line(substr(gv$qry_where_stmt,511,255));
--		dbms_output.put_line(substr(gv$qry_where_stmt,766,255));
	end if;
			
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		
		--!uniendo las piezas para formar el query de gl
		olap_sys.w_common_pkg.g_dml_stmt := pv_qry_stmt||' '||gv$qry_where_stmt||' '||pv_qry_order_stmt;
		
		--!ejecutar el query y retornar la lista de digitis por digito
		run_gl_query_rules(pn_digit_pos    => pn_digit_pos
		                 , pv_qry_stmt     => olap_sys.w_common_pkg.g_dml_stmt
					     , pv_save_qry_enable => pv_save_qry_enable
						 , xtbl_qry_output => xtbl_qry_output
					     , x_err_code      => x_err_code
					      );

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!si las decenas continuas son iguales y el digito de la 1a decena es 9 regresa false
				--!de lo contrario regresa true y el numero se agrega a la lista de numeros
				valida_ultimo_digito_decena (pv_decena     => pv_decena
										   , pv_next_decena  => pv_next_decena
										   , xtbl_qry_output => xtbl_qry_output
										   , x_err_code      => x_err_code								
										    );	
			end if;						  
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_gl_handler;						


--!validar parametros de entrada
procedure validar_parametros_entrada(pn_digit_pos      			 	  NUMBER
								   , pv_select_list              	  VARCHAR2
					               , pn_par_cnt                  	  NUMBER
					               , pn_non_cnt                  	  NUMBER
					               , pn_terminacion_cnt          	  NUMBER 
					               , x_err_code         IN OUT NOCOPY NUMBER
                                    ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_parametros_entrada';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
	--!validando que haya 6 IDs unicos en el select list del panorama
	for j in (with select_tbl as (select regexp_substr(pv_select_list,'[^,]+',1,level) str
									from dual 
								 connect by level <= length(pv_select_list)-length(replace(pv_select_list,',',''))+1
								 ) select count(distinct substr(str,1,1)) column_cnt from select_tbl) loop
		if j.column_cnt < 6 then
			x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
			dbms_output.put_line('-------------------------------------------------------------------');
			dbms_output.put_line('El ID de cada columna del select list debe ser unico. Column_cnt: '||j.column_cnt);		
		end if;
	end loop;
		
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then			
		if pn_par_cnt > 0 and pn_non_cnt > 0 then 
			if (pn_par_cnt!=2 or pn_non_cnt!=2) and (pn_par_cnt!=3 or pn_non_cnt!=1) then
				x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
				dbms_output.put_line('-------------------------------------------------------------------');
				dbms_output.put_line('Solo se aceptan jugadas con (2 pares, 2 nones) o (3 pares, 1 non)');				
			end if;	
		end if;
		
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then	
			if pn_terminacion_cnt is null or pn_terminacion_cnt > 5 then
			x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
			dbms_output.put_line('-------------------------------------------------------------------');
			dbms_output.put_line('pn_terminacion_cnt es requerido o debe ser menor a 5 terminaciones repetidas');								
			end if;
		end if;
	end if;

exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end validar_parametros_entrada;	

--!proceso para recuperar lista de digitos por posicion
procedure get_lista_digitos_por_posicion(pv_drawing_type             	  VARCHAR2
                                       , pn_digit_pos      		     	  NUMBER
                                       , pv_conf_lt     		     	  VARCHAR2
                                       , pv_decena                        VARCHAR2
					                   , pv_ciclo_aparicion               VARCHAR2
									   , pn_drawing_id               	  NUMBER
					                   , pv_select_list              	  VARCHAR2
					                   , pn_par_cnt                  	  NUMBER
					                   , pn_non_cnt                  	  NUMBER
					                   , pn_terminacion_cnt          	  NUMBER 
									   , pv_qry_stmt   				 	  VARCHAR2
									   , pv_qry_where_stmt           	  VARCHAR2
									   , pv_qry_order_stmt           	  VARCHAR2
									   , pv_conf_ppn                      VARCHAR2  
									   , pv_next_decena                   VARCHAR2 DEFAULT NULL
									   , pv_save_qry_enable				  VARCHAR2
									   , pv_chng_criteria_pos			  VARCHAR2
									   , pv_cambios_gl_enable 			  VARCHAR2
									   , pv_conf_frec					  VARCHAR2
									   , pv_frec_enable					  VARCHAR2
									   , pv_frec						  VARCHAR2
									   , pv_ca_enable					  VARCHAR2
									   , pv_lt_enable					  VARCHAR2
									   , xtbl_qry_output	IN OUT NOCOPY gt$gl_tbl
  				                       , x_err_code    		IN OUT NOCOPY NUMBER
                                        ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_lista_digitos_por_posicion';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

    --!validar parametros de entrada
    validar_parametros_entrada(pn_digit_pos       => pn_digit_pos
							 , pv_select_list     => pv_select_list
					         , pn_par_cnt         => pn_par_cnt
					         , pn_non_cnt         => pn_non_cnt
					         , pn_terminacion_cnt => pn_terminacion_cnt
					         , x_err_code         => x_err_code
                              );     	
		
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then		
		--!regresar lista de numeros en base a GL 
		get_gl_handler(pv_drawing_type   => pv_drawing_type
					 , pn_digit_pos      => pn_digit_pos
					 , pv_conf_lt     => pv_conf_lt
					 , pv_decena         => pv_decena
					 , pv_ciclo_aparicion=> pv_ciclo_aparicion				 
					 , pn_drawing_id 	 => pn_drawing_id
					 , pv_qry_stmt   	 => pv_qry_stmt 
					 , pv_qry_where_stmt => pv_qry_where_stmt
					 , pv_qry_order_stmt => pv_qry_order_stmt
					 , pv_conf_ppn       => pv_conf_ppn
					 , pv_next_decena    => pv_next_decena
					 , pv_save_qry_enable => pv_save_qry_enable
					 , pv_chng_criteria_pos	=> pv_chng_criteria_pos
					 , pv_cambios_gl_enable => pv_cambios_gl_enable
					 , pv_conf_frec      => pv_conf_frec
					 , pv_frec_enable	 => pv_frec_enable
					 , pv_frec           => pv_frec
					 , pv_ca_enable      => pv_ca_enable
					 , pv_lt_enable		 => pv_lt_enable
					 , xtbl_qry_output   => xtbl_qry_output
					 , x_err_code        => x_err_code
					  ); 
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;		  
	end if;
		
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_lista_digitos_por_posicion;


--!ejecutar el query y retornar la lista de digitis por digito
procedure ordena_select_list_panorama(pv_conf_ppn1   				 VARCHAR2
								    , pv_conf_ppn2   				 VARCHAR2
								    , pv_conf_ppn3   				 VARCHAR2
								    , pv_conf_ppn4   				 VARCHAR2
								    , pv_conf_ppn5   				 VARCHAR2
								    , pv_conf_ppn6       			 VARCHAR2
									, xv_select_list   IN OUT NOCOPY VARCHAR2
						            , x_err_code       IN OUT NOCOPY NUMBER
                                     ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'ordena_select_list_panorama';  
  ln$numero_primo_cnt              number(1) := 0;
  ln$numero_compuesto_cnt          number(1) := 0;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	gv$tmp_list := pv_conf_ppn1||','||pv_conf_ppn2||','||pv_conf_ppn3||','||pv_conf_ppn4||','||pv_conf_ppn5||','||pv_conf_ppn6;
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('CONFIG_PRIMOS_PARES_NONE: '||gv$tmp_list);
    end if;	
   
	--!validando la posicion de los numeros primos en el select list
	olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => gv$tmp_list
												 , xtbl_row   => gtbl$row_source 
												 , x_err_code => x_err_code
												  );    

	--!ejemplo de select list: '1-<PR1>,2-P1_COMP,3-<PR2>,4-P2_COMP,5-P3_COMP,6-P4_COMP'

	for t in gtbl$row_source.first..gtbl$row_source.last loop
		if t = 1 then
			if gtbl$row_source(t) = 'PR' then
				ln$numero_primo_cnt := ln$numero_primo_cnt + 1;
				xv_select_list := t||'-<PR'||ln$numero_primo_cnt||'>';
			else
				ln$numero_compuesto_cnt := ln$numero_compuesto_cnt + 1;
				xv_select_list := t||'-P'||ln$numero_compuesto_cnt||'_COMP';
			end if;	
		else
			if gtbl$row_source(t) = 'PR' then
				ln$numero_primo_cnt := ln$numero_primo_cnt + 1;
				xv_select_list := xv_select_list||','||t||'-<PR'||ln$numero_primo_cnt||'>';
			else
				ln$numero_compuesto_cnt := ln$numero_compuesto_cnt + 1;
				xv_select_list := xv_select_list||','||t||'-P'||ln$numero_compuesto_cnt||'_COMP';
			end if;			
		end if;
	end loop;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(xv_select_list);
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end ordena_select_list_panorama;


--!proceso para obtener las decenas del plan de juego
procedure get_plan_jugada_decenas(pv_drawing_type                  VARCHAR2
						        , pn_drawing_case                  NUMBER
								, pv_show_init_values			   VARCHAR2 DEFAULT 'Y'
								--!decenas
							    , xv_d1         	IN OUT NOCOPY VARCHAR2
							    , xv_d2         	IN OUT NOCOPY VARCHAR2
							    , xv_d3         	IN OUT NOCOPY VARCHAR2
							    , xv_d4         	IN OUT NOCOPY VARCHAR2
							    , xv_d5         	IN OUT NOCOPY VARCHAR2
							    , xv_d6         	IN OUT NOCOPY VARCHAR2
								, xv_decena_rank    IN OUT NOCOPY VARCHAR2								
						        , x_err_code    IN OUT NOCOPY NUMBER
						        ) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_decenas';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	--!decenas
	begin
		SELECT 'DIGIT BETWEEN '||SUBSTR(POS1,1,INSTR(POS1,'-',1,1)-1)||' AND '||SUBSTR(POS1,INSTR(POS1,'-',1,1)+1) POS1
			 , 'DIGIT BETWEEN '||SUBSTR(POS2,1,INSTR(POS2,'-',1,1)-1)||' AND '||SUBSTR(POS2,INSTR(POS2,'-',1,1)+1) POS2
			 , 'DIGIT BETWEEN '||SUBSTR(POS3,1,INSTR(POS3,'-',1,1)-1)||' AND '||SUBSTR(POS3,INSTR(POS3,'-',1,1)+1) POS3
			 , 'DIGIT BETWEEN '||SUBSTR(POS4,1,INSTR(POS4,'-',1,1)-1)||' AND '||SUBSTR(POS4,INSTR(POS4,'-',1,1)+1) POS4
			 , 'DIGIT BETWEEN '||SUBSTR(POS5,1,INSTR(POS5,'-',1,1)-1)||' AND '||SUBSTR(POS5,INSTR(POS5,'-',1,1)+1) POS5
			 , 'DIGIT BETWEEN '||SUBSTR(POS6,1,INSTR(POS6,'-',1,1)-1)||' AND '||SUBSTR(POS6,INSTR(POS6,'-',1,1)+1) POS6
			 , PM_DECENA_RANK
		  INTO xv_d1
			 , xv_d2
			 , xv_d3
			 , xv_d4
			 , xv_d5
			 , xv_d6
			 , xv_decena_rank
		  FROM OLAP_SYS.PLAN_JUGADAS
		 WHERE DRAWING_TYPE = pv_drawing_type
		   AND DESCRIPTION  = 'DECENAS'
		   AND STATUS       = 'A'
		   AND DRAWING_CASE = pn_drawing_case; 
	exception
		when no_data_found then
			xv_d1 := CV$SIN_VALOR;
			xv_d2 := CV$SIN_VALOR;
			xv_d3 := CV$SIN_VALOR;
			xv_d4 := CV$SIN_VALOR;
			xv_d5 := CV$SIN_VALOR;
			xv_d6 := CV$SIN_VALOR;
			xv_decena_rank := NULL;
	end;

	if pv_show_init_values = CV$ENABLE then
	   dbms_output.put_line('xv_d1: '||xv_d1);	
	   dbms_output.put_line('xv_d2: '||xv_d2);
	   dbms_output.put_line('xv_d3: '||xv_d3);
	   dbms_output.put_line('xv_d4: '||xv_d4);
	   dbms_output.put_line('xv_d5: '||xv_d5);
	   dbms_output.put_line('xv_d6: '||xv_d6);
	   dbms_output.put_line('xv_decena_rank: '||xv_decena_rank);
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	   
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_plan_jugada_decenas;	


--!proceso para obtener rango del ciclo de aparicion del plan de jugadas
procedure get_plan_jugada_ca(pv_drawing_type                  VARCHAR2
                           , pn_drawing_case                  NUMBER
						   , pv_show_init_values				  VARCHAR2 DEFAULT 'Y'
						   --!ciclo aparicion
						   , xv_ca1         	IN OUT NOCOPY VARCHAR2
						   , xv_ca2         	IN OUT NOCOPY VARCHAR2
						   , xv_ca3         	IN OUT NOCOPY VARCHAR2
						   , xv_ca4         	IN OUT NOCOPY VARCHAR2
						   , xv_ca5         	IN OUT NOCOPY VARCHAR2
						   , xv_ca6         	IN OUT NOCOPY VARCHAR2
						   , x_err_code         IN OUT NOCOPY NUMBER
						    ) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_ca';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
 
	--!ciclo aparicion
	begin
		SELECT DECODE(POS1,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS1,'>',1,1)> 0 OR INSTR(POS1,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS1 ELSE CASE WHEN INSTR(POS1,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS1,1,INSTR(POS1,'-',1,1)-1)||' AND '||SUBSTR(POS1,INSTR(POS1,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS1||')' END END) POS1
			 , DECODE(POS2,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS2,'>',1,1)> 0 OR INSTR(POS2,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS2 ELSE CASE WHEN INSTR(POS2,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS2,1,INSTR(POS2,'-',1,1)-1)||' AND '||SUBSTR(POS2,INSTR(POS2,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS2||')' END END) POS2
			 , DECODE(POS3,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS3,'>',1,1)> 0 OR INSTR(POS3,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS3 ELSE CASE WHEN INSTR(POS3,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS3,1,INSTR(POS3,'-',1,1)-1)||' AND '||SUBSTR(POS3,INSTR(POS3,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS3||')' END END) POS3
			 , DECODE(POS4,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS4,'>',1,1)> 0 OR INSTR(POS4,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS4 ELSE CASE WHEN INSTR(POS4,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS4,1,INSTR(POS4,'-',1,1)-1)||' AND '||SUBSTR(POS4,INSTR(POS4,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS4||')' END END) POS4
			 , DECODE(POS5,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS5,'>',1,1)> 0 OR INSTR(POS5,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS5 ELSE CASE WHEN INSTR(POS5,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS5,1,INSTR(POS5,'-',1,1)-1)||' AND '||SUBSTR(POS5,INSTR(POS5,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS5||')' END END) POS5
			 , DECODE(POS6,NULL,CV$SIN_VALOR,CASE WHEN INSTR(POS6,'>',1,1)> 0 OR INSTR(POS6,'<',1,1)> 0 THEN 'CICLO_APARICION '||POS6 ELSE CASE WHEN INSTR(POS6,'-',1,1)> 0 THEN 'CICLO_APARICION BETWEEN '||SUBSTR(POS6,1,INSTR(POS6,'-',1,1)-1)||' AND '||SUBSTR(POS6,INSTR(POS6,'-',1,1)+1) ELSE 'CICLO_APARICION IN ('||POS6||')' END END) POS6
		  INTO xv_ca1
			 , xv_ca2
			 , xv_ca3
			 , xv_ca4
			 , xv_ca5
			 , xv_ca6
		  FROM OLAP_SYS.PLAN_JUGADAS
		 WHERE DRAWING_TYPE = pv_drawing_type
		   AND DESCRIPTION  = 'CICLO_APARICION'
		   AND STATUS       = 'A'
		   AND DRAWING_CASE = pn_drawing_case; 	
		   
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;		   
	exception
		when no_data_found then
			xv_ca1 := CV$SIN_VALOR;
			xv_ca2 := CV$SIN_VALOR;
			xv_ca3 := CV$SIN_VALOR;
			xv_ca4 := CV$SIN_VALOR;
			xv_ca5 := CV$SIN_VALOR;
			xv_ca6 := CV$SIN_VALOR;		
	end;
	
	if pv_show_init_values = CV$ENABLE then
	   dbms_output.put_line('xv_ca1: '||xv_ca1);	
	   dbms_output.put_line('xv_ca2: '||xv_ca2);
	   dbms_output.put_line('xv_ca3: '||xv_ca3);
	   dbms_output.put_line('xv_ca4: '||xv_ca4);
	   dbms_output.put_line('xv_ca5: '||xv_ca5);
	   dbms_output.put_line('xv_ca6: '||xv_ca6);
	end if;
	
   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION; 
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_plan_jugada_ca;


--!proceso para obtener rango de las frecuencias del plan de jugadas
procedure get_plan_jugada_rango_frec(pv_drawing_type          		  VARCHAR2
								   , pn_drawing_case                  NUMBER								   
								   --!rango de frecuencias
								   , xv_frec1         	IN OUT NOCOPY VARCHAR2
								   , xv_frec2         	IN OUT NOCOPY VARCHAR2
								   , xv_frec3         	IN OUT NOCOPY VARCHAR2
								   , xv_frec4         	IN OUT NOCOPY VARCHAR2
								   , xv_frec5         	IN OUT NOCOPY VARCHAR2
								   , xv_frec6         	IN OUT NOCOPY VARCHAR2
								   , x_err_code         IN OUT NOCOPY NUMBER
									) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_rango_frec';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
 
	--!ciclo aparicion
	begin
		SELECT CASE WHEN INSTR(POS1,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS1,1,INSTR(POS1,'-',1,1)-1)||' AND '||SUBSTR(POS1,INSTR(POS1,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS1||')' END POS1
			 , CASE WHEN INSTR(POS2,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS2,1,INSTR(POS2,'-',1,1)-1)||' AND '||SUBSTR(POS2,INSTR(POS2,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS2||')' END POS2
			 , CASE WHEN INSTR(POS3,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS3,1,INSTR(POS3,'-',1,1)-1)||' AND '||SUBSTR(POS3,INSTR(POS3,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS3||')' END POS3
			 , CASE WHEN INSTR(POS4,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS4,1,INSTR(POS4,'-',1,1)-1)||' AND '||SUBSTR(POS4,INSTR(POS4,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS4||')' END POS4
			 , CASE WHEN INSTR(POS5,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS5,1,INSTR(POS5,'-',1,1)-1)||' AND '||SUBSTR(POS5,INSTR(POS5,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS5||')' END POS5
			 , CASE WHEN INSTR(POS6,'-',1,1)> 0 THEN 'COLOR_UBICACION BETWEEN '||SUBSTR(POS6,1,INSTR(POS6,'-',1,1)-1)||' AND '||SUBSTR(POS6,INSTR(POS6,'-',1,1)+1) ELSE 'COLOR_UBICACION'||' IN ('||POS6||')' END POS6
		  INTO xv_frec1
			 , xv_frec2
			 , xv_frec3
			 , xv_frec4
			 , xv_frec5
			 , xv_frec6
		  FROM OLAP_SYS.PLAN_JUGADAS
		 WHERE DRAWING_TYPE = pv_drawing_type
		   AND DESCRIPTION  = 'RANGO_FRECUENCIA'
		   AND STATUS       = 'A'
		   AND DRAWING_CASE = pn_drawing_case; 	
		   
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;		   
	exception
		when no_data_found then
			x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
			xv_frec1 := null;
			xv_frec2 := null;
			xv_frec3 := null;
			xv_frec4 := null;
			xv_frec5 := null;
			xv_frec6 := null;		
	end;
	
   dbms_output.put_line('xv_frec1: '||xv_frec1);	
   dbms_output.put_line('xv_frec2: '||xv_frec2);
   dbms_output.put_line('xv_frec3: '||xv_frec3);
   dbms_output.put_line('xv_frec4: '||xv_frec4);
   dbms_output.put_line('xv_frec5: '||xv_frec5);
   dbms_output.put_line('xv_frec6: '||xv_frec6);
   
   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION; 
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_plan_jugada_rango_frec;


--!proceso para seleccionar los digitos que no hayan tenido cambios en la posicion
procedure get_plan_jugada_change(pv_drawing_type                  	  VARCHAR2
                               , pn_drawing_case                  	  NUMBER
							   , xv_chng_posicion_pos1	IN OUT NOCOPY VARCHAR2
							   , xv_chng_posicion_pos2	IN OUT NOCOPY VARCHAR2
							   , xv_chng_posicion_pos3	IN OUT NOCOPY VARCHAR2
							   , xv_chng_posicion_pos4	IN OUT NOCOPY VARCHAR2
							   , xv_chng_posicion_pos5	IN OUT NOCOPY VARCHAR2
							   , xv_chng_posicion_pos6	IN OUT NOCOPY VARCHAR2
							   , x_err_code         	IN OUT NOCOPY NUMBER
						        ) is

  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_change';
  lv$temp_pos1					   varchar2(30);
  lv$temp_pos2					   varchar2(30);
  lv$temp_pos3					   varchar2(30);
  lv$temp_pos4					   varchar2(30);
  lv$temp_pos5					   varchar2(30);
  lv$temp_pos6					   varchar2(30);
  
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	--!recuperando el nombre de las columnas de la tabla
	SELECT DECODE(POS1,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS1
		 , DECODE(POS2,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS2
		 , DECODE(POS3,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS3
		 , DECODE(POS4,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS4
		 , DECODE(POS5,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS5
		 , DECODE(POS6,NULL,CV$SIN_VALOR,'NULL', 'CHNG_POSICION IS NULL','NOT NULL','CHNG_UBICACION IS NOT NULL') POS6
  	  INTO xv_chng_posicion_pos1
		 , xv_chng_posicion_pos2
		 , xv_chng_posicion_pos3
		 , xv_chng_posicion_pos4
		 , xv_chng_posicion_pos5
		 , xv_chng_posicion_pos6
	  FROM OLAP_SYS.PLAN_JUGADAS
	 WHERE DRAWING_TYPE = pv_drawing_type
	   AND DESCRIPTION  = 'CAMBIO_POSICION'
	   AND STATUS       = 'A'
	   AND DRAWING_CASE = pn_drawing_case;  

   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('xv_chng_posicion_pos1: '||xv_chng_posicion_pos1);
		dbms_output.put_line('xv_chng_posicion_pos2: '||xv_chng_posicion_pos2);
		dbms_output.put_line('xv_chng_posicion_pos3: '||xv_chng_posicion_pos3);
		dbms_output.put_line('xv_chng_posicion_pos4: '||xv_chng_posicion_pos4);
		dbms_output.put_line('xv_chng_posicion_pos5: '||xv_chng_posicion_pos5);
		dbms_output.put_line('xv_chng_posicion_pos6: '||xv_chng_posicion_pos6);
   end if;
   
   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when no_data_found then
	   xv_chng_posicion_pos1 := CV$SIN_VALOR;
	   xv_chng_posicion_pos2 := CV$SIN_VALOR;
	   xv_chng_posicion_pos3 := CV$SIN_VALOR;
	   xv_chng_posicion_pos4 := CV$SIN_VALOR;
	   xv_chng_posicion_pos5 := CV$SIN_VALOR;
	   xv_chng_posicion_pos6 := CV$SIN_VALOR; 
	   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_plan_jugada_change;


--!proceso para recuperar el patron de frecuencias del plan de jugadas
procedure get_plan_jugada_frec(pv_drawing_type                  VARCHAR2
						     , pn_drawing_case                  NUMBER
						     --!frecuencia
							 , xv_conf_frec1    IN OUT NOCOPY VARCHAR2
							 , xv_conf_frec2    IN OUT NOCOPY VARCHAR2
							 , xv_conf_frec3    IN OUT NOCOPY VARCHAR2
							 , xv_conf_frec4    IN OUT NOCOPY VARCHAR2
							 , xv_conf_frec5    IN OUT NOCOPY VARCHAR2
							 , xv_conf_frec6    IN OUT NOCOPY VARCHAR2
						     , x_err_code         IN OUT NOCOPY NUMBER
							  ) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_frec'; 
  le$patron_frecuencia_invalido        exception;
  cursor c_frecuencias (pv_drawing_type                  VARCHAR2
					  , pn_drawing_case                  NUMBER) is
  	SELECT DECODE(POS1,'R',1,'G',2,'B',3) POS1
		 , DECODE(POS2,'R',1,'G',2,'B',3) POS2
		 , DECODE(POS3,'R',1,'G',2,'B',3) POS3
		 , DECODE(POS4,'R',1,'G',2,'B',3) POS4
		 , DECODE(POS5,'R',1,'G',2,'B',3) POS5
		 , DECODE(POS6,'R',1,'G',2,'B',3) POS6
	  FROM OLAP_SYS.PLAN_JUGADAS
	 WHERE DRAWING_TYPE = pv_drawing_type
	   AND DESCRIPTION  = 'FRECUENCIA'
	   AND STATUS       = 'A'
	   AND DRAWING_CASE = pn_drawing_case;  
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	--!inicializando variables
	gv$valor_pos1 := null;
	gv$valor_pos2 := null;
	gv$valor_pos3 := null;
	gv$valor_pos4 := null;
	gv$valor_pos5 := null;
	gv$valor_pos6 := null;
	
	open c_frecuencias (pv_drawing_type => pv_drawing_type
						 , pn_drawing_case => pn_drawing_case);
	loop
		fetch c_frecuencias into gv$fetch_pos1, gv$fetch_pos2, gv$fetch_pos3, gv$fetch_pos4, gv$fetch_pos5, gv$fetch_pos6;			 
		exit when c_frecuencias%notfound;		
		if gv$fetch_pos1 is not null then
			gv$valor_pos1 := gv$valor_pos1||gv$fetch_pos1||',';
		end if;	
		if gv$fetch_pos2 is not null then
			gv$valor_pos2 := gv$valor_pos2||gv$fetch_pos2||',';
		end if;	
		if gv$fetch_pos3 is not null then
			gv$valor_pos3 := gv$valor_pos3||gv$fetch_pos3||',';
		end if;
		if gv$fetch_pos4 is not null then
			gv$valor_pos4 := gv$valor_pos4||gv$fetch_pos4||',';
		end if;
		if gv$fetch_pos5 is not null then
			gv$valor_pos5 := gv$valor_pos5||gv$fetch_pos5||',';
		end if;
		if gv$fetch_pos6 is not null then
			gv$valor_pos6 := gv$valor_pos6||gv$fetch_pos6||',';
		end if;	
	end loop;
	close c_frecuencias;

	--!removiendo ultima coma
	gv$valor_pos1 := substr(gv$valor_pos1,1,length(gv$valor_pos1)-1);
	gv$valor_pos2 := substr(gv$valor_pos2,1,length(gv$valor_pos2)-1);
	gv$valor_pos3 := substr(gv$valor_pos3,1,length(gv$valor_pos3)-1);
	gv$valor_pos4 := substr(gv$valor_pos4,1,length(gv$valor_pos4)-1);
	gv$valor_pos5 := substr(gv$valor_pos5,1,length(gv$valor_pos5)-1);
	gv$valor_pos6 := substr(gv$valor_pos6,1,length(gv$valor_pos6)-1);

    --!regresando valores distintos de las frecuencias
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos1, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec1);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos2, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec2);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos3, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec3);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos4, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec4);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos5, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec5);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos6, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_conf_frec6);

   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('frecuencia1: '||xv_conf_frec1);		
		dbms_output.put_line('frecuencia2: '||xv_conf_frec2);	
		dbms_output.put_line('frecuencia3: '||xv_conf_frec3);	
		dbms_output.put_line('frecuencia4: '||xv_conf_frec4);	
		dbms_output.put_line('frecuencia5: '||xv_conf_frec5);	
		dbms_output.put_line('frecuencia6: '||xv_conf_frec6);			
   end if;

   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when no_data_found then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line('Es necesario ingresar el patron de frecuencia a jugar en el sorteo');     
	raise;  
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	close c_frecuencias;
	xv_conf_frec1 := null;
	xv_conf_frec2 := null;
	xv_conf_frec3 := null;
	xv_conf_frec4 := null;
	xv_conf_frec5 := null;
	xv_conf_frec6 := null;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_plan_jugada_frec;


--!proceso para recuperar el patron de numeros del plan de jugadas
procedure get_plan_jugada_patron_numeros(pv_drawing_type                  VARCHAR2
									   , pn_drawing_case                  NUMBER
									   --!patrones de numeros
									   , xv_patron1    		IN OUT NOCOPY VARCHAR2
									   , xv_patron2    		IN OUT NOCOPY VARCHAR2
									   , xv_patron3    		IN OUT NOCOPY VARCHAR2
									   , xv_patron4    		IN OUT NOCOPY VARCHAR2
									   , xv_patron5    		IN OUT NOCOPY VARCHAR2
									   , xv_patron6    		IN OUT NOCOPY VARCHAR2
									   , x_err_code         IN OUT NOCOPY NUMBER
									    ) is

  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_plan_jugada_patron_numeros'; 							  
	   
	cursor c_patron_numeros (pv_drawing_type             	  VARCHAR2
					       , pn_drawing_case             	  NUMBER) is 
	SELECT POS1
		 , POS2
		 , POS3
		 , POS4
		 , POS5
		 , POS6				 
	  FROM OLAP_SYS.PLAN_JUGADAS
	 WHERE DRAWING_TYPE = pv_drawing_type
	   AND DESCRIPTION  = 'PATRON_NUMEROS'
	   AND STATUS       = 'A'
	   AND DRAWING_CASE = pn_drawing_case;
begin
	--!limpiando variables
	gv$valor_pos1 := null;
	gv$valor_pos2 := null;
	gv$valor_pos3 := null;
	gv$valor_pos4 := null;
	gv$valor_pos5 := null;
	gv$valor_pos6 := null;	
	open c_patron_numeros (pv_drawing_type => pv_drawing_type
						 , pn_drawing_case => pn_drawing_case);
	loop
		fetch c_patron_numeros into gv$fetch_pos1, gv$fetch_pos2, gv$fetch_pos3, gv$fetch_pos4, gv$fetch_pos5, gv$fetch_pos6;			 
		exit when c_patron_numeros%notfound;		

		if gv$fetch_pos1 is not null then
			gv$valor_pos1 := gv$valor_pos1||gv$fetch_pos1||',';
		end if;	
		if gv$fetch_pos2 is not null then
			gv$valor_pos2 := gv$valor_pos2||gv$fetch_pos2||',';
		end if;	
		if gv$fetch_pos3 is not null then
			gv$valor_pos3 := gv$valor_pos3||gv$fetch_pos3||',';
		end if;
		if gv$fetch_pos4 is not null then
			gv$valor_pos4 := gv$valor_pos4||gv$fetch_pos4||',';
		end if;
		if gv$fetch_pos5 is not null then
			gv$valor_pos5 := gv$valor_pos5||gv$fetch_pos5||',';
		end if;
		if gv$fetch_pos6 is not null then
			gv$valor_pos6 := gv$valor_pos6||gv$fetch_pos6||',';
		end if;	
	end loop;
	close c_patron_numeros;

	--!removiendo ultima coma
	gv$valor_pos1 := substr(gv$valor_pos1,1,length(gv$valor_pos1)-1);
	gv$valor_pos2 := substr(gv$valor_pos2,1,length(gv$valor_pos2)-1);
	gv$valor_pos3 := substr(gv$valor_pos3,1,length(gv$valor_pos3)-1);
	gv$valor_pos4 := substr(gv$valor_pos4,1,length(gv$valor_pos4)-1);
	gv$valor_pos5 := substr(gv$valor_pos5,1,length(gv$valor_pos5)-1);
	gv$valor_pos6 := substr(gv$valor_pos6,1,length(gv$valor_pos6)-1);


	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos1, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron1);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos2, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron2);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos3, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron3);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos4, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron4);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos5, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron5);
	olap_sys.w_common_pkg.get_distinct_values_from_list(pv_string => gv$valor_pos6, pv_data_type => 'NUMBER', xv_distinct_value_list => xv_patron6);

	dbms_output.put_line('configuracion patron numero');
	dbms_output.put_line('patron numero1: '||xv_patron1);		
	dbms_output.put_line('patron numero2: '||xv_patron2);	
	dbms_output.put_line('patron numero3: '||xv_patron3);	
	dbms_output.put_line('patron numero4: '||xv_patron4);	
	dbms_output.put_line('patron numero5: '||xv_patron5);	
	dbms_output.put_line('patron numero6: '||xv_patron6);	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when no_data_found then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
		close c_patron_numeros;
		xv_patron1 := null;
		xv_patron2 := null;
		xv_patron3 := null;
		xv_patron4 := null;
		xv_patron5 := null;
		xv_patron6 := null;
  when others then
    close c_patron_numeros;
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_plan_jugada_patron_numeros;


--!imprimiendo los arreglos de GL para cada posicion
procedure imprimir_arreglos (ptbl$list_array_pos1              	gt$gl_tbl
                           , ptbl$list_array_pos2              	gt$gl_tbl
                           , ptbl$list_array_pos3              	gt$gl_tbl
                           , ptbl$list_array_pos4              	gt$gl_tbl
                           , ptbl$list_array_pos5              	gt$gl_tbl
                           , ptbl$list_array_pos6              	gt$gl_tbl
					       , x_err_code    IN OUT NOCOPY NUMBER
                            ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'imprimir_arreglos';
  le$no_data_found                 exception;
  lv$b_type                         varchar2(2);
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	--!imprimiendo los arreglos de GL para cada posicion
	dbms_output.put_line('---------');
	dbms_output.put_line('Imprimiendo los arreglos de GL para cada posicion');
	dbms_output.put_line('B1');
	if ptbl$list_array_pos1.count > 0 then
		for a in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos1(a).digit||' , lt: '||ptbl$list_array_pos1(a).lt||' , rlt: '||ptbl$list_array_pos1(a).rlt||' , ca: '||ptbl$list_array_pos1(a).ca||' , pxc: '||ptbl$list_array_pos1(a).pxc||' , pr: '||ptbl$list_array_pos1(a).pr||' , non: '||ptbl$list_array_pos1(a).non||' , pref: '||ptbl$list_array_pos1(a).preferencia_flag||' , flag: '||ptbl$list_array_pos1(a).change_flag);
		end loop;
	else
		lv$b_type := 'B1';
		raise le$no_data_found;
	end if;
	
	dbms_output.put_line('---------');
	dbms_output.put_line('B2');
	if ptbl$list_array_pos2.count > 0 then
		for a in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos2(a).digit||' , lt: '||ptbl$list_array_pos2(a).lt||' , rlt: '||ptbl$list_array_pos2(a).rlt||' , ca: '||ptbl$list_array_pos2(a).ca||' , pxc: '||ptbl$list_array_pos2(a).pxc||' , pr: '||ptbl$list_array_pos2(a).pr||' , non: '||ptbl$list_array_pos2(a).non||' , pref: '||ptbl$list_array_pos2(a).preferencia_flag||' , flag: '||ptbl$list_array_pos2(a).change_flag);
		end loop;
	else
		lv$b_type := 'B2';
		raise le$no_data_found;
	end if;
	
	dbms_output.put_line('---------');
	dbms_output.put_line('B3');
	if ptbl$list_array_pos3.count > 0 then
		for a in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos3(a).digit||' , lt: '||ptbl$list_array_pos3(a).lt||' , rlt: '||ptbl$list_array_pos3(a).rlt||' , ca: '||ptbl$list_array_pos3(a).ca||' , pxc: '||ptbl$list_array_pos3(a).pxc||' , pr: '||ptbl$list_array_pos3(a).pr||' , non: '||ptbl$list_array_pos3(a).non||' , pref: '||ptbl$list_array_pos3(a).preferencia_flag||' , flag: '||ptbl$list_array_pos3(a).change_flag);
		end loop;
	else
		lv$b_type := 'B3';
		raise le$no_data_found;
	end if;
	
	dbms_output.put_line('---------');
	dbms_output.put_line('B4');
	if ptbl$list_array_pos4.count > 0 then
		for a in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos4(a).digit||' , lt: '||ptbl$list_array_pos4(a).lt||' , rlt: '||ptbl$list_array_pos4(a).rlt||' , ca: '||ptbl$list_array_pos4(a).ca||' , pxc: '||ptbl$list_array_pos4(a).pxc||' , pr: '||ptbl$list_array_pos4(a).pr||' , non: '||ptbl$list_array_pos4(a).non||' , pref: '||ptbl$list_array_pos4(a).preferencia_flag||' , flag: '||ptbl$list_array_pos4(a).change_flag);
		end loop;
	else
		lv$b_type := 'B4';
		raise le$no_data_found;
	end if;

	dbms_output.put_line('---------');
	dbms_output.put_line('B5');
	if ptbl$list_array_pos5.count > 0 then	
		for a in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos5(a).digit||' , lt: '||ptbl$list_array_pos5(a).lt||' , rlt: '||ptbl$list_array_pos5(a).rlt||' , ca: '||ptbl$list_array_pos5(a).ca||' , pxc: '||ptbl$list_array_pos5(a).pxc||' , pr: '||ptbl$list_array_pos5(a).pr||' , non: '||ptbl$list_array_pos5(a).non||' , pref: '||ptbl$list_array_pos5(a).preferencia_flag||' , flag: '||ptbl$list_array_pos5(a).change_flag);
		end loop;
	else
		lv$b_type := 'B5';
		raise le$no_data_found;
	end if;
	
	dbms_output.put_line('---------');
	dbms_output.put_line('B6');
	if ptbl$list_array_pos6.count > 0 then
		for a in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
			dbms_output.put_line('digit: '||ptbl$list_array_pos6(a).digit||' , lt: '||ptbl$list_array_pos6(a).lt||' , rlt: '||ptbl$list_array_pos6(a).rlt||' , ca: '||ptbl$list_array_pos6(a).ca||' , pxc: '||ptbl$list_array_pos6(a).pxc||' , pr: '||ptbl$list_array_pos6(a).pr||' , non: '||ptbl$list_array_pos6(a).non||' , pref: '||ptbl$list_array_pos6(a).preferencia_flag||' , flag: '||ptbl$list_array_pos6(a).change_flag);
		end loop;	
	else
		lv$b_type := 'B6';
		raise le$no_data_found;
	end if;	
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when le$no_data_found then
	dbms_output.put_line('[panorama] Valores nos encontrados en posicion '||lv$b_type);
--	raise;	
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end imprimir_arreglos;	


--!convertir array de digitos en string separado por comas para cada posicion
procedure array_digitos_to_coma_string (ptbl$list_array_pos1              	gt$gl_tbl
									  , ptbl$list_array_pos2              	gt$gl_tbl
									  , ptbl$list_array_pos3              	gt$gl_tbl
									  , ptbl$list_array_pos4              	gt$gl_tbl
									  , ptbl$list_array_pos5              	gt$gl_tbl
									  , ptbl$list_array_pos6              	gt$gl_tbl
									  --!listas finales de numeros								
									  , xv_digit_list_pos1	  IN OUT NOCOPY VARCHAR2
									  , xv_digit_list_pos2	  IN OUT NOCOPY VARCHAR2
									  , xv_digit_list_pos3	  IN OUT NOCOPY VARCHAR2
									  , xv_digit_list_pos4	  IN OUT NOCOPY VARCHAR2
									  , xv_digit_list_pos5	  IN OUT NOCOPY VARCHAR2
									  , xv_digit_list_pos6	  IN OUT NOCOPY VARCHAR2						  
									  , x_err_code    IN OUT NOCOPY NUMBER
									   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'array_digitos_to_coma_string';
  lv$temporal_list					varchar2(1000);
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('ptbl$list_array_pos1.count: '||ptbl$list_array_pos1.count);
		dbms_output.put_line('ptbl$list_array_pos2.count: '||ptbl$list_array_pos2.count);
		dbms_output.put_line('ptbl$list_array_pos3.count: '||ptbl$list_array_pos3.count);
		dbms_output.put_line('ptbl$list_array_pos4.count: '||ptbl$list_array_pos4.count);
		dbms_output.put_line('ptbl$list_array_pos5.count: '||ptbl$list_array_pos5.count);
		dbms_output.put_line('ptbl$list_array_pos6.count: '||ptbl$list_array_pos6.count);
   end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	--!filtrando los arreglos de GL para cada posicion
	--dbms_output.put_line('---------');
	--dbms_output.put_line('Filtrando los arreglos de GL para cada posicion');
	if ptbl$list_array_pos1.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos1 := null;
		for a in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop		
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos1(a).digit||' , lt: '||ptbl$list_array_pos1(a).lt||' , rlt: '||ptbl$list_array_pos1(a).rlt||' , ca: '||ptbl$list_array_pos1(a).ca||' , pxc: '||ptbl$list_array_pos1(a).pxc||' , pr: '||ptbl$list_array_pos1(a).pr||' , non: '||ptbl$list_array_pos1(a).non||' , pref: '||ptbl$list_array_pos1(a).preferencia_flag||' , flag: '||ptbl$list_array_pos1(a).change_flag);
			end if;
			xv_digit_list_pos1 := xv_digit_list_pos1||ptbl$list_array_pos1(a).digit||',';
		end loop;
		lv$temporal_list   := xv_digit_list_pos1;		
		xv_digit_list_pos1 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos1
															);
		xv_digit_list_pos1 := 'IN ('||xv_digit_list_pos1||')';
		dbms_output.put_line('AND COMB1 '||xv_digit_list_pos1);
	else
--		dbms_output.put_line('B1. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos1 := NULL;		
	end if;
	
--	dbms_output.put_line('---------');
	if ptbl$list_array_pos2.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos2 := null;
		for a in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop					
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos2(a).digit||' , lt: '||ptbl$list_array_pos2(a).lt||' , rlt: '||ptbl$list_array_pos2(a).rlt||' , ca: '||ptbl$list_array_pos2(a).ca||' , pxc: '||ptbl$list_array_pos2(a).pxc||' , pr: '||ptbl$list_array_pos2(a).pr||' , non: '||ptbl$list_array_pos2(a).non||' , pref: '||ptbl$list_array_pos2(a).preferencia_flag||' , flag: '||ptbl$list_array_pos2(a).change_flag);
			end if;
			xv_digit_list_pos2 := xv_digit_list_pos2||ptbl$list_array_pos2(a).digit||',';		
		end loop;
		lv$temporal_list   := xv_digit_list_pos2;
		xv_digit_list_pos2 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos2
															);
		xv_digit_list_pos2 := 'IN ('||xv_digit_list_pos2||')';
		dbms_output.put_line('AND COMB2 '||xv_digit_list_pos2);
	else
--		dbms_output.put_line('B2. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos2 := NULL;		
	end if;
	
--	dbms_output.put_line('---------');
	if ptbl$list_array_pos3.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos3 := null;
		for a in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop		
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos3(a).digit||' , lt: '||ptbl$list_array_pos3(a).lt||' , rlt: '||ptbl$list_array_pos3(a).rlt||' , ca: '||ptbl$list_array_pos3(a).ca||' , pxc: '||ptbl$list_array_pos3(a).pxc||' , pr: '||ptbl$list_array_pos3(a).pr||' , non: '||ptbl$list_array_pos3(a).non||' , pref: '||ptbl$list_array_pos3(a).preferencia_flag||' , flag: '||ptbl$list_array_pos3(a).change_flag);
			end if;
			xv_digit_list_pos3 := xv_digit_list_pos3||ptbl$list_array_pos3(a).digit||',';		
		end loop;
		lv$temporal_list   := xv_digit_list_pos3;
		xv_digit_list_pos3 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos3
															);
		xv_digit_list_pos3 := 'IN ('||xv_digit_list_pos3||')';	
		dbms_output.put_line('AND COMB3 '||xv_digit_list_pos3);
	else
--		dbms_output.put_line('B3. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos3 := NULL;
	end if;
	
--	dbms_output.put_line('---------');
	if ptbl$list_array_pos4.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos4 := null;
		for a in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop								
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos4(a).digit||' , lt: '||ptbl$list_array_pos4(a).lt||' , rlt: '||ptbl$list_array_pos4(a).rlt||' , ca: '||ptbl$list_array_pos4(a).ca||' , pxc: '||ptbl$list_array_pos4(a).pxc||' , pr: '||ptbl$list_array_pos4(a).pr||' , non: '||ptbl$list_array_pos4(a).non||' , pref: '||ptbl$list_array_pos4(a).preferencia_flag||' , flag: '||ptbl$list_array_pos4(a).change_flag);			
			end if;
			xv_digit_list_pos4 := xv_digit_list_pos4||ptbl$list_array_pos4(a).digit||',';		
		end loop;
		lv$temporal_list   := xv_digit_list_pos4;
		xv_digit_list_pos4 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos4
															);
		xv_digit_list_pos4 := 'IN ('||xv_digit_list_pos4||')';
		dbms_output.put_line('AND COMB4 '||xv_digit_list_pos4);
	else
--		dbms_output.put_line('B4. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos4 := NULL;		
	end if;

--	dbms_output.put_line('---------');
	if ptbl$list_array_pos5.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos5 := null;
		for a in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop								
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos5(a).digit||' , lt: '||ptbl$list_array_pos5(a).lt||' , rlt: '||ptbl$list_array_pos5(a).rlt||' , ca: '||ptbl$list_array_pos5(a).ca||' , pxc: '||ptbl$list_array_pos5(a).pxc||' , pr: '||ptbl$list_array_pos5(a).pr||' , non: '||ptbl$list_array_pos5(a).non||' , pref: '||ptbl$list_array_pos5(a).preferencia_flag||' , flag: '||ptbl$list_array_pos5(a).change_flag);			
			end if;
			xv_digit_list_pos5 := xv_digit_list_pos5||ptbl$list_array_pos5(a).digit||',';		
		end loop;
		lv$temporal_list   := xv_digit_list_pos5;
		xv_digit_list_pos5 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos5
															);
		xv_digit_list_pos5 := 'IN ('||xv_digit_list_pos5||')';
		dbms_output.put_line('AND COMB5 '||xv_digit_list_pos5);
	else
--		dbms_output.put_line('B5. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos5 := NULL;		
	end if;
	
--	dbms_output.put_line('---------');
	if ptbl$list_array_pos6.count > 0 then
		lv$temporal_list := null;
		xv_digit_list_pos6 := null;
		for a in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop										
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('digit: '||ptbl$list_array_pos6(a).digit||' , lt: '||ptbl$list_array_pos6(a).lt||' , rlt: '||ptbl$list_array_pos6(a).rlt||' , ca: '||ptbl$list_array_pos6(a).ca||' , pxc: '||ptbl$list_array_pos6(a).pxc||' , pr: '||ptbl$list_array_pos6(a).pr||' , non: '||ptbl$list_array_pos6(a).non||' , pref: '||ptbl$list_array_pos6(a).preferencia_flag||' , flag: '||ptbl$list_array_pos6(a).change_flag);
			end if;
			xv_digit_list_pos6 := xv_digit_list_pos6||ptbl$list_array_pos6(a).digit||',';			
		end loop;
		lv$temporal_list   := xv_digit_list_pos6;
		xv_digit_list_pos6 := null;
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string              => substr(lv$temporal_list,1,length(lv$temporal_list)-1)
														   , pv_data_type           => 'NUMBER'
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => xv_digit_list_pos6
															);
		xv_digit_list_pos6 := 'IN ('||xv_digit_list_pos6||')';
		dbms_output.put_line('AND COMB6 '||xv_digit_list_pos6);	
	else
--		dbms_output.put_line('B6. Valores nos encontrados para esta posicion');
		x_err_code := x_err_code + 1;
		xv_digit_list_pos6 := NULL;
	end if;	
	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end array_digitos_to_coma_string;


--!validar patrones de numeros en el arreglo en base a parametro de entrada
procedure validar_patrones_numeros (pv_patron    	 					VARCHAR2
						          , pv_replace_add_flag                 VARCHAR2 
								  , xtbl$list_array_pos   IN OUT NOCOPY gt$gl_tbl
					              , x_err_code    		  IN OUT NOCOPY NUMBER
                                   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_patrones_numeros';
  ln$ultimo_registro               number := 0;
  le$patron_numero_invalido        exception;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	if xtbl$list_array_pos.count > 0 then
		--!reemplazar el arreglo de numeros con el patron de numeros
		if pv_replace_add_flag = 'R' then
			xtbl$list_array_pos.delete;
			xtbl$list_array_pos(1).digit := to_number(pv_patron);
			xtbl$list_array_pos(1).change_flag  := 'Y'; 
		--!agregar el patron de numeros al arreglo de numeros 
		elsif pv_replace_add_flag = 'A' then
		    ln$ultimo_registro := xtbl$list_array_pos.count+1;
			
			--!validar si existe el patron en el arreglo
			olap_sys.w_common_pkg.g_data_found := 0;
			for m in xtbl$list_array_pos.first..xtbl$list_array_pos.last loop
				if xtbl$list_array_pos(m).digit = to_number(pv_patron) then
					olap_sys.w_common_pkg.g_data_found := 1;
					ln$ultimo_registro                 := m;
				end if;
			end loop;
			
			--!si el patron no existe se agrega
			if olap_sys.w_common_pkg.g_data_found = 0 then
				xtbl$list_array_pos(ln$ultimo_registro).digit := to_number(pv_patron);
				xtbl$list_array_pos(ln$ultimo_registro).change_flag  := 'Y';
			--!si el patron ya existe se activa
			else
				xtbl$list_array_pos(ln$ultimo_registro).change_flag  := 'Y';			
			end if;
		else
			raise le$patron_numero_invalido;
		end if;
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when le$patron_numero_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'Valores validos son R (Reemplazar) / A (Agregar)');     
	raise;  
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end validar_patrones_numeros;


--!validar ley del tercio log en el arreglo en base a parametro de entrada
procedure validar_ley_tercio_log (pv_conf_ltl    	 					VARCHAR2
						        , xtbl$list_array_pos   IN OUT NOCOPY gt$gl_tbl
					            , x_err_code    		  IN OUT NOCOPY NUMBER
                                 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_ley_tercio_log';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	if xtbl$list_array_pos.count > 0 then
		for k in xtbl$list_array_pos.first..xtbl$list_array_pos.last loop
			if xtbl$list_array_pos(k).rlt != pv_conf_ltl then
				xtbl$list_array_pos(k).change_flag := 'N';
			end if;
		end loop;
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end validar_ley_tercio_log;


--!validar multiplos en el arreglo en base a parametro de entrada
procedure validar_multiplo (pn_multiplo                         NUMBER
						  , xtbl$list_array_pos   IN OUT NOCOPY gt$gl_tbl
					      , x_err_code    		  IN OUT NOCOPY NUMBER
                           ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_multiplo';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	if xtbl$list_array_pos.count > 0 then
		for k in xtbl$list_array_pos.first..xtbl$list_array_pos.last loop
			if mod(xtbl$list_array_pos(k).digit,pn_multiplo) > 0 then
				xtbl$list_array_pos(k).change_flag := 'N';
			end if;
		end loop;
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end validar_multiplo;	


--!validar los arreglos de los numeros para obtener la lista final de numeros
procedure validar_arreglos_numeros (pn_drawing_case             		NUMBER
                                , pv_select_list                    	VARCHAR2
								, pv_replace_add_flag                   VARCHAR2
							    --!contador de primos, pares y nones
							    , pn_primo_cnt    	 					NUMBER
							    , pn_par_cnt      	 					NUMBER
							    , pn_non_cnt      	 					NUMBER
							    --!terminaciones duplicadas
							    , pn_term_cnt       					NUMBER
							    --!configuracion de primos, pares y nones
							    , pv_conf_ppn1    	 					VARCHAR2
							    , pv_conf_ppn2    	 					VARCHAR2
							    , pv_conf_ppn3      					VARCHAR2
							    , pv_conf_ppn4      					VARCHAR2
							    , pv_conf_ppn5      					VARCHAR2
							    , pv_conf_ppn6      					VARCHAR2
							    --!multiplos de 3
							    , pv_conf_m3_1    	 					VARCHAR2
							    , pv_conf_m3_2    	 					VARCHAR2
							    , pv_conf_m3_3      					VARCHAR2
							    , pv_conf_m3_4      					VARCHAR2
							    , pv_conf_m3_5      					VARCHAR2
							    , pv_conf_m3_6      					VARCHAR2
							    --!multiplos de 4
							    , pv_conf_m4_1    	 					VARCHAR2
							    , pv_conf_m4_2    	 					VARCHAR2
							    , pv_conf_m4_3      					VARCHAR2
							    , pv_conf_m4_4      					VARCHAR2
							    , pv_conf_m4_5      					VARCHAR2
							    , pv_conf_m4_6      					VARCHAR2
							    --!multiplos de 5
							    , pv_conf_m5_1    	 					VARCHAR2
							    , pv_conf_m5_2    	 					VARCHAR2
							    , pv_conf_m5_3      					VARCHAR2
							    , pv_conf_m5_4      					VARCHAR2
							    , pv_conf_m5_5      					VARCHAR2
							    , pv_conf_m5_6      					VARCHAR2
							    --!multiplos de 7
							    , pv_conf_m7_1    	 					VARCHAR2
							    , pv_conf_m7_2    	 					VARCHAR2
							    , pv_conf_m7_3      					VARCHAR2
							    , pv_conf_m7_4      					VARCHAR2
							    , pv_conf_m7_5      					VARCHAR2
							    , pv_conf_m7_6      					VARCHAR2		
							    --!ley del tercio log
							    , pv_conf_ltl1    	 					VARCHAR2
							    , pv_conf_ltl2    	 					VARCHAR2
							    , pv_conf_ltl3      					VARCHAR2
							    , pv_conf_ltl4      					VARCHAR2
							    , pv_conf_ltl5      					VARCHAR2
							    , pv_conf_ltl6      					VARCHAR2
								--!arreglos con la info de cada posicion
							    , xtbl$list_array_pos1    IN OUT NOCOPY gt$gl_tbl
                                , xtbl$list_array_pos2    IN OUT NOCOPY gt$gl_tbl
                                , xtbl$list_array_pos3    IN OUT NOCOPY gt$gl_tbl
                                , xtbl$list_array_pos4    IN OUT NOCOPY gt$gl_tbl
                                , xtbl$list_array_pos5    IN OUT NOCOPY gt$gl_tbl
                                , xtbl$list_array_pos6    IN OUT NOCOPY gt$gl_tbl
                                --!listas finales de numeros								
                                , xv_digit_list_pos1	  IN OUT NOCOPY VARCHAR2
                                , xv_digit_list_pos2	  IN OUT NOCOPY VARCHAR2
                                , xv_digit_list_pos3	  IN OUT NOCOPY VARCHAR2
                                , xv_digit_list_pos4	  IN OUT NOCOPY VARCHAR2
                                , xv_digit_list_pos5	  IN OUT NOCOPY VARCHAR2
                                , xv_digit_list_pos6	  IN OUT NOCOPY VARCHAR2
					            , x_err_code    		  IN OUT NOCOPY NUMBER
                                 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_arreglos_numeros';
  lb$valida_pares_nones            boolean := true;
  lb$valida_conf_pares_nones       boolean := true;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
	
	--!Imprimiendo los arreglos de GL para cada posicion
	imprimir_arreglos (ptbl$list_array_pos1 => xtbl$list_array_pos1
                     , ptbl$list_array_pos2 => xtbl$list_array_pos2
                     , ptbl$list_array_pos3 => xtbl$list_array_pos3
                     , ptbl$list_array_pos4 => xtbl$list_array_pos4
                     , ptbl$list_array_pos5 => xtbl$list_array_pos5
                     , ptbl$list_array_pos6 => xtbl$list_array_pos6
					 , x_err_code           => x_err_code
                      );
   end if;
   
	if pv_conf_ppn1 is null and pv_conf_ppn2 is null and pv_conf_ppn3 is null and pv_conf_ppn4 is null and pv_conf_ppn5 is null and pv_conf_ppn6 is null then
		 lb$valida_pares_nones := false;
	end if;
	
	if lb$valida_pares_nones then
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('pv_select_list: '||pv_select_list);
		end if;	
		
		gv$tmp_list := pv_conf_ppn1||','||pv_conf_ppn2||','||pv_conf_ppn3||','||pv_conf_ppn4||','||pv_conf_ppn5||','||pv_conf_ppn6;
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('CONFIG_PRIMOS_PARES_NONE: '||gv$tmp_list);
		end if;	
		
		--!validando la posicion de los numeros primos en el select list
		olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => gv$tmp_list
													 , xtbl_row   => gtbl$row_source 
													 , x_err_code => x_err_code
													  );    

		olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => pv_select_list
													 , xtbl_row   => gtbl$row_target 
													 , x_err_code => x_err_code
													  );  

		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('---------');
			dbms_output.put_line('gtbl$row_source.count: '||gtbl$row_source.count); 
			dbms_output.put_line('gtbl$row_target.count: '||gtbl$row_target.count); 
		end if;
		
		--!validando elementos en CONFIG_PRIMOS_PARES_NONES vs SELECT LIST   
		if gtbl$row_source.count >0 and gtbl$row_target.count > 0 then
			if gtbl$row_source.count != gtbl$row_target.count then
			   raise ge$select_list_len;
			end if;
		end if;
	  
		--!validando posicion y cantidad de numeros primos
		for i in gtbl$row_source.first..gtbl$row_source.last loop
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line(gtbl$row_target(i)||' # '||gtbl$row_source(i));
			end if;
			if instr(gtbl$row_target(i),gtbl$row_source(i)) > 0 then
			   gn$primo_cnt := gn$primo_cnt + 1;
			end if;  
		end loop;

		if GB$SHOW_PROC_NAME then		
			dbms_output.put_line('---------');
			dbms_output.put_line('gn$primo_cnt: '||gn$primo_cnt||'  #  pn_primo_cnt: '||pn_primo_cnt);
		end if;
		
		if gn$primo_cnt != pn_primo_cnt then
			raise ge$numeros_primos_no_match;
		end if;

		--!validando cantidad de numeros pares y nones
		for i in gtbl$row_source.first..gtbl$row_source.last loop
			if gtbl$row_source(i) = CV$NUMERO_PAR then
				gn$par_cnt := gn$par_cnt + 1;       
			end if;  
			
			if gtbl$row_source(i) = CV$NUMERO_NON then
				gn$non_cnt := gn$non_cnt + 1;           
			end if;  		
		end loop;   
    end if;

/*	validacion ya incluida en la ejecucion del query
	dbms_output.put_line('---------');
	dbms_output.put_line('gn$par_cnt: '||gn$par_cnt||'  #  pn_par_cnt: '||pn_par_cnt); 
	dbms_output.put_line('gn$non_cnt: '||gn$non_cnt||'  #  pn_non_cnt: '||pn_non_cnt);
	if gn$par_cnt != pn_par_cnt and gn$non_cnt != pn_non_cnt then
       raise ge$pares_nones_match;
    end if;
*/

    --!validando multiplos de 3
	if pv_conf_m3_1 is null and 
	   pv_conf_m3_2 is null and 
	   pv_conf_m3_3 is null and 
	   pv_conf_m3_4 is null and 
	   pv_conf_m3_5 is null and 
	   pv_conf_m3_6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de multiplos de 3'); 
	else
	    --!validar multiplos en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_conf_m3_1 is not null then
			validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
							, xtbl$list_array_pos => xtbl$list_array_pos1
							, x_err_code          => x_err_code
							 );
		end if;
		
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_conf_m3_2 is not null then
				validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
								, xtbl$list_array_pos => xtbl$list_array_pos2
								, x_err_code          => x_err_code
								 );		
			end if;

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_conf_m3_3 is not null then
					validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
									, xtbl$list_array_pos => xtbl$list_array_pos3
									, x_err_code          => x_err_code
									 );		
				end if;				 

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_conf_m3_4 is not null then
						validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
										, xtbl$list_array_pos => xtbl$list_array_pos4
										, x_err_code          => x_err_code
										 );		
					end if;
					
					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_conf_m3_5 is not null then
							validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
											, xtbl$list_array_pos => xtbl$list_array_pos5
											, x_err_code          => x_err_code
											 );		
						end if;				 

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_conf_m3_6 is not null then
								validar_multiplo (pn_multiplo         => CN$MULTIPLO_3
												, xtbl$list_array_pos => xtbl$list_array_pos6
												, x_err_code          => x_err_code
												 );		
							end if;				 
						end if;		
					end if;						
				end if;						
			end if;
		end if;
	end if;
  
    --!validando multiplos de 4
	if pv_conf_m4_1 is null and 
	   pv_conf_m4_2 is null and 
	   pv_conf_m4_3 is null and 
	   pv_conf_m4_4 is null and 
	   pv_conf_m4_5 is null and 
	   pv_conf_m4_6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de multiplos de 4'); 
	else
	    --!validar multiplos en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_conf_m4_1 is not null then
			validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
							, xtbl$list_array_pos => xtbl$list_array_pos1
							, x_err_code          => x_err_code
							 );
		end if;
		
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_conf_m4_2 is not null then
				validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
								, xtbl$list_array_pos => xtbl$list_array_pos2
								, x_err_code          => x_err_code
								 );		
			end if;				 

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_conf_m4_3 is not null then
					validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
									, xtbl$list_array_pos => xtbl$list_array_pos3
									, x_err_code          => x_err_code
									 );
				end if;						

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_conf_m4_4 is not null then
						validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
										, xtbl$list_array_pos => xtbl$list_array_pos4
										, x_err_code          => x_err_code
										 );	
					end if;						

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_conf_m4_5 is not null then
							validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
											, xtbl$list_array_pos => xtbl$list_array_pos5
											, x_err_code          => x_err_code
											 );	
						end if;					

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_conf_m4_6 is not null then
								validar_multiplo (pn_multiplo         => CN$MULTIPLO_4
												, xtbl$list_array_pos => xtbl$list_array_pos6
												, x_err_code          => x_err_code
												 );
							end if;								
						end if;		
					end if;						
				end if;						
			end if;
		end if;
	end if;

    --!validando multiplos de 5
	if pv_conf_m5_1 is null and 
	   pv_conf_m5_2 is null and 
	   pv_conf_m5_3 is null and 
	   pv_conf_m5_4 is null and 
	   pv_conf_m5_5 is null and 
	   pv_conf_m5_6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de multiplos de 5'); 
	else
	    --!validar multiplos en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_conf_m5_1 is not null then
			validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
							, xtbl$list_array_pos => xtbl$list_array_pos1
							, x_err_code          => x_err_code
							 );
		end if;				 
	
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_conf_m5_2 is not null then
				validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
								, xtbl$list_array_pos => xtbl$list_array_pos2
								, x_err_code          => x_err_code
								 );
			end if;					

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_conf_m5_3 is not null then
					validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
									, xtbl$list_array_pos => xtbl$list_array_pos3
									, x_err_code          => x_err_code
									 );	
				end if;					

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_conf_m5_4 is not null then
						validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
										, xtbl$list_array_pos => xtbl$list_array_pos4
										, x_err_code          => x_err_code
										 );	
					end if;					

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_conf_m5_5 is not null then
							validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
											, xtbl$list_array_pos => xtbl$list_array_pos5
											, x_err_code          => x_err_code
											 );	
						end if;					

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_conf_m5_6 is not null then
								validar_multiplo (pn_multiplo         => CN$MULTIPLO_5
												, xtbl$list_array_pos => xtbl$list_array_pos6
												, x_err_code          => x_err_code
												 );	
							end if;					
						end if;		
					end if;						
				end if;						
			end if;
		end if;
	end if;

    --!validando multiplos de 7
	if pv_conf_m7_1 is null and 
	   pv_conf_m7_2 is null and 
	   pv_conf_m7_3 is null and 
	   pv_conf_m7_4 is null and 
	   pv_conf_m7_5 is null and 
	   pv_conf_m7_6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de multiplos de 7'); 
	else
	    --!validar multiplos en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_conf_m7_1 is not null then
			validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
							, xtbl$list_array_pos => xtbl$list_array_pos1
							, x_err_code          => x_err_code
							 );
		end if;				 
	
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_conf_m7_2 is not null then
				validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
								, xtbl$list_array_pos => xtbl$list_array_pos2
								, x_err_code          => x_err_code
								 );	
			end if;				 

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_conf_m7_3 is not null then
					validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
									, xtbl$list_array_pos => xtbl$list_array_pos3
									, x_err_code          => x_err_code
									 );	
				end if;					

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_conf_m7_4 is not null then
						validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
										, xtbl$list_array_pos => xtbl$list_array_pos4
										, x_err_code          => x_err_code
										 );	
					end if;					

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_conf_m7_5 is not null then
							validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
											, xtbl$list_array_pos => xtbl$list_array_pos5
											, x_err_code          => x_err_code
											 );		
						end if;				 

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_conf_m7_6 is not null then
								validar_multiplo (pn_multiplo         => CN$MULTIPLO_7
												, xtbl$list_array_pos => xtbl$list_array_pos6
												, x_err_code          => x_err_code
												 );	
							end if;					
						end if;		
					end if;						
				end if;						
			end if;
		end if;
	end if;

	--!ley del tercio log
	if pv_conf_ltl1 is null and 
	   pv_conf_ltl2 is null and 
	   pv_conf_ltl3 is null and 
	   pv_conf_ltl4 is null and 
	   pv_conf_ltl5 is null and 
	   pv_conf_ltl6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de multiplos de ley del tercio log'); 	

	else
	    --!validar ley del tercio log en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_conf_ltl1 is not null then
			validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl1
						          , xtbl$list_array_pos => xtbl$list_array_pos1
					              , x_err_code    		=> x_err_code
                                   );
		end if;				 
	
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_conf_ltl2 is not null then
				validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl2
									  , xtbl$list_array_pos => xtbl$list_array_pos2
									  , x_err_code    		=> x_err_code
									   );	
			end if;				 

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_conf_ltl3 is not null then
					validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl3
										  , xtbl$list_array_pos => xtbl$list_array_pos3
										  , x_err_code    		=> x_err_code
										   );
				end if;					

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_conf_ltl4 is not null then
						validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl4
											  , xtbl$list_array_pos => xtbl$list_array_pos4
											  , x_err_code    		=> x_err_code
											   );
					end if;					

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_conf_ltl5 is not null then
							validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl5
												  , xtbl$list_array_pos => xtbl$list_array_pos5
												  , x_err_code    		=> x_err_code
												   );	
						end if;				 

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_conf_ltl6 is not null then
								validar_ley_tercio_log (pv_conf_ltl    	 	=> pv_conf_ltl6
													  , xtbl$list_array_pos => xtbl$list_array_pos6
													  , x_err_code    		=> x_err_code
													   );
							end if;					
						end if;		
					end if;						
				end if;						
			end if;
		end if;	
	end if;

/*
	--!patrones de numeros
	if pv_patron1 is null and 
	   pv_patron2 is null and 
	   pv_patron3 is null and 
	   pv_patron4 is null and 
	   pv_patron5 is null and 
	   pv_patron6 is null then
		dbms_output.put_line('---------');
		dbms_output.put_line('Warning: No hay configuracion de patrones de numeros'); 	

	else
	    --!validar patrones de numeros en el arreglo en base a parametro de entrada
		--!posicion 1
		if pv_patron1 is not null then
			validar_patrones_numeros (pv_patron    	 	  => pv_patron1
						            , pv_replace_add_flag => pv_replace_add_flag
								    , xtbl$list_array_pos => xtbl$list_array_pos1
					                , x_err_code          => x_err_code
                                     );
		end if;				 
	
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!posicion 2
			if pv_patron2 is not null then
				validar_patrones_numeros (pv_patron    	 	  => pv_patron2
										, pv_replace_add_flag => pv_replace_add_flag
										, xtbl$list_array_pos => xtbl$list_array_pos2
										, x_err_code          => x_err_code
										 );	
			end if;				 

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!posicion 3
				if pv_patron3 is not null then
					validar_patrones_numeros (pv_patron    	 	  => pv_patron3
											, pv_replace_add_flag => pv_replace_add_flag
											, xtbl$list_array_pos => xtbl$list_array_pos3
											, x_err_code          => x_err_code
											 );
				end if;					

				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!posicion 4
					if pv_patron4 is not null then
						validar_patrones_numeros (pv_patron    	 	  => pv_patron4
												, pv_replace_add_flag => pv_replace_add_flag
												, xtbl$list_array_pos => xtbl$list_array_pos4
												, x_err_code          => x_err_code
												 );
					end if;					

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!posicion 5
						if pv_patron5 is not null then
							validar_patrones_numeros (pv_patron    	 	  => pv_patron5
													, pv_replace_add_flag => pv_replace_add_flag
													, xtbl$list_array_pos => xtbl$list_array_pos5
													, x_err_code          => x_err_code
													 );
						end if;				 

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 6
							if pv_patron6 is not null then
								validar_patrones_numeros (pv_patron    	 	  => pv_patron6
														, pv_replace_add_flag => pv_replace_add_flag
														, xtbl$list_array_pos => xtbl$list_array_pos6
														, x_err_code          => x_err_code
														 );
							end if;					
						end if;		
					end if;						
				end if;						
			end if;
		end if;	
	end if;	
*/	
	--!filtrar los arreglos de GL para cada posicion
	array_digitos_to_coma_string (ptbl$list_array_pos1 => xtbl$list_array_pos1
					, ptbl$list_array_pos2 => xtbl$list_array_pos2
				    , ptbl$list_array_pos3 => xtbl$list_array_pos3
				    , ptbl$list_array_pos4 => xtbl$list_array_pos4
				    , ptbl$list_array_pos5 => xtbl$list_array_pos5
				    , ptbl$list_array_pos6 => xtbl$list_array_pos6
				    --!listas finales de numeros								
				    , xv_digit_list_pos1	 => xv_digit_list_pos1
				    , xv_digit_list_pos2	 => xv_digit_list_pos2
				    , xv_digit_list_pos3	 => xv_digit_list_pos3
				    , xv_digit_list_pos4	 => xv_digit_list_pos4
				    , xv_digit_list_pos5	 => xv_digit_list_pos5
				    , xv_digit_list_pos6	 => xv_digit_list_pos6						  
				    , x_err_code           => x_err_code
				     );						   
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception
  when ge$numero_primo_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20001,'El patron de numero en la posicion ('||olap_sys.w_common_pkg.g_index||') no es un numero primo');     
	raise;
   when ge$numero_par_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20001,'El patron de numero en la posicion ('||olap_sys.w_common_pkg.g_index||') no es un numero par');     
	raise;
   when ge$numero_non_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20001,'El patron de numero en la posicion ('||olap_sys.w_common_pkg.g_index||') no es un numero non');     
	raise;
  when ge$pares_nones_match then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20001,'El numero de elementos pares y nones es diferente en el plan de jugadas');     
	raise;
  when ge$numeros_primos_no_match then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20002,'La posicion de los numeros primos no coindide');     
	raise;
  when ge$select_list_len then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20001,'El numero de elementos en CONFIG_PRIMOS_PARES_NONES y SELECT LIST es diferente');     
	raise;
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;	
end validar_arreglos_numeros;	


--!ordenar un lista y devolverla como arreglo
procedure get_data_sorted (pv_string                VARCHAR2
					     , pv_sort_type				VARCHAR2 DEFAULT 'ASC'
					     , xtbl_row  IN OUT NOCOPY  dbms_sql.varchar2_table --gt$data_sort_tbl
					     , x_err_code IN OUT NOCOPY NUMBER
						  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_data_sorted';						  
  cursor c_sort_desc (pv_string  VARCHAR2) is
  select regexp_substr(pv_string,'[^,]+',1,level) str
       , substr(regexp_substr(pv_string,'[^,]+',1,level),instr(regexp_substr(pv_string,'[^,]+',1,level),'-')+1) sort_flag
    from dual 
 connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1 order by sort_flag desc;
 
  cursor c_sort_asc (pv_string  VARCHAR2) is
  select regexp_substr(pv_string,'[^,]+',1,level) str
       , substr(regexp_substr(pv_string,'[^,]+',1,level),instr(regexp_substr(pv_string,'[^,]+',1,level),'-')+1) sort_flag
    from dual 
 connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1 order by sort_flag; 
begin
  if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
  end if;
  --!creando sentencia dinamica
  olap_sys.w_common_pkg.g_dml_stmt := 'select regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level) str'; 
--  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||', substr( '||'regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level), instr('||'regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level),'||chr(39)||'-'||chr(39)||')+1) sort_flag';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from dual';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where substr( '||'regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level), instr('||'regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level),'||chr(39)||'-'||chr(39)||')+1) > 0';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' connect by level <= length('||chr(39)||pv_string||chr(39)||')-length(replace('||chr(39)||pv_string||chr(39)||','||chr(39)||','||chr(39)||','||chr(39)||chr(39)||'))+1';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' order by to_number(substr('||'regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level),1,1))'; 	
/* 
  if pv_sort_type = 'ASC' then
 	 olap_sys.w_common_pkg.g_dml_stmt := replace(olap_sys.w_common_pkg.g_dml_stmt,'<SORT_FLAG>','ASC');
  else
	 olap_sys.w_common_pkg.g_dml_stmt := replace(olap_sys.w_common_pkg.g_dml_stmt,'<SORT_FLAG>',pv_sort_type);
  end if;
*/
--		dbms_output.put_line(substr(olap_sys.w_common_pkg.g_dml_stmt,1,255));
--		dbms_output.put_line(substr(olap_sys.w_common_pkg.g_dml_stmt,256,255));

  execute immediate olap_sys.w_common_pkg.g_dml_stmt bulk collect into xtbl_row;

  x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

	if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('imprimiendo arreglo');
	  for t in xtbl_row.first..xtbl_row.last loop
	  dbms_output.put_line(t||' # '||xtbl_row(t));
	  end loop;
	end if;  
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end get_data_sorted;


--!imprimir los numeros primos salvados en tabla temporal filtro_pm_numeros_primos
procedure imprimir_pm_parejas_primos is
  LV$PROCEDURE_NAME       CONSTANT VARCHAR2(30) := 'imprimir_pm_parejas_primos';    
	cursor c_primos is
	select drawing_id
	     , lpad(primo_ini,2,'0') primo_ini
		 , lpad(primo_fin,2,'0') primo_fin
		 , diferencia_tipo
		 , diferencia
		 , estadistica
		 , jugar_numero
	  from olap_sys.filtro_pm_numeros_primos
	 order by diferencia_tipo
		 , primo_ini
		 , primo_fin;	

	ln$diferencia_tipo_prev		NUMBER := 0 ;
	lrec$row					c_primos%ROWTYPE;
begin  
	if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;

	dbms_output.put_line('----------<<<<<   Diferencia Tipo y Numeros Primos   >>>>>----------------------');
	open c_primos;
	fetch c_primos into lrec$row;
	loop
		exit when c_primos%notfound;
		ln$diferencia_tipo_prev := lrec$row.diferencia_tipo;
		while ln$diferencia_tipo_prev = lrec$row.diferencia_tipo loop
			dbms_output.put_line('Sorteo: '||lrec$row.drawing_id||' D Tipo: '||lrec$row.diferencia_tipo||' Jugar Numero: '||lrec$row.jugar_numero||' Primo Ini: '||lrec$row.primo_ini||' Primo End: '||lrec$row.primo_fin||' Estadistica: '||lrec$row.estadistica||' Diferencia: '||lrec$row.diferencia);
			fetch c_primos into lrec$row;
			exit when c_primos%notfound;
		end loop;
		dbms_output.put_line(' ');
	end loop;	
--	dbms_output.put_line('----------<<<<<   --------------------------------   >>>>>----------------------');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end imprimir_pm_parejas_primos;


--!proceso para obtener las posiciones de los numeros primos en base a una lista separada por comas
procedure get_par_numeros_primos (pv_string						      VARCHAR2
								, pv_columna_tipo					  VARCHAR2 DEFAULT 'COMB'
								, xn_primo_pos1			IN OUT NOCOPY NUMBER
								, xv_nombre_columna1	IN OUT NOCOPY VARCHAR2
								, xn_primo_pos2			IN OUT NOCOPY NUMBER
								, xv_nombre_columna2	IN OUT NOCOPY VARCHAR2
								, x_err_code        	IN OUT NOCOPY NUMBER
								) is
  LV$PROCEDURE_NAME       CONSTANT VARCHAR2(30) := 'get_par_numeros_primos';    
  CV$COL_TYPE_C			  CONSTANT VARCHAR2(30) := 'COMB';
  CV$COL_TYPE_P			  CONSTANT VARCHAR2(30) := 'POS';
  lb$primo1						   BOOLEAN := true;
  
  
begin
  if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
	  dbms_output.put_line('pv_columna_tipo: '||pv_columna_tipo);
  end if;

	gtbl$row_source.delete;
	
	--!convertir un string separado por comas en renglones de un query
	olap_sys.w_common_pkg.translate_string_to_rows (pv_string  => pv_string
												  , xtbl_row   => gtbl$row_source
												  , x_err_code => x_err_code
												   );
												  
	if gtbl$row_source.count > 0 then
		for r in gtbl$row_source.first..gtbl$row_source.last loop
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line(r||'  '||gtbl$row_source(r));
			end if;
			if to_number(gtbl$row_source(r)) = 1 then
				if lb$primo1 then
					if pv_columna_tipo = CV$COL_TYPE_C then
						xv_nombre_columna1 := CV$COL_TYPE_C||r;
					else
						xv_nombre_columna1 := CV$COL_TYPE_P||r;
					end if;
					xn_primo_pos1 := r;
					lb$primo1 := false;
				else
					if pv_columna_tipo = CV$COL_TYPE_C then
						xv_nombre_columna2 := CV$COL_TYPE_C||r;
					else
						xv_nombre_columna2 := CV$COL_TYPE_P||r;
					end if;					
					xn_primo_pos2 := r;
				end if;	
			end if;				
		end loop;
	end if;	

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('xn_primo_pos1: '||xn_primo_pos1);
		dbms_output.put_line('xn_primo_pos2: '||xn_primo_pos2);
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end get_par_numeros_primos;


--!agregar numeros primos en cada posicion en base a la decena
procedure set_numero_primo_decena (pv_decena						  VARCHAR2
								 , pv_numero_primo_list				  VARCHAR2 
								 , xtbl_qry_output    	IN OUT NOCOPY gt$gl_tbl
								 , x_err_code        	IN OUT NOCOPY NUMBER
								  ) is
  LV$PROCEDURE_NAME       CONSTANT VARCHAR2(30) := 'set_numero_primo_decena'; 
	ln$decena_ini				   NUMBER := 0;
	ln$decena_end				   NUMBER := 0;
	
	cursor c_rows_tbl (pv_string    VARCHAR2) is
	select regexp_substr(pv_string,'[^,]+',1,level) xrow
							 from dual 
						  connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1;	
begin
  if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_decena: '||pv_decena);
	  dbms_output.put_line('pv_numero_primo_list: '||pv_numero_primo_list);
  end if;   
	
	ln$decena_ini := substr(replace(pv_decena,'DIGIT BETWEEN ',null),1,instr(replace(pv_decena,'DIGIT BETWEEN ',null),'AND',1,1)-2);
	ln$decena_end := substr(replace(pv_decena,'DIGIT BETWEEN ',null),instr(replace(pv_decena,'DIGIT BETWEEN ',null),'AND ',1)+4);
	
	
	if xtbl_qry_output.count > 0 then
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('<<<   antes   >>> cnt: '||xtbl_qry_output.count);	
			for t in xtbl_qry_output.first..xtbl_qry_output.last loop
				dbms_output.put_line(xtbl_qry_output(t).digit);	
			end loop;
		end if;
		
		olap_sys.w_common_pkg.g_index := 0;
		for k in c_rows_tbl (pv_string => pv_numero_primo_list) loop
			if k.xrow between ln$decena_ini and ln$decena_end then  
				olap_sys.w_common_pkg.g_index := xtbl_qry_output.count+1;
				xtbl_qry_output(olap_sys.w_common_pkg.g_index).digit := k.xrow;
				xtbl_qry_output(olap_sys.w_common_pkg.g_index).change_flag  := 'Y';
			end if;
		end loop;	

		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('<<<   despues   >>> cnt: '||xtbl_qry_output.count);	
			for t in xtbl_qry_output.first..xtbl_qry_output.last loop
				dbms_output.put_line(xtbl_qry_output(t).digit);	
			end loop;
		end if;
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	   
end set_numero_primo_decena;
  

/*
--!recuperar los numeros primos guardando la info en un arreglo
procedure get_numero_primo_rule (pv_conf_ppn1 	                  VARCHAR2
							   , pv_conf_ppn2 	                  VARCHAR2
							   , pv_conf_ppn3 	                  VARCHAR2
							   , pv_conf_ppn4 	                  VARCHAR2
							   , pv_conf_ppn5 	                  VARCHAR2
							   , pv_conf_ppn6 	                  VARCHAR2								   
							   --!arreglos con la info de cada posicion
							   , ptbl$list_array_pos1             gt$gl_tbl
                               , ptbl$list_array_pos2             gt$gl_tbl
                               , ptbl$list_array_pos3             gt$gl_tbl
                               , ptbl$list_array_pos4             gt$gl_tbl
                               , ptbl$list_array_pos5             gt$gl_tbl
                               , ptbl$list_array_pos6             gt$gl_tbl
							   , pn_drawing_id                    NUMBER DEFAULT NULL
							   , pn_diferencia_tipo				  NUMBER
							   --!numeros primos
							   , xtbl$numero_primo  IN OUT NOCOPY gt$np_tbl
							   , x_err_code         IN OUT NOCOPY NUMBER
							   ) is
  LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_numero_primo_rule';
  lv$string                          varchar2(1000);
  ln$numero_primo1_cnt				 number := 0;
  ln$numero_primo2_cnt				 number := 0;
  ln$numero_primo3_cnt				 number := 0;
  ln$numero_primo4_cnt				 number := 0;
  ln$numero_primo5_cnt				 number := 0;
  ln$numero_primo6_cnt				 number := 0; 
  ln$caso_primo1                     number := 0; 
  ln$rowcount_primo1                 number := 0;   
  ln$caso_primo2                     number := 0; 
  ln$rowcount_primo2                 number := 0;
  ln$numero_primo1					 number := 0;
  ln$numero_primo2					 number := 0;  					
  ltbl$numero_primo                  dbms_sql.varchar2_table;
  ltbl$$data_sorted				     dbms_sql.varchar2_table; 
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_conf_ppn1: '||pv_conf_ppn1);
		dbms_output.put_line('pv_conf_ppn2: '||pv_conf_ppn2);
		dbms_output.put_line('pv_conf_ppn3: '||pv_conf_ppn3);
		dbms_output.put_line('pv_conf_ppn4: '||pv_conf_ppn4);
		dbms_output.put_line('pv_conf_ppn5: '||pv_conf_ppn5);
		dbms_output.put_line('pv_conf_ppn6: '||pv_conf_ppn6);
		dbms_output.put_line('ptbl$list_array_pos1.count: '||ptbl$list_array_pos1.count);	
		dbms_output.put_line('ptbl$list_array_pos2.count: '||ptbl$list_array_pos2.count);	
		dbms_output.put_line('ptbl$list_array_pos3.count: '||ptbl$list_array_pos3.count);			
		dbms_output.put_line('ptbl$list_array_pos4.count: '||ptbl$list_array_pos4.count);	
		dbms_output.put_line('ptbl$list_array_pos5.count: '||ptbl$list_array_pos5.count);	
		dbms_output.put_line('ptbl$list_array_pos6.count: '||ptbl$list_array_pos6.count);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
   end if;
	
	--!limpiando el array
	xtbl$numero_primo.delete;
	
	--!contando los numeros numeros primos de la posicion 1
	if instr(pv_conf_ppn1,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos1.count: '||ptbl$list_array_pos1.count);
		if ptbl$list_array_pos1.count > 0 then
			for t in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
				if ptbl$list_array_pos1(t).numero_primo_flag = 1 then
--				dbms_output.put_line('##ptbl$list_array_pos1.digit: '||ptbl$list_array_pos1(t).digit||' - '||ptbl$list_array_pos1(t).numero_primo_flag);
					ln$numero_primo1_cnt := ln$numero_primo1_cnt + 1; 
				end if;
			end loop;
		end if;
	end if;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo1_cnt: '||ln$numero_primo1_cnt);	
	end if;	
	
	--!contando los numeros numeros primos de la posicion 2	
	if instr(pv_conf_ppn2,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos2.count: '||ptbl$list_array_pos2.count);
		if ptbl$list_array_pos2.count > 0 then
			for t in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
				if ptbl$list_array_pos2(t).numero_primo_flag = 1 then
	--			dbms_output.put_line('##ptbl$list_array_pos2.digit: '||ptbl$list_array_pos2(t).digit||' - '||ptbl$list_array_pos2(t).numero_primo_flag);
					ln$numero_primo2_cnt := ln$numero_primo2_cnt + 1; 
				end if;
			end loop; 
		end if;
	end if;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo2_cnt: '||ln$numero_primo2_cnt);	
	end if;

	--!contando los numeros numeros primos de la posicion 3		
	if instr(pv_conf_ppn3,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos3.count: '||ptbl$list_array_pos3.count);
		if ptbl$list_array_pos3.count > 0 then
			for t in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
				if ptbl$list_array_pos3(t).numero_primo_flag = 1 then
	--			dbms_output.put_line('##ptbl$list_array_pos3.digit: '||ptbl$list_array_pos3(t).digit||' - '||ptbl$list_array_pos3(t).numero_primo_flag);
					ln$numero_primo3_cnt := ln$numero_primo3_cnt + 1; 
				end if;
			end loop; 
		end if;
	end if;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo3_cnt: '||ln$numero_primo3_cnt);	
	end if;

	--!contando los numeros numeros primos de la posicion 4		
	if instr(pv_conf_ppn4,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos4.count: '||ptbl$list_array_pos4.count);
		if ptbl$list_array_pos4.count > 0 then
			for t in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
				if ptbl$list_array_pos4(t).numero_primo_flag = 1 then
	--			dbms_output.put_line('##ptbl$list_array_pos4.digit: '||ptbl$list_array_pos4(t).digit||' - '||ptbl$list_array_pos4(t).numero_primo_flag);
					ln$numero_primo4_cnt := ln$numero_primo4_cnt + 1; 
				end if;
			end loop; 
		end if;
	end if;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo4_cnt: '||ln$numero_primo4_cnt);	
	end if;

	--!contando los numeros numeros primos de la posicion 5		
	if instr(pv_conf_ppn5,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos5.count: '||ptbl$list_array_pos5.count);
		if ptbl$list_array_pos5.count > 0 then
			for t in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
				if ptbl$list_array_pos5(t).numero_primo_flag = 1 then
	--			dbms_output.put_line('##ptbl$list_array_pos5.digit: '||ptbl$list_array_pos5(t).digit||' - '||ptbl$list_array_pos5(t).numero_primo_flag);
					ln$numero_primo5_cnt := ln$numero_primo5_cnt + 1; 
				end if;
			end loop;
		end if;
	end if;

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo5_cnt: '||ln$numero_primo5_cnt);	
	end if;

	--!contando los numeros numeros primos de la posicion 6		
	if instr(pv_conf_ppn6,CV$NUMERO_PRIMO) > 0 then
--		dbms_output.put_line('##ptbl$list_array_pos6.count: '||ptbl$list_array_pos6.count);
		if ptbl$list_array_pos6.count > 0 then
			for t in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
				if ptbl$list_array_pos6(t).numero_primo_flag = 1 then
	--			dbms_output.put_line('##ptbl$list_array_pos6.digit: '||ptbl$list_array_pos6(t).digit||' - '||ptbl$list_array_pos6(t).numero_primo_flag);
					ln$numero_primo6_cnt := ln$numero_primo6_cnt + 1; 
				end if;
			end loop;
		end if;
	end if;	

	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('##ln$numero_primo6_cnt: '||ln$numero_primo6_cnt);	
	end if;	

	lv$string :=  '1-'||ln$numero_primo1_cnt
				||',2-'||ln$numero_primo2_cnt
				||',3-'||ln$numero_primo3_cnt
				||',4-'||ln$numero_primo4_cnt
				||',5-'||ln$numero_primo5_cnt
				||',6-'||ln$numero_primo6_cnt;	

	--!construye el select list final
	olap_sys.w_common_pkg.translate_string_to_rows (pv_string  => lv$string
                                                  , xtbl_row   => ltbl$numero_primo
								                  , x_err_code => x_err_code
								                   );
	
	--!construir string con las sumas de los string separado por comas para convertilo en arreglo
	if ltbl$numero_primo.count > 0 then
		lv$string := null;
		for r in ltbl$numero_primo.first..ltbl$numero_primo.last loop
			if to_number(substr(ltbl$numero_primo(r),instr(ltbl$numero_primo(r),'-')+1)) > 0 then			
				lv$string := lv$string||ltbl$numero_primo(r)||',';
				
				if GB$SHOW_PROC_NAME then
					dbms_output.put_line('ubicacion final de primos [posicion-#primos]: '||ltbl$numero_primo(r));
				end if;
			end if;
		end loop;
	end if;
	
	lv$string := substr(lv$string,1,length(lv$string)-1);
	
	--!ordenar un lista y devolverla como arreglo
	get_data_sorted (pv_string  => lv$string
				   , xtbl_row   => ltbl$$data_sorted
				   , x_err_code => x_err_code
				    );

	if ltbl$$data_sorted.count > 0 then
		for k in ltbl$$data_sorted.first..ltbl$$data_sorted.last loop

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line(k ||'  primo: '||ltbl$$data_sorted(k));	
			end if;
			
			if k = 1 then
				ln$caso_primo1     :=  to_number(substr(ltbl$$data_sorted(k),1,1));
				ln$rowcount_primo1 :=  to_number(substr(ltbl$$data_sorted(k),instr(ltbl$$data_sorted(k),'-')+1));
			elsif k = 2 then
				ln$caso_primo2     :=  to_number(substr(ltbl$$data_sorted(k),1,1));
				ln$rowcount_primo2 :=  to_number(substr(ltbl$$data_sorted(k),instr(ltbl$$data_sorted(k),'-')+1));
			end if;	
		end loop;

		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
			dbms_output.put_line('ln$rowcount_primo1: '||ln$rowcount_primo1||'  ln$rowcount_primo2: '||ln$rowcount_primo2);	
		end if;

		--!comparando el rowcount de los arreglos resultantes 
		olap_sys.w_common_pkg.g_index := 0;
		if ln$caso_primo1 = 1 then			
			if ln$caso_primo2 = 2 then		
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('100. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos2.count > 0 then
						for k in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
							if ptbl$list_array_pos1.count > 0 then
								for j in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('10 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(j).digit||' * '||ptbl$list_array_pos2(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos2(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;	
				else
					if ptbl$list_array_pos1.count > 0 then
						for k in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
							if ptbl$list_array_pos2.count > 0 then
								for j in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('20 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(k).digit||' * '||ptbl$list_array_pos2(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos2(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;	
				end if;
			end if;
			
			if ln$caso_primo2 = 3 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('110. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos3.count > 0 then
						for k in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
							if ptbl$list_array_pos1.count > 0 then
								for j in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('30 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(j).digit||' * '||ptbl$list_array_pos3(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos3(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos1.count > 0 then
						for k in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
							if ptbl$list_array_pos3.count > 0 then
								for j in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('40 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(k).digit||' * '||ptbl$list_array_pos3(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos3(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;
			
			if ln$caso_primo2 = 4 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('120. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos4.count > 0 then
						for k in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
							if ptbl$list_array_pos1.count > 0 then
								for j in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('50 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(j).digit||' * '||ptbl$list_array_pos4(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos1.count > 0 then
						for k in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
							if ptbl$list_array_pos4.count > 0 then
								for j in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('60 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(k).digit||' * '||ptbl$list_array_pos4(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;	
				end if;
			end if;

			if ln$caso_primo2 = 5 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('130. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;			
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos5.count > 0 then
						for k in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
							if ptbl$list_array_pos1.count > 0 then
								for j in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('70 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(j).digit||' * '||ptbl$list_array_pos5(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos1.count > 0 then
						for k in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
							if ptbl$list_array_pos5.count > 0 then
								for j in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('90 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(k).digit||' * '||ptbl$list_array_pos5(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;

			if ln$caso_primo2 = 6 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('140. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos6.count > 0 then
						for k in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
							if ptbl$list_array_pos1.count > 0 then
								for j in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('100 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(j).digit||' * '||ptbl$list_array_pos6(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos1.count > 0 then
						for k in ptbl$list_array_pos1.first..ptbl$list_array_pos1.last loop
							if ptbl$list_array_pos6.count > 0 then
								for j in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('110 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos1(k).digit||' * '||ptbl$list_array_pos6(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos1(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;			
		end if;	
		
		if ln$caso_primo1 = 2 then
			if ln$caso_primo2 = 3 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('150. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos3.count > 0 then
						for k in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
							if ptbl$list_array_pos2.count > 0 then
								for j in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('120 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(j).digit||' * '||ptbl$list_array_pos3(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos3(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos2.count > 0 then
						for k in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
							if ptbl$list_array_pos3.count > 0 then
								for j in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('130 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(k).digit||' * '||ptbl$list_array_pos3(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos3(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;

			if ln$caso_primo2 = 4 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('160. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos4.count > 0 then
						for k in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
							if ptbl$list_array_pos2.count > 0 then
								for j in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('140 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(j).digit||' * '||ptbl$list_array_pos4(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos2.count > 0 then
						for k in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
							if ptbl$list_array_pos4.count > 0 then
								for j in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('150 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(k).digit||' * '||ptbl$list_array_pos4(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;	
			
			if ln$caso_primo2 = 5 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('170. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos5.count > 0 then
						for k in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
							if ptbl$list_array_pos2.count > 0 then
								for j in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('160 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(j).digit||' * '||ptbl$list_array_pos5(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos2.count > 0 then
						for k in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
							if ptbl$list_array_pos5.count > 0 then
								for j in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('170 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(k).digit||' * '||ptbl$list_array_pos5(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;				
			
			if ln$caso_primo2 = 6 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('180. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos6.count > 0 then
						for k in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
							if ptbl$list_array_pos2.count > 0 then
								for j in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('180 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(j).digit||' * '||ptbl$list_array_pos6(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos2.count > 0 then
						for k in ptbl$list_array_pos2.first..ptbl$list_array_pos2.last loop
							--dbms_output.put_line(k ||'  pos2: '||ptbl$list_array_pos2(k).digit);
							if ptbl$list_array_pos6.count > 0 then
								for j in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
									--dbms_output.put_line(j ||'  pos6: '||ptbl$list_array_pos6(j).digit);
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('190 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos2(k).digit||' * '||ptbl$list_array_pos6(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos2(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;					
		end if;		
		
		if ln$caso_primo1 = 3 then
			if ln$caso_primo2 = 4 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('190. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos4.count > 0 then
						for k in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
							if ptbl$list_array_pos3.count > 0 then
								for j in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('200 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(j).digit||' * '||ptbl$list_array_pos4(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos3.count > 0 then
						for k in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
							if ptbl$list_array_pos4.count > 0 then
								for j in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('210 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(k).digit||' * '||ptbl$list_array_pos4(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos4(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;	

			if ln$caso_primo2 = 5 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('200. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos5.count > 0 then
						for k in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
							if ptbl$list_array_pos3.count > 0 then
								for j in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('220 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(j).digit||' * '||ptbl$list_array_pos5(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos3.count > 0 then
						for k in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
							if ptbl$list_array_pos5.count > 0 then
								for j in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('230 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(k).digit||' * '||ptbl$list_array_pos5(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;	

			if ln$caso_primo2 = 6 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('210. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos6.count > 0 then
						for k in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
							if ptbl$list_array_pos3.count > 0 then
								for j in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('240 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(j).digit||' * '||ptbl$list_array_pos6(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos3.count > 0 then
						for k in ptbl$list_array_pos3.first..ptbl$list_array_pos3.last loop
							if ptbl$list_array_pos6.count > 0 then
								for j in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('250 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos3(k).digit||' * '||ptbl$list_array_pos6(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos3(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;				
		end if;	

		if ln$caso_primo1 = 4 then
			if ln$caso_primo2 = 5 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('220. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					if ptbl$list_array_pos5.count > 0 then
						for k in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
							if ptbl$list_array_pos4.count > 0 then
								for j in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('260 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos4(j).digit||' * '||ptbl$list_array_pos5(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos4(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos4.count > 0 then
						for k in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
							if ptbl$list_array_pos5.count > 0 then
								for j in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('270 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos4(k).digit||' * '||ptbl$list_array_pos5(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos4(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos5(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;		

			if ln$caso_primo2 = 6 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('230. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos6.count > 0 then
						for k in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
							if ptbl$list_array_pos4.count > 0 then
								for j in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('280 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos4(j).digit||' * '||ptbl$list_array_pos6(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos4(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos4.count > 0 then
						for k in ptbl$list_array_pos4.first..ptbl$list_array_pos4.last loop
							if ptbl$list_array_pos6.count > 0 then
								for j in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('290 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos4(k).digit||' * '||ptbl$list_array_pos6(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos4(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;				
		end if;
		
		if ln$caso_primo1 = 5 then	
			if ln$caso_primo2 = 6 then
if GB$SHOW_PROC_NAME then
	dbms_output.put_line('240. ln$caso_primo1: '||ln$caso_primo1||'  ln$caso_primo2: '||ln$caso_primo2);	
end if;
				if ln$rowcount_primo1 <= ln$rowcount_primo2 then
					olap_sys.w_common_pkg.g_index := 0;
					if ptbl$list_array_pos6.count > 0 then
						for k in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
							if ptbl$list_array_pos5.count > 0 then
								for j in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('300 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos5(j).digit||' * '||ptbl$list_array_pos6(k).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos5(j).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(k).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);
					end if;
				else
					if ptbl$list_array_pos5.count > 0 then
						for k in ptbl$list_array_pos5.first..ptbl$list_array_pos5.last loop
							if ptbl$list_array_pos6.count > 0 then
								for j in ptbl$list_array_pos6.first..ptbl$list_array_pos6.last loop
									if GB$SHOW_PROC_NAME then
										dbms_output.put_line('310 caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  '||ptbl$list_array_pos5(k).digit||' * '||ptbl$list_array_pos6(j).digit);
									end if;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ptbl$list_array_pos5(k).digit;
									xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ptbl$list_array_pos6(j).digit;
									olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
								end loop;
							end if;
						end loop;
					end if;
					if GB$SHOW_PROC_NAME then
						dbms_output.put_line('caso_primo1: '||ln$caso_primo1||'  caso_primo2: '||ln$caso_primo2||'  rowcount: '||xtbl$numero_primo.count);				
					end if;
				end if;
			end if;	
		end if;		
	end if;


	dbms_output.put_line('### xtbl$numero_primo.count: '||xtbl$numero_primo.count);
	if xtbl$numero_primo.count > 0 then	
	  for t in xtbl$numero_primo.first..xtbl$numero_primo.last loop
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line(t||' # [primo1-primo2]: '||xtbl$numero_primo(t).numero_primo1||' - '||xtbl$numero_primo(t).numero_primo2);
		end if;
		ln$numero_primo1 := xtbl$numero_primo(t).numero_primo1;
		ln$numero_primo2 := xtbl$numero_primo(t).numero_primo2;
		filtrar_numeros_primos (pn_drawing_id      => pn_drawing_id
							  , pn_diferencia_tipo => pn_diferencia_tipo
							  , xn_numero_primo1   => ln$numero_primo1
							  , xn_numero_primo2   => ln$numero_primo2
							  , x_err_code         => x_err_code
							   );

		--!filtrando las parejas de numeros primos
		if ln$numero_primo1 = 0 and ln$numero_primo2 = 0 then
			xtbl$numero_primo(t).numero_primo1 := ln$numero_primo1; 
			xtbl$numero_primo(t).numero_primo2 := ln$numero_primo2; 
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('numero primo filtrado: '||ln$numero_primo1||' - '||ln$numero_primo2);
			end if;	
		end if;	
	  end loop;	

		--!logica para remover ceros y ordenar la informacion del arreglo
		olap_sys.w_common_pkg.g_dml_stmt := null;
		lv$string 		:= null;
		for t in xtbl$numero_primo.first..xtbl$numero_primo.last loop
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||xtbl$numero_primo(t).numero_primo1||'-'||xtbl$numero_primo(t).numero_primo2||',';	
		end loop;
		
		--!recuperar los valores distinctos de un string separado por comas
		olap_sys.w_common_pkg.get_distinct_values_from_list (pv_string 				=> substr(olap_sys.w_common_pkg.g_dml_stmt,1,length(olap_sys.w_common_pkg.g_dml_stmt)-1)
														   , pv_data_sort			=> 'ASC'
														   , xv_distinct_value_list => lv$string
															);
		
		--!removiendo apostrofes del string		
		lv$string := replace(lv$string,chr(39),null);
		--!limpiando el arreglo
		xtbl$numero_primo.delete;
		olap_sys.w_common_pkg.g_index := 1;
		for k in (select regexp_substr(lv$string,'[^,]+',1,level) str
			        from dual 
				 connect by level <= length(lv$string)-length(replace(lv$string,',',''))+1) loop
			ln$numero_primo1 := to_number(substr(k.str,1,instr(k.str,'-',1,1)-1));
			ln$numero_primo2 := to_number(substr(k.str,instr(k.str,'-',1,1)+1));
			if ln$numero_primo1 > 0 then
				xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo1 := ln$numero_primo1; 
				xtbl$numero_primo(olap_sys.w_common_pkg.g_index).numero_primo2 := ln$numero_primo2; 
				olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
			end if;			
		end loop;
		
		if GB$SHOW_PROC_NAME then
			for t in xtbl$numero_primo.first..xtbl$numero_primo.last loop
				dbms_output.put_line(xtbl$numero_primo(t).numero_primo1||'-'||xtbl$numero_primo(t).numero_primo2);	
			end loop;
		end if;		
	end if;


	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('250. final '||LV$PROCEDURE_NAME);	
	end if;	
										
exception	
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_numero_primo_rule;   


--!obtener el where clause del panorama en base a los numeros primos y lista de numeros
procedure get_panorama_where_clause(--!numeros primos
							       pv_numero_primo_1   					 VARCHAR2
							     , pv_numero_primo_2   					 VARCHAR2
								 , pv_digit_list_pos1				     VARCHAR2
							     , pv_digit_list_pos2				     VARCHAR2
							     , pv_digit_list_pos3				     VARCHAR2
							     , pv_digit_list_pos4				     VARCHAR2
							     , pv_digit_list_pos5				     VARCHAR2
							     , pv_digit_list_pos6				  	 VARCHAR2
								 --!configuracion de primos, pares y nones
							     , pv_conf_ppn1 	   					VARCHAR2
							     , pv_conf_ppn2 	   					VARCHAR2
							     , pv_conf_ppn3 	   					VARCHAR2
							     , pv_conf_ppn4 	   					VARCHAR2
							     , pv_conf_ppn5 	   					VARCHAR2
							     , pv_conf_ppn6 	   					VARCHAR2
                                 , pv_final_list_enable                 VARCHAR2 	
								 --!patrones de numeros
								 , pv_patron1                           VARCHAR2 DEFAULT NULL
								 , pv_patron2                           VARCHAR2 DEFAULT NULL
								 , pv_patron3                           VARCHAR2 DEFAULT NULL
								 , pv_patron4                           VARCHAR2 DEFAULT NULL
								 , pv_patron5                           VARCHAR2 DEFAULT NULL
								 , pv_patron6                           VARCHAR2 DEFAULT NULL
							     --!decenas
							     , pv_d1         	                    VARCHAR2
							     , pv_d2         	                    VARCHAR2
							     , pv_d3         	                    VARCHAR2
							     , pv_d4         	                    VARCHAR2
							     , pv_d5         	                    VARCHAR2
							     , pv_d6         	                    VARCHAR2								 
								 , xv_where_clause        IN OUT NOCOPY VARCHAR2
								 , x_err_code             IN OUT NOCOPY NUMBER
								 ) is
  LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_panorama_where_clause';
  CV$IN_SIN_INFO            constant varchar2(30) := 'IN ()';
  le$pos1_invalid                    exception;
  le$pos2_invalid                    exception;
  le$pos3_invalid                    exception;
  le$pos4_invalid                    exception;
  le$pos5_invalid                    exception;
  le$pos6_invalid                    exception;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_numero_primo_1: '||pv_numero_primo_1);
		dbms_output.put_line('pv_numero_primo_2: '||pv_numero_primo_2);
		dbms_output.put_line('pv_digit_list_pos1: '||pv_digit_list_pos1);
		dbms_output.put_line('pv_digit_list_pos2: '||pv_digit_list_pos2);
		dbms_output.put_line('pv_digit_list_pos3: '||pv_digit_list_pos3);
		dbms_output.put_line('pv_digit_list_pos4: '||pv_digit_list_pos4);
		dbms_output.put_line('pv_digit_list_pos5: '||pv_digit_list_pos5);
		dbms_output.put_line('pv_digit_list_pos6: '||pv_digit_list_pos6);
		dbms_output.put_line('pv_conf_ppn1: '||pv_conf_ppn1);
		dbms_output.put_line('pv_conf_ppn2: '||pv_conf_ppn2);
		dbms_output.put_line('pv_conf_ppn3: '||pv_conf_ppn3);
		dbms_output.put_line('pv_conf_ppn4: '||pv_conf_ppn4);
		dbms_output.put_line('pv_conf_ppn5: '||pv_conf_ppn5);
		dbms_output.put_line('pv_conf_ppn6: '||pv_conf_ppn6);
		dbms_output.put_line('pv_d1: '||pv_d1);	
		dbms_output.put_line('pv_d2: '||pv_d2);	
		dbms_output.put_line('pv_d3: '||pv_d3);	
		dbms_output.put_line('pv_d4: '||pv_d4);	
		dbms_output.put_line('pv_d5: '||pv_d5);	
		dbms_output.put_line('pv_d6: '||pv_d6);	
   end if;


	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('numero_primo_1: '||pv_numero_primo_1||'  numero_primo_2: '||pv_numero_primo_2);		
	end if;
	
	if instr(pv_conf_ppn1,'PR') > 0 then 
		--!PR1	PR2	P1_COMP	P2_COMP	P3_COMP	P4_COMP
		if instr(pv_conf_ppn2,'PR') > 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND '||pv_numero_primo_1||' < '||pv_numero_primo_2;
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron1||')';
			end if;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P1_COMP ';
			if pv_patron2 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron2||')';
			end if;			
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P1_COMP');
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos3;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron3||')';
					end if;						
			end if;
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P2_COMP');
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
					end if;							
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
				end if;						
			end if;					
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;											
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;						
			end if;	
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;											
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;				

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('10. '||substr(xv_where_clause,1,250));
			end if;
			
		--!PR1	P1_COMP	PR2	P2_COMP	P3_COMP	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') > 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND '||pv_numero_primo_1||' < P1_COMP';
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron1||')';
			end if;	
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P1_COMP');
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
				end if;						
			end if;		
			xv_where_clause := xv_where_clause ||' AND P1_COMP < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P2_COMP';
			if pv_patron3 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron3||')';
			end if;				
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P2_COMP');
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
				end if;					
			end if;						
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';				
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;					
			end if;				
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;					
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;				
			end if;				

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('20. '||substr(xv_where_clause,1,250));
			end if;
			
		--!PR1	P1_COMP	P2_COMP	PR2	P3_COMP	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND '||pv_numero_primo_1||' < P1_COMP ';
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron1||')';
			end if;			
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P1_COMP');
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;					
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P3_COMP ';
			if pv_patron4 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron4||')';
			end if;			
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;								

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('30. '||substr(xv_where_clause,1,250));
			end if;
			
		--!PR1	P1_COMP	P2_COMP	P3_COMP	PR2	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') > 0 and instr(pv_conf_ppn6,'PR') = 0 then	
			xv_where_clause := ' AND '||pv_numero_primo_1||' < P1_COMP ';
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron1||')';
			end if;
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P1_COMP');
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
				end if;						
			end if;							
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;						
			end if;				
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;					
			end if;								
			xv_where_clause := xv_where_clause ||' AND P3_COMP < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P4_COMP ';
			if pv_patron5 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron5||')';
			end if;			
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
					end if;							
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
				end if;						
			end if;								

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('40. '||substr(xv_where_clause,1,250));
			end if;
			
		--!PR1	P1_COMP	P2_COMP	P3_COMP	P4_COMP	PR2
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') > 0 then
			xv_where_clause := ' AND '||pv_numero_primo_1||' < P1_COMP ';
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron1||')';
			end if;
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P1_COMP');
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron2||')';
				end if;						
			end if;							
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;					
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;				
			end if;										
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P4_COMP');
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P4_COMP < '||pv_numero_primo_2;
			if pv_patron6 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron6||')';
			end if;	

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('50. '||substr(xv_where_clause,1,250));
			end if;
			
		end if;
	else
		--!P1_COMP	PR1	PR2	P2_COMP	P3_COMP	P4_COMP
		if instr(pv_conf_ppn2,'PR') > 0 and instr(pv_conf_ppn3,'PR') > 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND P1_COMP < '||pv_numero_primo_1;
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;				
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < '||pv_numero_primo_2;
			if pv_patron2 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron2||')';
			end if;				
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P2_COMP ';
			if pv_patron3 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron3||')';
			end if;				
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P2_COMP');
					end if;	
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron4||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d5 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;					
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;				

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('110. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	PR1	P2_COMP	PR2	P3_COMP	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') > 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND P1_COMP < '||pv_numero_primo_1;
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;				
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P2_COMP ';
			if pv_patron2 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron2||')';
			end if;			
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P3_COMP';
			if pv_patron4 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron4||')';
			end if;							
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d5 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;					
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;					
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;				

			if GB$SHOW_PROC_NAME then	
				dbms_output.put_line('120. '||substr(xv_where_clause,1,250));
			end if;	
				
		--!P1_COMP	PR1	P2_COMP	P3_COMP	PR2	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') > 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') > 0 and instr(pv_conf_ppn6,'PR') = 0 then
			xv_where_clause := ' AND P1_COMP < '||pv_numero_primo_1;
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P2_COMP ';
			if pv_patron2 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron2||')';
			end if;			
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;					
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					end if;
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;				
			end if;								
			xv_where_clause := xv_where_clause ||' AND P3_COMP < '||pv_numero_primo_2;	
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P4_COMP';	
			if pv_patron5 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron5||')';
			end if;			
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;											

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('130. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	PR1	P2_COMP	P3_COMP	P4_COMP	PR2
		elsif instr(pv_conf_ppn2,'PR') > 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') > 0 then
			xv_where_clause := ' AND P1_COMP < '||pv_numero_primo_1;
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P2_COMP ';
			if pv_patron2 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron2||')';
			end if;				
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P2_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					end if;
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d5 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P4_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
				end if;					
			end if;	
			
			xv_where_clause := xv_where_clause ||' AND P4_COMP < '||pv_numero_primo_2;					
			if pv_patron6 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron6||')';
			end if;	
				
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('140. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	PR1	PR2	P3_COMP	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') > 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;				
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;				
			end if;				
			xv_where_clause := xv_where_clause ||' AND P2_COMP < '||pv_numero_primo_1;
			if pv_patron3 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron3||')';
			end if;				
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P3_COMP';
			if pv_patron4 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron4||')';
			end if;				
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d5 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P3_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron5||')';
				end if;										
			end if;				
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;				

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('150. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	PR1	P3_COMP	PR2	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') > 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') = 0 then
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;					
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < '||pv_numero_primo_1;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P3_COMP';
			if pv_patron3 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron3||')';
			end if;	
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					end if;
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;					
			end if;						
			xv_where_clause := xv_where_clause ||' AND P3_COMP < '||pv_numero_primo_2;								
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P4_COMP';	
			if pv_patron5 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron5||')';
			end if;				
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;					
			end if;											

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('160. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	PR1	P2_COMP	P3_COMP	PR2
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') > 0 and instr(pv_conf_ppn4,'PR') = 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') > 0 then
			xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
			if pv_patron1 is not null then
				xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
			end if;
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < '||pv_numero_primo_1;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P3_COMP';
			if pv_patron3 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron3||')';
			end if;				
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P3_COMP');
					end if;	
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
					end if;					
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron4||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos5 = CV$IN_SIN_INFO or pv_digit_list_pos5 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d5 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d5,'DIGIT','P4_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos5_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos5;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P4_COMP < '||pv_numero_primo_2;
			if pv_patron6 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron6||')';
			end if;	
			
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('170. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	P3_COMP	PR1	PR2	P4_COMP
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') > 0 and instr(pv_conf_ppn6,'PR') = 0 then
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;						
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;						
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P3_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
					end if;					
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
				end if;					
			end if;	
			xv_where_clause := xv_where_clause ||' AND P3_COMP < '||pv_numero_primo_1;
			if pv_patron4 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron4||')';
			end if;			
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < '||pv_numero_primo_2;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' < P4_COMP';	
			if pv_patron5 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron5||')';
			end if;				
			if pv_digit_list_pos6 = CV$IN_SIN_INFO or pv_digit_list_pos6 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d6 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d6,'DIGIT','P4_COMP');
					end if;
					if pv_patron6 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
					end if;						
				else
					raise le$pos6_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos6;
				if pv_patron6 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron6||')';
				end if;										
			end if;								

			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('180. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	P3_COMP	PR1	P4_COMP	PR2
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') > 0 then
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;					
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;				
			end if;							
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P3_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
					end if;					
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < '||pv_numero_primo_1;
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < P4_COMP';
			if pv_patron4 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron4||')';
			end if;				
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P4_COMP');
					end if;
					if pv_patron5 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos4;
				if pv_patron5 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron5||')';
				end if;					
			end if;				
			
			xv_where_clause := xv_where_clause ||' AND P4_COMP < '||pv_numero_primo_2;
			if pv_patron6 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron6||')';
			end if;	
			
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('190. '||substr(xv_where_clause,1,250));
			end if;
			
		--!P1_COMP	P2_COMP	P3_COMP	P4_COMP	PR1	PR2
		elsif instr(pv_conf_ppn2,'PR') = 0 and instr(pv_conf_ppn3,'PR') = 0 and instr(pv_conf_ppn4,'PR') > 0 and instr(pv_conf_ppn5,'PR') = 0 and instr(pv_conf_ppn6,'PR') > 0 then
			if pv_digit_list_pos1 = CV$IN_SIN_INFO or pv_digit_list_pos1 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P1_COMP = P1_COMP';
					if pv_d1 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d1,'DIGIT','P1_COMP');
					end if;
					if pv_patron1 is not null then
						xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
					end if;					
				else
					raise le$pos1_invalid;
				end if;
			else
				xv_where_clause := ' AND P1_COMP '||pv_digit_list_pos1;
				if pv_patron1 is not null then
					xv_where_clause := xv_where_clause ||' AND P1_COMP IN ('||pv_patron1||')';
				end if;				
			end if;
			xv_where_clause := xv_where_clause ||' AND P1_COMP < P2_COMP';
			if pv_digit_list_pos2 = CV$IN_SIN_INFO or pv_digit_list_pos2 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P2_COMP = P2_COMP';
					if pv_d2 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d2,'DIGIT','P2_COMP');
					end if;
					if pv_patron2 is not null then
						xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
					end if;					
				else
					raise le$pos2_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P2_COMP '||pv_digit_list_pos2;
				if pv_patron2 is not null then
					xv_where_clause := xv_where_clause ||' AND P2_COMP IN ('||pv_patron2||')';
				end if;					
			end if;				
			xv_where_clause := xv_where_clause ||' AND P2_COMP < P3_COMP';
			if pv_digit_list_pos3 = CV$IN_SIN_INFO or pv_digit_list_pos3 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P3_COMP = P3_COMP';
					if pv_d3 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d3,'DIGIT','P3_COMP');
					end if;
					if pv_patron3 is not null then
						xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
					end if;						
				else
					raise le$pos3_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P3_COMP '||pv_digit_list_pos3;
				if pv_patron3 is not null then
					xv_where_clause := xv_where_clause ||' AND P3_COMP IN ('||pv_patron3||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P3_COMP < P4_COMP';
			if pv_digit_list_pos4 = CV$IN_SIN_INFO or pv_digit_list_pos4 is null then
				if pv_final_list_enable = CV$ENABLE then
					xv_where_clause := xv_where_clause ||' AND P4_COMP = P4_COMP';
					if pv_d4 != CV$SIN_VALOR then
						xv_where_clause := xv_where_clause ||' AND '||replace(pv_d4,'DIGIT','P4_COMP');
					end if;
					if pv_patron4 is not null then
						xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron4||')';
					end if;						
				else
					raise le$pos4_invalid;
				end if;
			else
				xv_where_clause := xv_where_clause ||' AND P4_COMP '||pv_digit_list_pos4;
				if pv_patron4 is not null then
					xv_where_clause := xv_where_clause ||' AND P4_COMP IN ('||pv_patron4||')';
				end if;					
			end if;							
			xv_where_clause := xv_where_clause ||' AND P4_COMP < '||pv_numero_primo_1;
			if pv_patron5 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' IN ('||pv_patron5||')';
			end if;				
			xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_1||' < '||pv_numero_primo_2;				
			if pv_patron6 is not null then
				xv_where_clause := xv_where_clause ||' AND '||pv_numero_primo_2||' IN ('||pv_patron6||')';
			end if;	
			
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('200. '||substr(xv_where_clause,1,250));
			end if;
			
		end if;
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

exception	
  when le$pos1_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B1');
	raise;
  when le$pos2_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B2');
	raise;
  when le$pos3_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B3');
	raise;
  when le$pos4_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B4');
	raise;
  when le$pos5_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B5');
	raise;
  when le$pos6_invalid then
	dbms_output.put_line('Invalid relational operator en la posicion B6');
	raise;
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;
end get_panorama_where_clause;
*/

							


--!imprimir las jugadas intermedias de gigaloterias
procedure imprimir_jugadas_gl (pn_drawing_id               	NUMBER
							 , pn_drawing_case				NUMBER
                             , pv_show_init_values			VARCHAR2
							 , pltbl_list_array_pos1 		gt$gl_tbl
							 , pltbl_list_array_pos2 		gt$gl_tbl
							 , pltbl_list_array_pos3 		gt$gl_tbl
							 , pltbl_list_array_pos4 		gt$gl_tbl
							 , pltbl_list_array_pos5 		gt$gl_tbl
							 , pltbl_list_array_pos6 		gt$gl_tbl
							  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'imprimir_jugadas_gl';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pltbl_list_array_pos1: '||pltbl_list_array_pos1.count);
		dbms_output.put_line('pltbl_list_array_pos2: '||pltbl_list_array_pos2.count);
		dbms_output.put_line('pltbl_list_array_pos3: '||pltbl_list_array_pos3.count);
		dbms_output.put_line('pltbl_list_array_pos4: '||pltbl_list_array_pos4.count);
		dbms_output.put_line('pltbl_list_array_pos5: '||pltbl_list_array_pos5.count);
		dbms_output.put_line('pltbl_list_array_pos6: '||pltbl_list_array_pos6.count);
   end if;  

	if pv_show_init_values = CV$ENABLE and GB$SHOW_PROC_NAME then
		if pltbl_list_array_pos1.count > 0 then
			for a in pltbl_list_array_pos1.first..pltbl_list_array_pos1.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos1(a).b_type,'B','P')||'-'||
									 pltbl_list_array_pos1(a).digit||'-'||
									 pltbl_list_array_pos1(a).frec||'-'||
									 pltbl_list_array_pos1(a).lt||'-'||
									 pltbl_list_array_pos1(a).ca||'-'||
									 nvl(pltbl_list_array_pos1(a).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos1(a).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos1(a).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;

		if pltbl_list_array_pos2.count > 0 then
			for b in pltbl_list_array_pos2.first..pltbl_list_array_pos2.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos2(b).b_type,'B','P')||'-'||
									 pltbl_list_array_pos2(b).digit||'-'||
									 pltbl_list_array_pos2(b).frec||'-'||
									 pltbl_list_array_pos2(b).lt||'-'||
									 pltbl_list_array_pos2(b).ca||'-'||
									 nvl(pltbl_list_array_pos2(b).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos2(b).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos2(b).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;

		if pltbl_list_array_pos3.count > 0 then
			for c in pltbl_list_array_pos3.first..pltbl_list_array_pos3.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos3(c).b_type,'B','P')||'-'||
									 pltbl_list_array_pos3(c).digit||'-'||
									 pltbl_list_array_pos3(c).frec||'-'||
									 pltbl_list_array_pos3(c).lt||'-'||
									 pltbl_list_array_pos3(c).ca||'-'||
									 nvl(pltbl_list_array_pos3(c).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos3(c).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos3(c).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;

		if pltbl_list_array_pos4.count > 0 then
			for d in pltbl_list_array_pos4.first..pltbl_list_array_pos4.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos4(d).b_type,'B','P')||'-'||
									 pltbl_list_array_pos4(d).digit||'-'||
									 pltbl_list_array_pos4(d).frec||'-'||
									 pltbl_list_array_pos4(d).lt||'-'||
									 pltbl_list_array_pos4(d).ca||'-'||
									 nvl(pltbl_list_array_pos4(d).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos4(d).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos4(d).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;

		if pltbl_list_array_pos5.count > 0 then
			for e in pltbl_list_array_pos5.first..pltbl_list_array_pos5.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos5(e).b_type,'B','P')||'-'||
									 pltbl_list_array_pos5(e).digit||'-'||
									 pltbl_list_array_pos5(e).frec||'-'||
									 pltbl_list_array_pos5(e).lt||'-'||
									 pltbl_list_array_pos5(e).ca||'-'||
									 nvl(pltbl_list_array_pos5(e).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos5(e).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos5(e).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;

		if pltbl_list_array_pos6.count > 0 then
			for f in pltbl_list_array_pos6.first..pltbl_list_array_pos6.last loop
				dbms_output.put_line(pn_drawing_case||'-'||
									 replace(pltbl_list_array_pos6(f).b_type,'B','P')||'-'||
									 pltbl_list_array_pos6(f).digit||'-'||
									 pltbl_list_array_pos6(f).frec||'-'||
									 pltbl_list_array_pos6(f).lt||'-'||
									 pltbl_list_array_pos6(f).ca||'-'||
									 nvl(pltbl_list_array_pos6(f).pxc,0)||'-'||
									 nvl(pltbl_list_array_pos6(f).preferencia_flag,'#')||'-'||
									 pltbl_list_array_pos6(f).rlt||'-'||
									 pn_drawing_id
									);
			end loop;
		end if;
	end if;	
exception	
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;
end imprimir_jugadas_gl;   


--!recuperando info de la tabl plan_jugadas en un arreglo						
procedure get_tabla_info (pn_case                     NUMBER
					    , pv_description              VARCHAR2
						, xtbl_row	    IN OUT NOCOPY dbms_sql.varchar2_table
					 	, x_err_code    IN OUT NOCOPY NUMBER
						   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_tabla_info';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_description: '||pv_description);
   end if;

	--!recuperando info de la tabla plan_jugadas
	select POS1||','||POS2||','||POS3||','||POS4||','||POS5||','||POS6 str
	  into olap_sys.w_common_pkg.g_dml_stmt
	  from olap_sys.plan_jugadas
     where description  = pv_description
       and drawing_case = pn_case; 
	
	--!arreglo para convertir un string separado por comas en renglones de un query   
    olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => olap_sys.w_common_pkg.g_dml_stmt
                                                 , xtbl_row   => xtbl_row
								                 , x_err_code => x_err_code
                                                  );  
	
--	dbms_output.put_line('array decenas: '||xtbl_row.count);																																																																																																																																																																																																																																																																																																																																																																																													|);
	
	if xtbl_row.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;
	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_tabla_info;


--!recuperando info de la tabl plan_jugadas en un arreglo						
procedure get_tabla_info (pn_case                     NUMBER
					    , pv_description              VARCHAR2
						, xtbl_row	    IN OUT NOCOPY gt$plan_tbl
					 	, x_err_code    IN OUT NOCOPY NUMBER
						   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_tabla_info';
  pragma autonomous_transaction;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_description: '||pv_description);
   end if;

	--!recuperando info de la tabla plan_jugadas
	select POS1
	     , POS2
		 , POS3
		 , POS4
		 , POS5
		 , POS6
	  bulk collect into xtbl_row
	  from olap_sys.plan_jugadas
     where description  = pv_description
       and drawing_case = pn_case; 
	

--	dbms_output.put_line('array decenas: '||xtbl_row.count);																																																																																																																																																																																																																																																																																																																																																																																													|);
	
	if xtbl_row.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;
	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_tabla_info;

--!validar la sincronizacion entre decenas y patron de numeros						
procedure validar_decenas_patron_numeros (ptbl_row	  				   			dbms_sql.varchar2_table
										, pv_pos1					   			VARCHAR2
										, pv_pos2					   			VARCHAR2
										, pv_pos3					   			VARCHAR2
										, pv_pos4					   			VARCHAR2
										, pv_pos5					   			VARCHAR2
										, pv_pos6					   			VARCHAR2
									    , x_err_code    IN OUT NOCOPY NUMBER
									     ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_decenas_patron_numeros';
  CN$TERMINACION_NUEVE    constant number(1) := 9;
  CN$POSICION_1    		  constant number(1) := 1;  
  CN$POSICION_2    		  constant number(1) := 2;
  CN$POSICION_3    		  constant number(1) := 3;
  CN$POSICION_4    		  constant number(1) := 4;
  CN$POSICION_5    		  constant number(1) := 5;
  CN$POSICION_6    		  constant number(1) := 6;
  lv$terminacion_digito            varchar2(1);
  lv$patron_numero                 varchar2(10);
  lv$decena                        varchar2(10);
  ln$decena_ini                    number(2) := 0;
  ln$decena_end                    number(2) := 0;
  ln$posicion_digito			   number(1) := 0;
  ln$siguiente_posicion			   number(1) := 0;	  
  
  le$posicion_digit_invalido       exception;
  le$rango_decena_invalido         exception;
  le$patron_numero_duplicado       exception;

	--!obtener la tersminacion del digito
	function get_terminacion_digito (pv_pos    VARCHAR2) return NUMBER is
	begin
		--dbms_output.put_line('pv_pos: '||pv_pos||', terminacion: '||to_number(substr(lpad(pv_pos,2,'0'),2,1)));
		return to_number(substr(lpad(pv_pos,2,'0'),2,1));
	end get_terminacion_digito;
	
	procedure get_rango_decena(pv_decena          			VARCHAR2
	                         , pn_decena_ini  IN OUT NOCOPY NUMBER
							 , pn_decena_end  IN OUT NOCOPY NUMBER
							  ) is
	begin
		pn_decena_ini := to_number(substr(pv_decena,1,instr(pv_decena,'-',1,1)-1));
		pn_decena_end := to_number(substr(pv_decena,instr(pv_decena,'-',1,1)+1));
		--dbms_output.put_line('pv_decena:'||pv_decena||' pn_decena_ini: '||pn_decena_ini||' pn_decena_end: '||pn_decena_end);
	end get_rango_decena;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

--1-9	1-9     10-19	10-19	20-29	30-39						
--1-9   1-9     10-19   20-29   20-29   30-39						
--1-9   10-19	10-19   20-29   20-29   30-39						
--1-9   10-19	10-19   10-19   20-29   30-39

	--!validando posicion 1
	if pv_pos1 is not null then
		ln$posicion_digito := CN$POSICION_1;
		dbms_output.put_line('validando posicion '||ln$posicion_digito||': '||pv_pos1);
		lv$patron_numero := pv_pos1;
		get_rango_decena(pv_decena     => ptbl_row(CN$POSICION_1)
	                   , pn_decena_ini => ln$decena_ini
					   , pn_decena_end => ln$decena_end
						);
		--!validar rango de la decenas
		if to_number(pv_pos1) between ln$decena_ini and ln$decena_end then				
			--!validar la terminacion del digito en base a la decena
			if get_terminacion_digito (pv_pos => pv_pos1) = CN$TERMINACION_NUEVE then
				if ptbl_row(CN$POSICION_1) = ptbl_row(CN$POSICION_2) then
					lv$decena 			  := ptbl_row(CN$POSICION_2);
					ln$siguiente_posicion := CN$POSICION_2;
					raise le$posicion_digit_invalido;
				end if;
			end if;
		else
			lv$decena := ptbl_row(CN$POSICION_1);
			raise le$rango_decena_invalido;
		end if;
		--!validar que no haya patrones nes digitos duplicados
		if (to_number(pv_pos1) = to_number(nvl(pv_pos2,'0')))
		or (to_number(pv_pos1) = to_number(nvl(pv_pos3,'0')))
		or (to_number(pv_pos1) = to_number(nvl(pv_pos4,'0')))
		or (to_number(pv_pos1) = to_number(nvl(pv_pos5,'0')))
		or (to_number(pv_pos1) = to_number(nvl(pv_pos6,'0'))) then
			raise le$patron_numero_duplicado;
		end if;
	end if;

	--!validando posicion 2
	if pv_pos2 is not null then
		ln$posicion_digito := CN$POSICION_2;
		dbms_output.put_line('validando posicion '||ln$posicion_digito||': '||pv_pos2);
		lv$patron_numero := pv_pos2;
		get_rango_decena(pv_decena     => ptbl_row(CN$POSICION_2)
	                   , pn_decena_ini => ln$decena_ini
					   , pn_decena_end => ln$decena_end
						);
		--!validar rango de la decenas
		if to_number(pv_pos2) between ln$decena_ini and ln$decena_end then				
			--!validar la terminacion del digito en base a la decena
			if get_terminacion_digito (pv_pos => pv_pos2) = CN$TERMINACION_NUEVE then
				if ptbl_row(CN$POSICION_2) = ptbl_row(CN$POSICION_3) then
					lv$decena 			  := ptbl_row(CN$POSICION_3);
					ln$siguiente_posicion := CN$POSICION_3;
					raise le$posicion_digit_invalido;
				end if;
			end if;
		else
			lv$decena := ptbl_row(CN$POSICION_2);
			raise le$rango_decena_invalido;
		end if;
		--!validar que no haya patrones nes digitos duplicados
		if (to_number(pv_pos2) = to_number(nvl(pv_pos1,'0')))
		or (to_number(pv_pos2) = to_number(nvl(pv_pos3,'0')))
		or (to_number(pv_pos2) = to_number(nvl(pv_pos4,'0')))
		or (to_number(pv_pos2) = to_number(nvl(pv_pos5,'0')))
		or (to_number(pv_pos2) = to_number(nvl(pv_pos6,'0'))) then
			raise le$patron_numero_duplicado;
		end if;
	end if;

	--!validando posicion 3
	if pv_pos3 is not null then
		ln$posicion_digito := CN$POSICION_3;
		dbms_output.put_line('validando posicion '||ln$posicion_digito||': '||pv_pos3);
		lv$patron_numero := pv_pos3;
		get_rango_decena(pv_decena     => ptbl_row(CN$POSICION_3)
	                   , pn_decena_ini => ln$decena_ini
					   , pn_decena_end => ln$decena_end
						);
		--!validar rango de la decenas
		if to_number(pv_pos3) between ln$decena_ini and ln$decena_end then				
			--!validar la terminacion del digito en base a la decena
			if get_terminacion_digito (pv_pos => pv_pos3) = CN$TERMINACION_NUEVE then
				if ptbl_row(CN$POSICION_3) = ptbl_row(CN$POSICION_4) then
					lv$decena 			  := ptbl_row(CN$POSICION_4);
					ln$siguiente_posicion := CN$POSICION_4;
					raise le$posicion_digit_invalido;
				end if;
			end if;
		else
			lv$decena := ptbl_row(CN$POSICION_3);
			raise le$rango_decena_invalido;
		end if;
		--!validar que no haya patrones nes digitos duplicados
		if (to_number(pv_pos3) = to_number(nvl(pv_pos1,'0')))
		or (to_number(pv_pos3) = to_number(nvl(pv_pos2,'0')))
		or (to_number(pv_pos3) = to_number(nvl(pv_pos4,'0')))
		or (to_number(pv_pos3) = to_number(nvl(pv_pos5,'0')))
		or (to_number(pv_pos3) = to_number(nvl(pv_pos6,'0'))) then
			raise le$patron_numero_duplicado;
		end if;
	end if;

	--!validando posicion 4
	if pv_pos4 is not null then
		ln$posicion_digito := CN$POSICION_4;
		dbms_output.put_line('validando posicion '||ln$posicion_digito||': '||pv_pos4);
		lv$patron_numero := pv_pos4;
		get_rango_decena(pv_decena     => ptbl_row(CN$POSICION_4)
	                   , pn_decena_ini => ln$decena_ini
					   , pn_decena_end => ln$decena_end
						);
		--!validar rango de la decenas
		if to_number(pv_pos4) between ln$decena_ini and ln$decena_end then				
			--!validar la terminacion del digito en base a la decena
			if get_terminacion_digito (pv_pos => pv_pos4) = CN$TERMINACION_NUEVE then
				if ptbl_row(CN$POSICION_4) = ptbl_row(CN$POSICION_5) then
					lv$decena 			  := ptbl_row(CN$POSICION_5);
					ln$siguiente_posicion := CN$POSICION_5;
					raise le$posicion_digit_invalido;
				end if;
			end if;
		else
			lv$decena := ptbl_row(CN$POSICION_4);
			raise le$rango_decena_invalido;
		end if;
		--!validar que no haya patrones nes digitos duplicados
		if (to_number(pv_pos4) = to_number(nvl(pv_pos1,'0')))
		or (to_number(pv_pos4) = to_number(nvl(pv_pos2,'0')))
		or (to_number(pv_pos4) = to_number(nvl(pv_pos3,'0')))
		or (to_number(pv_pos4) = to_number(nvl(pv_pos5,'0')))
		or (to_number(pv_pos4) = to_number(nvl(pv_pos6,'0'))) then
			raise le$patron_numero_duplicado;
		end if;
	end if;

	--!validando posicion 5
	if pv_pos5 is not null then
		ln$posicion_digito := CN$POSICION_5;
		dbms_output.put_line('validando posicion '||ln$posicion_digito||': '||pv_pos5);
		lv$patron_numero := pv_pos5;
		get_rango_decena(pv_decena     => ptbl_row(CN$POSICION_5)
	                   , pn_decena_ini => ln$decena_ini
					   , pn_decena_end => ln$decena_end
						);
		--!validar rango de la decenas
		if to_number(pv_pos5) between ln$decena_ini and ln$decena_end then				
			--!validar la terminacion del digito en base a la decena
			if get_terminacion_digito (pv_pos => pv_pos5) = CN$TERMINACION_NUEVE then
				if ptbl_row(CN$POSICION_5) = ptbl_row(CN$POSICION_6) then
					lv$decena 			  := ptbl_row(CN$POSICION_6);
					ln$siguiente_posicion := CN$POSICION_6;
					raise le$posicion_digit_invalido;
				end if;
			end if;
		else
			lv$decena := ptbl_row(CN$POSICION_5);
			raise le$rango_decena_invalido;
		end if;
		--!validar que no haya patrones nes digitos duplicados
		if (to_number(pv_pos5) = to_number(nvl(pv_pos1,'0')))
		or (to_number(pv_pos5) = to_number(nvl(pv_pos2,'0')))
		or (to_number(pv_pos5) = to_number(nvl(pv_pos3,'0')))
		or (to_number(pv_pos5) = to_number(nvl(pv_pos4,'0')))
		or (to_number(pv_pos5) = to_number(nvl(pv_pos6,'0'))) then
			raise le$patron_numero_duplicado;
		end if;
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when le$patron_numero_duplicado then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||lv$patron_numero||' ya existe en otra posicion');     
	raise;   
  when le$rango_decena_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||lv$patron_numero||' en la posocion ('||ln$posicion_digito||') esta fuera del rango de la decena '||lv$decena);     
	raise;   
  when le$posicion_digit_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||lv$patron_numero||' en la posicion ('||ln$posicion_digito||') no puede ser configurado en esta posicion debido a que la siguiente decena en la posicion ('||ln$siguiente_posicion||') es '||lv$decena);     
	raise;    
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end validar_decenas_patron_numeros;


--!validar la sincronizacion entre configuracion de primos, pares y nones, y patron de numeros						
procedure validar_config_patron_numeros (ptbl_row	  				   			dbms_sql.varchar2_table
										 --!patrones de numeros
									   , pv_patron1        					VARCHAR2
									   , pv_patron2        					VARCHAR2
									   , pv_patron3        					VARCHAR2
									   , pv_patron4        					VARCHAR2
									   , pv_patron5        					VARCHAR2
									   , pv_patron6        					VARCHAR2										
									   , x_err_code    IN OUT NOCOPY NUMBER
									    ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_config_patron_numeros';
  ln$patron						   number;
  le$numero_primo_invalido	       exception;
  le$numero_par_invalido	       exception;
  le$numero_non_invalido	       exception;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	gv$tmp_list := nvl(pv_patron1,'0')||','||nvl(pv_patron2,'0')||','||nvl(pv_patron3,'0')||','||nvl(pv_patron4,'0')||','||nvl(pv_patron5,'0')||','||nvl(pv_patron6,'0');
	dbms_output.put_line('PATRON_NUMEROS: '||gv$tmp_list);
	
    --!validando la posicion de los numeros primos en el select list
    olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => gv$tmp_list
                                                 , xtbl_row   => gtbl$row_target 
								                 , x_err_code => x_err_code
                                                  );    

	dbms_output.put_line('---------');
    --!validando posicion y cantidad de numeros primos
	for i in gtbl$row_target.first..gtbl$row_target.last loop
		dbms_output.put_line(i||' : '||gtbl$row_target(i));
		dbms_output.put_line(i||' # '||ptbl_row(i));
		if to_number(gtbl$row_target(i)) > 0 then
			ln$patron := to_number(gtbl$row_target(i));
			--!validar numero primo
			if ptbl_row(i) = CV$NUMERO_PRIMO then				
				if olap_sys.w_common_pkg.is_prime_number (pn_digit => to_number(ln$patron)) = 0 then
					raise le$numero_primo_invalido;
				end if;				
			else
				--!validar numero non
				if ptbl_row(i) = CV$NUMERO_NON then				
					if mod(ln$patron,2) = 0 then
						raise le$numero_non_invalido;
					end if;				
				end if;
			end if;		

			--!validar numero par
			if ptbl_row(i) = CV$NUMERO_PAR then				
				if mod(ln$patron,2) = 1 then
					raise le$numero_par_invalido;
				end if;				
			end if;					
		end if;
	end loop;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when le$numero_primo_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||ln$patron||' debe ser un numero primo');     
	raise;   
  when le$numero_par_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||ln$patron||' debe ser un numero par'); 
	raise;   
  when le$numero_non_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El digito '||ln$patron||' debe ser un numero non'); 
	raise;    
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end validar_config_patron_numeros;


--!validar la configuracion de 2 primos, pares y nones						
procedure validar_primos_pares_nones (pv_pos1					  VARCHAR2
									, pv_pos2					  VARCHAR2
									, pv_pos3					  VARCHAR2
								    , x_err_code    IN OUT NOCOPY NUMBER
								     ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_primos_pares_nones';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	--!validando que existan solo 2 numeros primos en el plan de jugadas
	if to_number(pv_pos1) = 2 then
		--!validando que el plan de juagadas solo contenga (3 pares, 1 non) o (2 pares, 2 nones)
		if not (to_number(pv_pos2) = 3 and to_number(pv_pos3) = 1) or  not (to_number(pv_pos2) = 2 and to_number(pv_pos3) = 2) then
			raise ge$cant_num_par_non_invalido;
		end if;
	else
		raise ge$cant_numero_primo_invalido;
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when ge$cant_numero_primo_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El plan de jugadas debe contener 2 numeros primos');     
	raise;    
  when ge$cant_num_par_non_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El plan de jugadas solo puede estar basado en (3 pares, 1 non) o (2 pares, 2 nones)');     
	raise;    
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end validar_primos_pares_nones;


--!validar la configuracion de last terminaciones repetidas						
procedure validar_terminacion_repetida (pv_pos1					  VARCHAR2
								      , x_err_code    IN OUT NOCOPY NUMBER
								      ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_terminacion_repetida';
  le$termnacion_invalida           exception;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	--!validando que existan solo 2 numeros primos en el plan de jugadas
	if to_number(pv_pos1) not between 0 and 2 then
		raise le$termnacion_invalida;
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when le$termnacion_invalida then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'Los numeros validos para las terminaciones repetidas son (0,1,2)');     
	raise;      
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end validar_terminacion_repetida;


--!validar que la configuracion de primos, pares y nones este basada en 2 numeros primos					
procedure validar_config_primo_par_non (ptbl_row	  				gt$plan_tbl
									  , pv_conf_ppn1				VARCHAR2
									  , pv_conf_ppn2				VARCHAR2
									  , pv_conf_ppn3				VARCHAR2
									  , pv_conf_ppn4				VARCHAR2
									  , pv_conf_ppn5				VARCHAR2
									  , pv_conf_ppn6				VARCHAR2
								      , x_err_code    IN OUT NOCOPY NUMBER
								       ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'validar_config_primo_par_non';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	gv$tmp_list := pv_conf_ppn1||','||pv_conf_ppn2||','||pv_conf_ppn3||','||pv_conf_ppn4||','||pv_conf_ppn5||','||pv_conf_ppn6;
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('CONFIG_PRIMOS_PARES_NONE: '||gv$tmp_list);
	end if;
	
    --!formando un arreglo con la configuracion de primos, pares y nones
    olap_sys.w_common_pkg.translate_string_to_rows(pv_string  => gv$tmp_list
                                                 , xtbl_row   => gtbl$row_source 
								                 , x_err_code => x_err_code
                                                  );  
	
	
	gn$primo_cnt := 0;
	for k in gtbl$row_source.first..gtbl$row_source.last loop
		if gtbl$row_source(k) = CV$NUMERO_PRIMO then
			gn$primo_cnt :=  gn$primo_cnt + 1;
		end if;

		if gtbl$row_source(k) = CV$NUMERO_PAR then
			gn$par_cnt :=  gn$par_cnt + 1;
		end if;
		
		if gtbl$row_source(k) = CV$NUMERO_NON then
			gn$non_cnt :=  gn$non_cnt + 1;
		end if;		
	end loop;
	
	--!validando que la configuracion de jugadas este basada en 2 numeros primos
	for m in ptbl_row.first..ptbl_row.last loop
		if ptbl_row(m).pos1 != gn$primo_cnt then
			raise ge$cant_numero_primo_invalido;
		end if;
	end loop;
	
	--!validando pares y nones en el plan de jugadas
	if not (gn$par_cnt = ptbl_row(1).pos2 and gn$non_cnt = ptbl_row(1).pos3) or  not (gn$par_cnt = ptbl_row(2).pos2 and gn$non_cnt = ptbl_row(1).pos3) then
		raise ge$cant_num_par_non_invalido;
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when ge$cant_numero_primo_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El plan de jugadas debe contener 2 numeros primos');     
	raise;    
  when ge$cant_num_par_non_invalido then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    raise_application_error(-20005,'El plan de jugadas solo puede estar basado en (3 pares, 1 non) o (2 pares, 2 nones)');     
	raise;
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end validar_config_primo_par_non;


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
								  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'table_validator_handler';
  ltbl$plan_jugadas                gt$plan_tbl;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	if upper(pv_description) = 'PATRON_NUMEROS' then						 
		gtbl$row_source.delete;
		--!recuperando las decenas por posicion en un arreglo						
		get_tabla_info (pn_case        => pn_case
					  , pv_description => 'DECENAS'
					  , xtbl_row	   => gtbl$row_source
					  , x_err_code     => x_err_code
					   );

		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then	
			--!validar la sincronizacion entre decenas y patron de numeros						
			validar_decenas_patron_numeros (ptbl_row   => gtbl$row_source
										  , pv_pos1	   => pv_pos1
										  , pv_pos2	   => pv_pos2
										  , pv_pos3	   => pv_pos3
										  , pv_pos4	   => pv_pos4
										  , pv_pos5	   => pv_pos5
										  , pv_pos6	   => pv_pos6
										  , x_err_code => x_err_code
										   );							   

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				gtbl$row_source.delete;
				--!recuperando la info relacionada a la configuracion de primos, pares y nones en un arreglo						
				get_tabla_info (pn_case        => pn_case
							  , pv_description => 'CONFIG_PRIMOS_PARES_NONES'
							  , xtbl_row	   => gtbl$row_source
							  , x_err_code     => x_err_code
							   );

				--!validar la sincronizacion entre configuracion de primos, pares y nones, y patron de numeros						
				validar_config_patron_numeros (ptbl_row	  => gtbl$row_source
											   --!patrones de numeros
										     , pv_patron1 => pv_pos1
										     , pv_patron2 => pv_pos2
										     , pv_patron3 => pv_pos3
										     , pv_patron4 => pv_pos4
										     , pv_patron5 => pv_pos5
										     , pv_patron6 => pv_pos6										
										     , x_err_code => x_err_code
											  );

			end if;
		end if;	
	elsif upper(pv_description) = 'PRIMOS_PARES_NONES' then											  
		--!validar la configuracion de 2 primos, pares y nones						
		validar_primos_pares_nones (pv_pos1	   => pv_pos1
								  , pv_pos2	   => pv_pos2
								  , pv_pos3	   => pv_pos3
								  , x_err_code => x_err_code
								   );
	elsif upper(pv_description) = 'CONFIG_PRIMOS_PARES_NONES' then
			gtbl$row_source.delete;
			--!recuperando la info relacionada a la configuracion de primos, pares y nones en un arreglo						
			get_tabla_info (pn_case        => pn_case
						  , pv_description => 'PRIMOS_PARES_NONES'
						  , xtbl_row	   => ltbl$plan_jugadas
						  , x_err_code     => x_err_code
						   );

		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then											  						
			--!validar que la configuracion de primos, pares y nones este basada en 2 numeros primos					
			validar_config_primo_par_non (ptbl_row	   => ltbl$plan_jugadas
										, pv_conf_ppn1 => pv_pos1
										, pv_conf_ppn2 => pv_pos2
										, pv_conf_ppn3 => pv_pos3
										, pv_conf_ppn4 => pv_pos4
										, pv_conf_ppn5 => pv_pos5
										, pv_conf_ppn6 => pv_pos6
										, x_err_code   => x_err_code
										 );
		end if;									   
	elsif upper(pv_description) = 'TERMINACIONES_REPETIDAS' then											  
		--!validar la configuracion de last terminaciones repetidas						
		validar_terminacion_repetida (pv_pos1	 => pv_pos1
								    , x_err_code => x_err_code
								     );
	else
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION; 
	end if;	
	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end table_validator_handler;


--!si las decenas continuas son iguales y el digito de la 1a decena es 9 regresa false
--!de lo contrario regresa true y el numero se agrega a la lista de numeros
function valida_ultimo_digito_decena (pv_digit                         VARCHAR2
                                    , pv_decena                        VARCHAR2
									, pv_next_decena                   VARCHAR2 
									, pv_b_type                        VARCHAR2
                                     ) return boolean is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'valida_ultimo_digito_decena';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line('fnc: '||LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_b_type: '||pv_b_type);
		dbms_output.put_line('pv_digit: '||pv_digit);
		dbms_output.put_line('pv_decena: '||pv_decena);
		dbms_output.put_line('pv_next_decena: '||pv_next_decena);
   end if;
	
	--!si la decena inicial es igual a la siguiente y la decena inicial el numero termina en 9 entonces se remueve ese numero
	--!terminado en 9 de la lista. Ejemplo: el numero 19 se removera de la lista de la decena inicial cuando ambas decenas sean 10-19
	if substr(lpad(pv_digit,2,0),2,1) = '9' and pv_decena = pv_next_decena then
		--!cuando la decena sea 30-39 no se aplicara la regla anterior debido a que nunca podria seleccionarse el numero 39
		--!en la decena inicial
		if instr(pv_decena,'39',1,1)> 0 then
			return true;
		else	
			if GB$SHOW_PROC_NAME then
				dbms_output.put_line('removiendo: '||pv_digit||' de '||pv_b_type);
			end if;	
			return false;
		end if;
	else	
		return true;
	end if;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
	return false;
end valida_ultimo_digito_decena;  

--!proceso para obtener la lista inicial de numeros del plan de jugadas
procedure get_digit_list(pv_drawing_type                  VARCHAR2
                       , pn_drawing_case                  NUMBER
					   , pn_drawing_id                    NUMBER
					   , pv_b_type                        VARCHAR2  	 
                       , pv_decena                        VARCHAR2  	 
					   , pv_ca                            VARCHAR2 
					   , pv_conf_lt						  VARCHAR2
					   , pv_next_decena                   VARCHAR2 
					   , pv_conf_ppn                      VARCHAR2 
					   , pv_conf_ppn_enable               VARCHAR2
                       , pv_save_qry_enable				  VARCHAR2
					   , pv_chng_criteria_pos			  VARCHAR2
					   , pv_cambios_gl_enable			  VARCHAR2
					   , pv_conf_frec		  			  VARCHAR2
					   , pv_frec_enable					  VARCHAR2
					   , xv_digit_list      IN OUT NOCOPY VARCHAR2       					   
					   , x_err_code         IN OUT NOCOPY NUMBER
						) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_digit_list';
  lrc$ref_cursor          		   SYS_REFCURSOR;
  lv$digit_str                     varchar2(30) := null;
  lv$digit_list                    varchar2(30) := null;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;


   olap_sys.w_common_pkg.g_dml_stmt := 'select '||pn_drawing_case||'||'||chr(39)||'-'||chr(39)||'||replace(b_type,'||chr(39)||'B'||chr(39)||','||chr(39)||'P'||chr(39)||')||'||chr(39)||'-'||chr(39)||'||digit||'||chr(39)||'-'||chr(39)||'||decode(ley_tercio,0,'||chr(39)||'B'||chr(39)||',1,'||chr(39)||'G'||chr(39)||','||chr(39)||'R'||chr(39)||')||'||chr(39)||'-'||chr(39)||'||ciclo_aparicion||'||chr(39)||'-'||chr(39)||'||nvl(pronos_ciclo,0)||'||chr(39)||'-'||chr(39)||'||nvl(preferencia_flag,'||chr(39)||'#'||chr(39)||')||'||chr(39)||'-'||chr(39)||'||color_rango_ley_tercio||'||chr(39)||'-'||chr(39)||'||drawing_id str, digit ';
   olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' from olap_sys.s_calculo_stats';
   olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' where drawing_id = '||pn_drawing_id||' and b_type = '||chr(39)||pv_b_type||chr(39)||' and '||pv_decena||' and '||pv_ca;
   
   if pv_conf_lt is not null then
		olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and color_ley_tercio IN ('||pv_conf_lt||')';
   end if;
   
   if pv_conf_ppn is not null and pv_conf_ppn_enable = CV$ENABLE then
		if pv_conf_ppn = CV$NUMERO_PRIMO then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and prime_number_flag = 1';
		elsif pv_conf_ppn = CV$NUMERO_PAR then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and prime_number_flag = 0 and inpar_number_flag = 0';
		elsif pv_conf_ppn = CV$NUMERO_NON then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and prime_number_flag = 0 and inpar_number_flag = 1';
		elsif pv_conf_ppn = CV$NUMERO_COMODIN then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and prime_number_flag = 0 and inpar_number_flag IN (0,1)';	
		end if;
   end if;
   
   if pv_cambios_gl_enable = CV$ENABLE then
		if pv_chng_criteria_pos is not null then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' AND '||pv_chng_criteria_pos;
		end if;	
   end if;		
 
   if pv_frec_enable = CV$ENABLE then
		if pv_conf_frec is not null then
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and color_ubicacion = '||pv_conf_frec;		
		end if;
   end if;
 
   olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' order by ciclo_aparicion';  

   if pv_save_qry_enable = CV$ENABLE then
		ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt); 
   end if;
     
   --!recuperando numeros iniciales	  
   open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
   loop
		fetch lrc$ref_cursor into lv$digit_str, lv$digit_list;
		exit when lrc$ref_cursor%notfound;
			--!si las decenas continuas son iguales y el digito de la 1a decena es 9 regresa false
			--!de lo contrario regresa true y el numero se agrega a la lista de numeros
		if valida_ultimo_digito_decena (pv_digit       => lv$digit_list
                                      , pv_decena      => pv_decena
									  , pv_next_decena => pv_next_decena
									  , pv_b_type      => pv_b_type
									  ) then
			xv_digit_list :=  xv_digit_list||lv$digit_list||',';
			dbms_output.put_line(lv$digit_str);
--			dbms_output.put_line('into <-> '||xv_digit_list);
		end if;	
   end loop;
   close lrc$ref_cursor;
   
--   dbms_output.put_line('out <-> '||xv_digit_list);
   --!eliminando la ultima coma
   xv_digit_list := substr(xv_digit_list,1,length(xv_digit_list)-1);
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line(pv_b_type||' <-> '||xv_digit_list);
   end if;

exception
  when others then
    close lrc$ref_cursor;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_digit_list;


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
							   , pv_frec_enable         	  VARCHAR2 DEFAULT 'Y'								   
							   , x_err_code     IN OUT NOCOPY NUMBER
						        ) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'initial_digits_handler';       
  lv$b_type                        varchar2(2);
  lv$digit_list                    varchar2(200);
  lv$decena                        varchar2(100);
  lv$ca                            varchar2(100);
  lv$lt                            varchar2(100);
  lv$next_decena                   varchar2(100);
  lv$conf_ppn                      varchar2(100);
  lv$digit_list1                   varchar2(100);
  lv$digit_list2                   varchar2(100);
  lv$digit_list3                   varchar2(100);
  lv$digit_list4                   varchar2(100);
  lv$digit_list5                   varchar2(100);
  lv$digit_list6                   varchar2(100);
  lv$chng_criteria_pos			   varchar2(100);
  lv$conf_frec					   varchar2(100);
  ltbl$conf_lt                     gt$plan_tbl;	
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
   
	dbms_output.put_line('Parametros de entrada');
	dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
	dbms_output.put_line('pv_lt_enable: '||pv_lt_enable);
	dbms_output.put_line('pv_conf_ppn_enable: '||pv_conf_ppn_enable);
	dbms_output.put_line('pv_cambios_gl_enable: '||pv_cambios_gl_enable);
	dbms_output.put_line('pv_save_qry_enable: '||pv_save_qry_enable);
	dbms_output.put_line('pv_frec_enable: '||pv_frec_enable);
	dbms_output.put_line('--------------------------------');
	
	--!inicializando variables globales
	initialize_global_variables;
		
	--!recuperar el ID del ultimo sorteo
	gn$drawing_id := get_max_drawing_id(pv_drawing_type => pv_drawing_type);

	--!proceso para obtener las decenas del plan de juego
	get_plan_jugada_decenas(pv_drawing_type => pv_drawing_type
						  , pn_drawing_case => pn_drawing_case
						  , xv_d1           => gv$d1
						  , xv_d2           => gv$d2
						  , xv_d3           => gv$d3
						  , xv_d4           => gv$d4
						  , xv_d5           => gv$d5
						  , xv_d6           => gv$d6
						  , xv_decena_rank  => gv$decena_rank
						  , x_err_code      => x_err_code
						   ); 

	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		--!proceso para obtener rango del ciclo de aparicion del plan de jugadas
		get_plan_jugada_ca(pv_drawing_type => pv_drawing_type		
						 , pn_drawing_case => pn_drawing_case
						 , xv_ca1          => gv$ca1
						 , xv_ca2          => gv$ca2
						 , xv_ca3          => gv$ca3
						 , xv_ca4          => gv$ca4
						 , xv_ca5          => gv$ca5
						 , xv_ca6          => gv$ca6	
						 , x_err_code      => x_err_code
						  );				   

		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			if upper(pv_lt_enable) = CV$ENABLE then
				--!proceso para ley del tercio del plan de jugadas
				get_plan_jugada_lt(pv_drawing_type => pv_drawing_type
								 , pn_drawing_case => pn_drawing_case
								  --!ley del tercio
								 , xtbl$conf_lt    => ltbl$conf_lt	
								 , x_err_code      => x_err_code
								  );	
			end if;
			
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			
				if pv_cambios_gl_enable = CV$ENABLE then
				--!proceso para el plan de jugada en base a las columnas auditoras de cambios en gl
				get_plan_jugada_change(pv_drawing_type          => pv_drawing_type
									 , pn_drawing_case          => pn_drawing_case
								     , xv_chng_criteria_pos1	=> gv$chng_criteria_pos1
								     , xv_chng_criteria_pos2	=> gv$chng_criteria_pos2
								     , xv_chng_criteria_pos3	=> gv$chng_criteria_pos3
								     , xv_chng_criteria_pos4	=> gv$chng_criteria_pos4
								     , xv_chng_criteria_pos5	=> gv$chng_criteria_pos5
								     , xv_chng_criteria_pos6	=> gv$chng_criteria_pos6
									 , x_err_code         	    => x_err_code
									  );
				end if;
				
				if pv_frec_enable = CV$ENABLE then
					--!proceso para recuperar el patron de frecuencias del plan de jugadas
					get_plan_jugada_frec(pv_drawing_type   => pv_drawing_type
										 , pn_drawing_case => pn_drawing_case
										 --!frecuencia
									   , xv_conf_frec1     => gv$conf_frec1
									   , xv_conf_frec2     => gv$conf_frec2
									   , xv_conf_frec3     => gv$conf_frec3
									   , xv_conf_frec4     => gv$conf_frec4
									   , xv_conf_frec5     => gv$conf_frec5
									   , xv_conf_frec6     => gv$conf_frec6
									   , x_err_code        => x_err_code
										);
				end if;
				
				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then				
					olap_sys.w_common_pkg.g_rowcnt := 1;
					for ppn in c_conf_ppn (pv_drawing_type => pv_drawing_type
										 , pn_drawing_case => pn_drawing_case) loop
						for k in ltbl$conf_lt.first..ltbl$conf_lt.last loop
							--dbms_output.put_line('--------------------------------');
							dbms_output.put_line('rec: '||olap_sys.w_common_pkg.g_rowcnt||' <-> '||' ultimo sorteo ID: '||gn$drawing_id||' <-> '||'  case: '||pn_drawing_case||' <-> '||ppn.pos1||' - '||ppn.pos2||' - '||ppn.pos3||' - '||ppn.pos4||' - '||ppn.pos5||' - '||ppn.pos6||' <-> '||ltbl$conf_lt(k).pos1||' - '||ltbl$conf_lt(k).pos2||' - '||ltbl$conf_lt(k).pos3||' - '||ltbl$conf_lt(k).pos4||' - '||ppn.pos5||' - '||ltbl$conf_lt(k).pos6);
							--dbms_output.put_line('--------------------------------');

							for t in 1..6 loop
									if t = 1 then
										lv$decena      := gv$d1;
										lv$ca          := gv$ca1;
										lv$lt          := ltbl$conf_lt(k).pos1;
										lv$next_decena := gv$d2;
										lv$conf_ppn    := ppn.pos1; 
										lv$chng_criteria_pos := gv$chng_criteria_pos1;
										lv$conf_frec   := gv$conf_frec1;
										lv$b_type      := 'B1';
									elsif t = 2 then
										lv$decena      := gv$d2;
										lv$ca          := gv$ca2;
										lv$lt          := ltbl$conf_lt(k).pos2;
										lv$next_decena := gv$d3;
										lv$conf_ppn    := ppn.pos2; 
										lv$chng_criteria_pos := gv$chng_criteria_pos2;
										lv$conf_frec   := gv$conf_frec2;
										lv$b_type      := 'B2';
									elsif t = 3 then
										lv$decena      := gv$d3;
										lv$ca          := gv$ca3;
										lv$lt          := ltbl$conf_lt(k).pos3;
										lv$conf_ppn    := ppn.pos3; 										
										lv$next_decena := gv$d4;
										lv$chng_criteria_pos := gv$chng_criteria_pos3;
										lv$conf_frec   := gv$conf_frec3;
										lv$b_type      := 'B3';
									elsif t = 4 then
										lv$decena      := gv$d4;
										lv$ca          := gv$ca4;
										lv$lt          := ltbl$conf_lt(k).pos4;
										lv$next_decena := gv$d5;
										lv$conf_ppn    := ppn.pos4; 
										lv$chng_criteria_pos := gv$chng_criteria_pos4;
										lv$conf_frec   := gv$conf_frec4;
										lv$b_type      := 'B4';
									elsif t = 5 then
										lv$decena      := gv$d5;
										lv$ca          := gv$ca5;
										lv$lt          := ltbl$conf_lt(k).pos5;
										lv$next_decena := gv$d6;
										lv$conf_ppn    := ppn.pos5; 
										lv$chng_criteria_pos := gv$chng_criteria_pos5;
										lv$conf_frec   := gv$conf_frec5;
										lv$b_type      := 'B5';
									elsif t = 6 then
										lv$decena      := gv$d6;
										lv$ca          := gv$ca6;
										lv$lt          := ltbl$conf_lt(k).pos6;
										lv$conf_ppn    := ppn.pos6; 
										lv$chng_criteria_pos := gv$chng_criteria_pos6;
										lv$conf_frec   := gv$conf_frec6;
										lv$b_type      := 'B6';
									end if;	
									
									lv$digit_list := null;
									--!proceso para obtener la lista inicial de numeros del plan de jugadas
									get_digit_list(pv_drawing_type    => pv_drawing_type
												 , pn_drawing_case    => pn_drawing_case
												 , pn_drawing_id      => gn$drawing_id
												 , pv_b_type          => lv$b_type 	 
												 , pv_decena          => lv$decena  	 
												 , pv_ca              => lv$ca
												 , pv_conf_lt         => lv$lt
												 , pv_next_decena     => lv$next_decena
												 , pv_conf_ppn        => lv$conf_ppn
												 , pv_conf_ppn_enable => pv_conf_ppn_enable
												 , pv_save_qry_enable => pv_save_qry_enable
												 , pv_chng_criteria_pos => lv$chng_criteria_pos
												 , pv_cambios_gl_enable => pv_cambios_gl_enable
												 , pv_conf_frec		  => lv$conf_frec
												 , pv_frec_enable     => pv_frec_enable
												 , xv_digit_list      => lv$digit_list     					   
												 , x_err_code         => x_err_code
												  );
												  
									if t = 1 then
										lv$digit_list1 := lv$digit_list;
									elsif t = 2 then
										lv$digit_list2 := lv$digit_list;
									elsif t = 3 then
										lv$digit_list3 := lv$digit_list;
									elsif t = 4 then
										lv$digit_list4 := lv$digit_list;
									elsif t = 5 then
										lv$digit_list5 := lv$digit_list;
									elsif t = 6 then
										lv$digit_list6 := lv$digit_list;
									end if;	
							end loop;	
							dbms_output.put_line('--------------------------------');
							dbms_output.put_line('B1: '||lv$digit_list1);                                                        
							dbms_output.put_line('B2: '||lv$digit_list2);
							dbms_output.put_line('B3: '||lv$digit_list3);
							dbms_output.put_line('B4: '||lv$digit_list4);
							dbms_output.put_line('B5: '||lv$digit_list5);
							dbms_output.put_line('B6: '||lv$digit_list6); 						
						end loop;
						olap_sys.w_common_pkg.g_rowcnt := olap_sys.w_common_pkg.g_rowcnt + 1;   						
					end loop;
				end if;
			end if;
		end if;		
	end if;
	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end initial_digits_handler;							
*/


--|----------------------------------------------------------------------------------------------------------|--
--|                                                                                                          |--
--|	logica con el aproach de obtener los digitos de GL en base a la posicion de los digitos                  |--
--|                                                                                                          |--
--|----------------------------------------------------------------------------------------------------------|--

--!proceso para obtener el string para formar el IN de las frecuencias
procedure get_frecuencia_IN (pv_drawing_type                 	  VARCHAR2
						   , pn_drawing_case                  	  NUMBER
						   , pv_show_init_values				  VARCHAR2 DEFAULT 'Y'
						   --!frecuencia IN
						   , xv_fre_IN1         	IN OUT NOCOPY VARCHAR2
						   , xv_fre_IN2         	IN OUT NOCOPY VARCHAR2
						   , xv_fre_IN3        	IN OUT NOCOPY VARCHAR2
						   , xv_fre_IN4         	IN OUT NOCOPY VARCHAR2
						   , xv_fre_IN5         	IN OUT NOCOPY VARCHAR2
						   , xv_fre_IN6         	IN OUT NOCOPY VARCHAR2
						   , x_err_code    IN OUT NOCOPY NUMBER
						    ) is
							
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_frecuencia_IN';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;

	--!inicializando parametros de salida
	xv_fre_IN1 := null;
	xv_fre_IN2 := null;
	xv_fre_IN3 := null;
	xv_fre_IN4 := null;
	xv_fre_IN5 := null;
	xv_fre_IN6 := null; 
	
	--!decenas
	begin
		SELECT POS1
			 , POS2
			 , POS3
			 , POS4
			 , POS5
			 , POS6
		  INTO xv_fre_IN1
			 , xv_fre_IN2
			 , xv_fre_IN3
			 , xv_fre_IN4
			 , xv_fre_IN5
			 , xv_fre_IN6
		  FROM OLAP_SYS.PLAN_JUGADAS
		 WHERE DRAWING_TYPE = pv_drawing_type
		   AND DESCRIPTION  = 'FRECUENCIA_IN'
		   AND STATUS       = 'A'
		   AND DRAWING_CASE = pn_drawing_case; 

		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
	exception
		when no_data_found then
			x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
			xv_fre_IN1 := null;
			xv_fre_IN2 := null;
			xv_fre_IN3 := null;
			xv_fre_IN4 := null;
			xv_fre_IN5 := null;
			xv_fre_IN6 := null;
	end;

	if pv_show_init_values = CV$ENABLE then
	   dbms_output.put_line('xv_fre_IN1: '||xv_fre_IN1);	
	   dbms_output.put_line('xv_fre_IN2: '||xv_fre_IN2);
	   dbms_output.put_line('xv_fre_IN3: '||xv_fre_IN3);
	   dbms_output.put_line('xv_fre_IN4: '||xv_fre_IN4);
	   dbms_output.put_line('xv_fre_IN5: '||xv_fre_IN5);
	   dbms_output.put_line('xv_fre_IN6: '||xv_fre_IN6);
   end if;
   
   x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
   
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_frecuencia_IN;


procedure get_lista_digitos_por_posicion (pv_drawing_type           		VARCHAR2
										, pn_drawing_id                     NUMBER
										, pn_digit_pos      				NUMBER
										, pv_fre_IN							VARCHAR2
										, pv_lt_IN							VARCHAR2
										, pv_ca								VARCHAR2
										, pv_decena							VARCHAR2
										, pv_next_decena					VARCHAR2 DEFAULT NULL
										, pv_conf_ppn						VARCHAR2
										, pv_qry_stmt   					VARCHAR2
									    , pv_qry_where_stmt         		VARCHAR2
									    , pv_qry_order_stmt         		VARCHAR2
										, pv_numero_primo_list				VARCHAR2
										, pv_add_primo_enable				VARCHAR2
										, pv_chng_posicion_pos				VARCHAR2
										, xtbl_qry_output    IN OUT NOCOPY 	gt$gl_tbl 
										, x_err_code    	 IN OUT NOCOPY 	NUMBER
										 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_lista_digitos_por_posicion';  
  lv$qry_where_stmt  				varchar2(1000);
  ln$lt_IN							number(1) := 0;
  ln$fre_IN							number(1) := 0;
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_digit_pos: '||pn_digit_pos);
		dbms_output.put_line('pv_fre_IN: '||pv_fre_IN);
		dbms_output.put_line('pv_lt_IN: '||pv_lt_IN);
		dbms_output.put_line('pv_ca: '||pv_ca);
		dbms_output.put_line('pv_decena: '||pv_decena);
		dbms_output.put_line('pv_next_decena: '||pv_next_decena);
		dbms_output.put_line('pv_numero_primo_list: '||pv_numero_primo_list);
   end if;

	--!inicializando las variables para formar el query
	olap_sys.w_common_pkg.g_dml_stmt := null;
	lv$qry_where_stmt				 := null;

	--!reemplazando valores del where clause
	--!reemplazando drawing_type
	lv$qry_where_stmt := REPLACE(pv_qry_where_stmt,'<DRAWING_TYPE>', CHR(39)||pv_drawing_type||CHR(39));

	--!reemplazando drawing_id
	lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<DRAWING_ID>', pn_drawing_id);

	--!reemplazando drawing_id
	lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<B_TYPE>', CHR(39)||'B'||pn_digit_pos||CHR(39));
	
	--!reemplazando frecuancia IN
	if pv_fre_IN is null then
		lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<FRECUENCIA_IN>', CV$SIN_VALOR);
	else
		--!reemplazando frecuencia IN
		select decode(pv_fre_IN,'R',1,'G',2,'B',3) into ln$lt_IN from dual;		
		lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<FRECUENCIA_IN>', 'COLOR_UBICACION = '||ln$fre_IN);
	end if;
	
	--!reemplazando ley tercio IN
	select decode(pv_lt_IN,'R',1,'G',2,'B',3) into ln$lt_IN from dual;
	lv$qry_where_stmt := REPLACE(lv$qry_where_stmt, '<LEY_TERCIO_IN>', 'COLOR_LEY_TERCIO = '||ln$lt_IN);

	--!filtrado los digitos en base a la decena
	lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<DIGIT>', pv_decena);

	--!reemplazando ciclo aparicion
	lv$qry_where_stmt := REPLACE(lv$qry_where_stmt,'<CICLO_APARICION>', pv_ca);

	--!reemplazando valores para numeros primos
	if instr(pv_conf_ppn,CV$NUMERO_PRIMO) > 0 then 
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 1');
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<PAR_NON>', CV$SIN_VALOR);		
	--!reemplazando valores para numeros pares y nones
	elsif instr(pv_conf_ppn,CV$NUMERO_PAR) > 0 then
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<PAR_NON>','INPAR_NUMBER_FLAG = 0');		
	elsif instr(pv_conf_ppn,CV$NUMERO_NON) > 0 then
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<PAR_NON>','INPAR_NUMBER_FLAG = 1');		
	elsif instr(pv_conf_ppn,CV$NUMERO_COMODIN) > 0 then
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<NUMERO_PRIMO>','PRIME_NUMBER_FLAG = 0');
		lv$qry_where_stmt := replace(lv$qry_where_stmt,'<PAR_NON>', CV$SIN_VALOR);		
	end if;
	
	--!uniendo las piezas para formar el query de gl
	olap_sys.w_common_pkg.g_dml_stmt := pv_qry_stmt||' '||lv$qry_where_stmt||' '||pv_qry_order_stmt;
	
	--!ejecutar el query y retornar la lista de digitis por digito
	run_gl_query_rules(pn_digit_pos    	  => pn_digit_pos
					 , pv_qry_stmt        => olap_sys.w_common_pkg.g_dml_stmt
					 , pv_save_qry_enable => 'Y'
					 , xtbl_qry_output    => xtbl_qry_output
					 , x_err_code         => x_err_code
					  );

	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		if pv_next_decena is not null then
			if pv_add_primo_enable = CV$ENABLE then
				--!agregar numeros primos en cada posicion en base a la decena
				set_numero_primo_decena (pv_decena			  => pv_decena
									   , pv_numero_primo_list => pv_numero_primo_list 
									   , xtbl_qry_output      => xtbl_qry_output
									   , x_err_code        	  => x_err_code
										);
			else
				x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
			end if;
			
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!si las decenas continuas son iguales y el digito de la 1a decena es 9 regresa false
				--!de lo contrario regresa true y el numero se agrega a la lista de numeros
				valida_ultimo_digito_decena (pv_decena       => pv_decena
										   , pv_next_decena  => pv_next_decena
										   , xtbl_qry_output => xtbl_qry_output
										   , x_err_code      => x_err_code								
											);
			end if;							
		end if;						
	

		
	
	end if;		
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_package_name   => GV$PACKAGE_NAME
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack()
                                   );
    raise;   
end get_lista_digitos_por_posicion;	

/*
--!proceso principal que aplicara validaciones adicionales a las jugadas finales
procedure valida_plan_jugadas_sums (x_err_code     IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'valida_plan_jugadas_sums';
	ln$comb_sum					   number :=0;	
	
	cursor c_sums is
	select drawing_id
		 , drawing_case
		 , record_id
		 , seq_no
		 , drawing_ready
	  from olap_sys.jugadas_listas
	  for update;
	  
	--!funcion para validar si la suma de la combinacion esta marcada como play_flag = Y
	function is_sum_play_flag (pn_drawing_case		NUMBER
							 , pn_xsum				NUMBER)  return boolean is
	begin
		olap_sys.w_common_pkg.g_rowcnt := 0;
		select count(1) cnt
		  into olap_sys.w_common_pkg.g_rowcnt
		  from olap_sys.plan_jugadas_sums
		 where play_flag 	= 'Y'
		   and drawing_case = pn_drawing_case
		   and xsum 		= pn_xsum;
		
		if olap_sys.w_common_pkg.g_rowcnt = 0 then
			return false;
		else
			return true;
		end if;	
	end is_sum_play_flag; 	
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;		
	
	for z in c_sums loop
		--!limpiando el arreglo
		gtbl$row_source.delete;
		
		--!convertir un string separado por comas en renglones de un query
		olap_sys.w_common_pkg.translate_string_to_rows (pv_string  => z.drawing_ready
													  , xtbl_row   => gtbl$row_source 
													  , x_err_code => x_err_code
													   );
		
		if gtbl$row_source.count > 0 then
			ln$comb_sum := 0;
			--!calculando la sumatoria de la combinacion
			for t in gtbl$row_source.first..gtbl$row_source.last loop
				ln$comb_sum := ln$comb_sum + to_number(gtbl$row_source(t));
			end loop;	
		end if;	


		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('data: '||z.drawing_id||'-'||z.drawing_case||'-'||z.record_id||'-'||z.seq_no||'-'||z.drawing_ready||' => '||ln$comb_sum);
		end if;	
			

	end loop;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
end valida_plan_jugadas_sums;
*/


--!proceso principal que aplicara validaciones adicionales a las jugadas finales
procedure extra_validations_handler (pv_drawing_type			  VARCHAR2
								   , pn_drawing_case			  NUMBER								   
								   , pn_drawing_id				  NUMBER
								   , pv_val_sum_enable  		  VARCHAR2
								   , pv_val_ca_enable  		      VARCHAR2
								   , x_err_code     IN OUT NOCOPY NUMBER								 
								    ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'extra_validations_handler';
	ln$ca_sum					   number :=0;	
	
	cursor c_jugadas is
	select sto.process_id
		 , sto.seq_no
		 , to_char(sto.pos1)||','||to_char(sto.pos2)||','||to_char(sto.pos3)||','||to_char(sto.pos4)||','||to_char(sto.pos5)||','||to_char(sto.pos6) drawing_ready
		 , sto.pos_sum
		 , nvl(sto.gl_ca_sum,0) gl_ca_sum
	  from olap_sys.s_template_hdr t
         , olap_sys.s_template_outputs sto
     where t.process_id   = sto.process_id
       and sto.status     = CV$STATUS_ACTIVO
	   and t.drawing_case = pn_drawing_case; 
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);		
		dbms_output.put_line('pv_val_sum_enable: '||pv_val_sum_enable);
		dbms_output.put_line('pv_val_sum_enable: '||pv_val_ca_enable);
   end if;		

	for z in c_jugadas loop
		--dbms_output.put_line(z.drawing_ready);

		--!limpiando el arreglo
		gtbl$row_source.delete;
		
		--!convertir un string separado por comas en renglones de un query
		olap_sys.w_common_pkg.translate_string_to_rows (pv_string  => z.drawing_ready
													  , xtbl_row   => gtbl$row_source 
													  , x_err_code => x_err_code
													   );
		
		if gtbl$row_source.count > 0 then
			ln$ca_sum   := 0;
			for t in gtbl$row_source.first..gtbl$row_source.last loop
	
				begin
					olap_sys.w_common_pkg.g_column_value := 0;
--					dbms_output.put_line(pv_drawing_type||'  '||pn_drawing_id||'  '||to_number(gtbl$row_source(t))||'  '||'B'||t);
					
					--!query dinamico para recuperar el CA
					olap_sys.w_common_pkg.g_dml_stmt := 'select ciclo_aparicion ca from olap_sys.s_calculo_stats';
					olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where drawing_type = '||chr(39)||pv_drawing_type||chr(39);
					olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and drawing_id = '||pn_drawing_id; 
					olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and digit = '||to_number(gtbl$row_source(t));     
					olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and b_type = '||chr(39)||'B'||t||chr(39); 
					
					execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_column_value; 
					
--					dbms_output.put_line(olap_sys.w_common_pkg.g_column_value);
					ln$ca_sum := ln$ca_sum + olap_sys.w_common_pkg.g_column_value;
				exception				
					when no_data_found then
						olap_sys.w_common_pkg.g_column_value := 0;
				end;																						   
			end loop;

		--!aplicando la validacion de COMB_SUM
		if pv_val_sum_enable = CV$ENABLE then
			begin
				select 1
				  into olap_sys.w_common_pkg.g_data_found
				  from olap_sys.plan_jugadas
				 where drawing_case = pn_drawing_case
				   and description 	= 'DECENAS'
				   and status 		= 'A'
				   and z.pos_sum between j_comb_sum_ini and j_comb_sum_end;
			exception
				when no_data_found then
					--!proceso para actualizar contadores, estados y mensajes en los templates
					upd_s_templates_error (pn_process_id		   => z.process_id
										 , pn_seq_no			   => z.seq_no
										 , pv_validation_message => 'VAL POSITION SUM'
										  ); 
			end;	
		end if;		
		   
		--!aplicando la validacion de CA
		if pv_val_ca_enable = CV$ENABLE then
--				dbms_output.put_line('ln$ca_sum: '||ln$ca_sum);
			olap_sys.w_common_pkg.g_data_found := 0;
			begin
				select 1
				  into olap_sys.w_common_pkg.g_data_found
				  from olap_sys.plan_jugadas
				 where drawing_case = pn_drawing_case
				   and description 	= 'DECENAS'
				   and status 		= 'A'
				   and ln$ca_sum  between R_CA_INI and R_CA_END;
			exception
				when no_data_found then
					--!proceso para actualizar contadores, estados y mensajes en los templates
					upd_s_templates_error (pn_process_id		   => z.process_id
										 , pn_seq_no			   => z.seq_no
										 , pv_validation_message => 'VAL GL CA SUM'
										  ); 
			end;	
		end if;			
		end if;	


		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('data: '||z.process_id||'-'||z.seq_no||'-'||z.pos_sum||'-'||z.drawing_ready||' => '||ln$ca_sum);
		end if;	
	end loop;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end extra_validations_handler;


--!imprimir resumen de jugadas presentadas o finales
procedure imprime_resumen_jugadas (pn_drawing_case	NUMBER
								 , pn_drawing_id	NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'imprime_resumen_jugadas'; 
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
   end if;	


	dbms_output.put_line(gn$jugadas_presentadas_cnt||' Total de jugadas generadas');		

	--!contando total de jugadas activas
	select count(1) cnt
	  into gn$jugadas_finales_cnt
	  from olap_sys.s_template_hdr t
		 , olap_sys.s_template_outputs sto
	 where t.process_id = sto.process_id
	   and sto.status = 'A'
	   and t.drawing_case = pn_drawing_case
	   and t.drawing_id   = pn_drawing_id;

	dbms_output.put_line(gn$jugadas_finales_cnt||' Total de jugadas finales generadas');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());      
end imprime_resumen_jugadas;


--!proceso para insertar la configuracion a ser utilizada para escoger los numeros a jugar
procedure ins_s_template_hdr(pn_process_id		number
						   , pn_drawing_case	number
						   , pn_drawing_id		number
						   , pn_diferencia_tipo number
						   , pn_term_cnt		number
						   , pn_lt_red_cnt		number
						   , pn_seq_no_pct		number
						   , pn_seq_no			number
						   , pv_fr1				varchar2
						   , pv_fr2				varchar2
						   , pv_fr3				varchar2
						   , pv_fr4				varchar2
						   , pv_fr5				varchar2
						   , pv_fr6				varchar2
						   , pv_lt1				varchar2
						   , pv_lt2				varchar2
						   , pv_lt3				varchar2
						   , pv_lt4				varchar2
						   , pv_lt5				varchar2
						   , pv_lt6				varchar2
						   , pv_pr1				varchar2
						   , pv_pr2				varchar2
						   , pv_pr3				varchar2
						   , pv_pr4				varchar2
						   , pv_pr5				varchar2
						   , pv_pr6				varchar2
						   , pn_pr_sort	        number
							) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_s_template_hdr';

begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pn_diferencia_tipo: '||pn_diferencia_tipo);
		dbms_output.put_line('pn_term_cnt: '||pn_term_cnt);
		dbms_output.put_line('pn_lt_red_cnt: '||pn_lt_red_cnt);
		dbms_output.put_line('pn_seq_no_pct: '||pn_seq_no_pct);
		dbms_output.put_line('pn_seq_no: '||pn_seq_no);
		dbms_output.put_line('pv_fr1: '||pv_fr1);
		dbms_output.put_line('pv_fr2: '||pv_fr2);
		dbms_output.put_line('pv_fr3: '||pv_fr3);
		dbms_output.put_line('pv_fr4: '||pv_fr4);
		dbms_output.put_line('pv_fr5: '||pv_fr5);
		dbms_output.put_line('pv_fr6: '||pv_fr6);
		dbms_output.put_line('pv_lt1: '||pv_lt1);
		dbms_output.put_line('pv_lt2: '||pv_lt2);
		dbms_output.put_line('pv_lt3: '||pv_lt3);
		dbms_output.put_line('pv_lt4: '||pv_lt4);
		dbms_output.put_line('pv_lt5: '||pv_lt5);
		dbms_output.put_line('pv_lt6: '||pv_lt6);
		dbms_output.put_line('pv_pr1: '||pv_pr1);
		dbms_output.put_line('pv_pr2: '||pv_pr2);
		dbms_output.put_line('pv_pr3: '||pv_pr3);
		dbms_output.put_line('pv_pr4: '||pv_pr4);
		dbms_output.put_line('pv_pr5: '||pv_pr5);
		dbms_output.put_line('pv_pr6: '||pv_pr6);
		dbms_output.put_line('pn_pr_sort: '||pn_pr_sort);
	end if;
	
	insert into olap_sys.s_template_hdr(process_id
									, drawing_case
									, drawing_id
									, dif_tipo
									, term_cnt
									, lt_red_cnt
									, seq_no_pct
									, seq_no
									, fr1
									, fr2
									, fr3
									, fr4
									, fr5
									, fr6
									, lt1
									, lt2
									, lt3
									, lt4
									, lt5
									, lt6
									, pr1
									, pr2
									, pr3
									, pr4
									, pr5
									, pr6
									, pr_sort
									, active_cnt
									, error_cnt
									, created_by
									, creation_date)
	values (pn_process_id
		, pn_drawing_case
		, pn_drawing_id
		, pn_diferencia_tipo
		, pn_term_cnt
		, pn_lt_red_cnt
		, pn_seq_no_pct
		, pn_seq_no
		, pv_fr1
		, pv_fr2
		, pv_fr3
		, pv_fr4
		, pv_fr5
		, pv_fr6
		, pv_lt1
		, pv_lt2
		, pv_lt3
		, pv_lt4
		, pv_lt5
		, pv_lt6
		, pv_pr1
		, pv_pr2
		, pv_pr3
		, pv_pr4
		, pv_pr5
		, pv_pr6
		, pn_pr_sort
		, 0
		, 0
		, user
		, sysdate
		);
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());      
end ins_s_template_hdr;

--!recupera el ID global de las jugadas listas
function get_jugadas_listas_seq return number is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_jugadas_listas_seq';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
   end if;
	
	olap_sys.w_common_pkg.g_column_value :=  0;
	
	select olap_sys.jugadas_listas_seq.nextval
	  into olap_sys.w_common_pkg.g_column_value
	  from dual;
	  
	return olap_sys.w_common_pkg.g_column_value;  
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
	return 0;
end get_jugadas_listas_seq;


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
								 , pv_val_sum_enable  		    VARCHAR2 DEFAULT 'Y'
								 --!validacion gl ca
								 , pv_val_ca_enable  		    VARCHAR2 DEFAULT 'Y'
								 --!indicador de terminaciones repetidas en la jugadas. Default 2
								 , pn_term_cnt					NUMBER DEFAULT 2
								 , x_err_code     IN OUT NOCOPY NUMBER								 
								  ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'main_posicion_IN_handler';
  lv$digit_list_pos1				varchar2(1000);
  lv$digit_list_pos2				varchar2(1000);
  lv$digit_list_pos3				varchar2(1000);
  lv$digit_list_pos4				varchar2(1000);
  lv$digit_list_pos5				varchar2(1000);
  lv$digit_list_pos6				varchar2(1000);
  ltbl$list_array_pos1              gt$gl_tbl;
  ltbl$list_array_pos2              gt$gl_tbl;
  ltbl$list_array_pos3              gt$gl_tbl;
  ltbl$list_array_pos4              gt$gl_tbl;
  ltbl$list_array_pos5              gt$gl_tbl;
  ltbl$list_array_pos6              gt$gl_tbl;  
  lv$exception_msg					varchar2(1000);
  ln$next_drawing_id				number := 0;
  
  --|-----------------------------------|--
  --|     VARIABLES DE GIGALOTERIAS     |--
  --|-----------------------------------|--
  lv$fre_IN1				varchar2(1000);
  lv$fre_IN2				varchar2(1000);
  lv$fre_IN3				varchar2(1000);
  lv$fre_IN4				varchar2(1000);
  lv$fre_IN5				varchar2(1000);
  lv$fre_IN6				varchar2(1000); 
  lv$chng_posicion_pos1		varchar2(1000);
  lv$chng_posicion_pos2		varchar2(1000);
  lv$chng_posicion_pos3		varchar2(1000);  
  lv$chng_posicion_pos4		varchar2(1000);
  lv$chng_posicion_pos5		varchar2(1000);
  lv$chng_posicion_pos6		varchar2(1000);  
								  
  ltbl$lt_IN                gt$plan_tbl;	
  
  --|-------------------------------|--
  --|     VARIABLES DE PANORAMA     |--
  --|-------------------------------|--
  lv$select_list                    varchar2(1000); 
  lv$final_select_list 				varchar2(1000); 
  lv$numero_primo_list				varchar2(1000);
  lv$where_clause                  	varchar2(4000);
  lv$qry_where_stmt				   	varchar2(4000);
  lv$qry_stmt                      	varchar2(4000); 
  ln$process_id             		number := 0;
  ltbl$numero_primo                 gt$np_tbl; 
 

  --!cursor para recuperar la configuracion de la lt basado en plan_jugadas
  cursor c_lt_pattern_master (pn_drawing_id			number) is
	select distinct jd.pos1
	     , jd.pos2
	     , jd.pos3
	     , jd.pos4
	     , jd.pos5
	     , jd.pos6 
	     , (select pj2.pos1 from olap_sys.plan_jugadas pj2 where pj2.id = jd.lt_pattern_id) percentage_ini 
	     , (select pj2.pos2 from olap_sys.plan_jugadas pj2 where pj2.id = jd.lt_pattern_id) percentage_end
	     , (select pj2.pos3 from olap_sys.plan_jugadas pj2 where pj2.id = jd.lt_pattern_id) seq_ini
	     , (select pj2.pos4 from olap_sys.plan_jugadas pj2 where pj2.id = jd.lt_pattern_id) seq_end
		 , nvl(jd.pos1,'#') pos1_str
	     , nvl(jd.pos2,'#') pos2_str
	     , nvl(jd.pos3,'#') pos3_str
	     , nvl(jd.pos4,'#') pos4_str
	     , nvl(jd.pos5,'#') pos5_str
	     , nvl(jd.pos6,'#') pos6_str
	  from olap_sys.plan_jugada_details jd
		 , olap_sys.plan_jugadas pj 
	 where pj.drawing_type = jd.drawing_type 
	   and pj.id = jd.plan_jugada_id 
	   and jd.status = CV$STATUS_ACTIVO
	   and pj.status = CV$STATUS_ACTIVO
	   and jd.description = 'LEY_TERCIO_IN' 
	   and pj.drawing_case= pn_drawing_case
	 order by jd.pos1 nulls last
	     , jd.pos6 nulls last; 
 
  --!cursor para recuperar la configuracion de la lt basado en s_gl_ley_tercio_patterns
  cursor c_lt_pattern_detail (pn_percentage_ini		  number
						    , pn_percentage_end		  number
						    , pn_percentage_seq_ini   number
						    , pn_percentage_seq_end   number
						    , pv_pos1				  varchar2
						    , pv_pos2				  varchar2
						    , pv_pos3				  varchar2
						    , pv_pos4				  varchar2
						    , pv_pos5				  varchar2
						    , pv_pos6				  varchar2
						    , pn_lt_red_cnt			  number
						    , pn_drawing_id			  number
						    , pn_drawing_case		  number
						     ) is
	select seq_no_percentage seq_no_pct
	     , seq_no
		 , lt1
		 , lt2
		 , lt3
		 , lt4
		 , lt5
		 , lt6
		 , lt.red_cnt
	  from olap_sys.s_gl_ley_tercio_patterns lt
	where lt.seq_no_percentage between pn_percentage_ini and pn_percentage_end
	  and lt.seq_no >= decode(pn_percentage_seq_ini,0,lt.seq_no,pn_percentage_seq_ini)
	  and lt.seq_no <= decode(pn_percentage_seq_end,0,lt.seq_no,pn_percentage_seq_end)
	  and lt.match_cnt = 0
	  and lt.lt1 != '#'
	  and lt.lt2 != '#'
	  and lt.lt3 != '#'
	  and lt.lt4 != '#'
	  and lt.lt5 != '#'
	  and lt.lt6 != '#'
	  and lt.red_cnt <= pn_lt_red_cnt
	  and lt.last_drawing_id = pn_drawing_id           
	  and (lt.lt1, lt.lt2, lt.lt3, lt.lt4, lt.lt5, lt.lt6) in ((nvl(pv_pos1,lt.lt1)
															  , nvl(pv_pos2,lt.lt2)
															  , nvl(pv_pos3,lt.lt3)
															  , nvl(pv_pos4,lt.lt4)
															  , nvl(pv_pos5,lt.lt5)
															  , nvl(pv_pos6,lt.lt6)))
	order by seq_no_pct, seq_no;     
  
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pv_execution_type: '||pv_execution_type);
   end if;

	--!haciendo ilimitado el buffer para impimir mensajes
	dbms_output.enable(NULL);
	
	--!limpiando tablas temporales
	delete olap_sys.tmp_testing;
	delete olap_sys.s_template_outputs;
	delete olap_sys.s_template_hdr;
	commit;
	
	--!inicializando contador de variables globales
	gn$jugadas_finales_cnt := 0;
	gn$jugadas_presentadas_cnt := 0;
	
	--recuperar el ID del ultimo sorteo
	gn$drawing_id := get_max_drawing_id (pv_drawing_type => pv_drawing_type);

	--!ID del siguiente sorteo
	ln$next_drawing_id := gn$drawing_id + 1;
																	
	--!proceso para obtener las decenas del plan de juego
	get_plan_jugada_decenas(pv_drawing_type 	=> pv_drawing_type
						  , pn_drawing_case 	=> pn_drawing_case
						  , pv_show_init_values => pv_show_init_values
						  --!decenas
						  , xv_d1           	=> gv$d1
						  , xv_d2           	=> gv$d2
						  , xv_d3           	=> gv$d3
						  , xv_d4           	=> gv$d4
						  , xv_d5           	=> gv$d5
						  , xv_d6           	=> gv$d6
						  , xv_decena_rank      => gv$decena_rank
						  , x_err_code      	=> x_err_code
					  	   );	

	--|----------------------------------|--
	--|     PROCESOS DE GIGALOTERIAS     |--
	--|----------------------------------|--
/*
	--!proceso para obtener el string para formar el IN de la digito_posicion_orden
	get_digito_posicion_orden_IN(pv_drawing_type 	 => pv_drawing_type
							   , pn_drawing_case 	 => pn_drawing_case
							   , pv_show_init_values => pv_show_init_values
							   --!digito_posicion_orden IN
							   , xv_dp_IN1        	 => lv$dp_IN1
							   , xv_dp_IN2        	 => lv$dp_IN2
							   , xv_dp_IN3        	 => lv$dp_IN3
							   , xv_dp_IN4        	 => lv$dp_IN4
							   , xv_dp_IN5        	 => lv$dp_IN5
							   , xv_dp_IN6        	 => lv$dp_IN6
							   , x_err_code       	 => x_err_code
								);*/
							 
	--!proceso para seleccionar los digitos que no hayan tenido cambios en la posicion
	get_plan_jugada_change(pv_drawing_type       => pv_drawing_type
						 , pn_drawing_case       => pn_drawing_case
						 , xv_chng_posicion_pos1 => lv$chng_posicion_pos1
						 , xv_chng_posicion_pos2 => lv$chng_posicion_pos2
						 , xv_chng_posicion_pos3 => lv$chng_posicion_pos3
						 , xv_chng_posicion_pos4 => lv$chng_posicion_pos4
						 , xv_chng_posicion_pos5 => lv$chng_posicion_pos5
						 , xv_chng_posicion_pos6 => lv$chng_posicion_pos6
						 , x_err_code         	 => x_err_code
						  );
								  
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		--!proceso para obtener el string para formar el IN de las frecuencias
		get_frecuencia_IN(pv_drawing_type 	=> pv_drawing_type
					    , pn_drawing_case   	=> pn_drawing_case
					    , pv_show_init_values	=> pv_show_init_values
					    --!frecuencia IN
					    , xv_fre_IN1        	=> lv$fre_IN1
					    , xv_fre_IN2        	=> lv$fre_IN2
					    , xv_fre_IN3        	=> lv$fre_IN3
					    , xv_fre_IN4        	=> lv$fre_IN4
					    , xv_fre_IN5        	=> lv$fre_IN5
					    , xv_fre_IN6        	=> lv$fre_IN6
					    , x_err_code    	  	=> x_err_code
						 ); 

		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!proceso para obtener rango del ciclo de aparicion del plan de jugadas
			get_plan_jugada_ca(pv_drawing_type 	   => pv_drawing_type
							 , pn_drawing_case 	   => pn_drawing_case
							 , pv_show_init_values => pv_show_init_values
							 --!ciclo aparicion
							 , xv_ca1          	   => gv$ca1
							 , xv_ca2              => gv$ca2
							 , xv_ca3              => gv$ca3
							 , xv_ca4              => gv$ca4
							 , xv_ca5              => gv$ca5
							 , xv_ca6              => gv$ca6
							 , x_err_code          => x_err_code
							  );

--				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then							
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				
				for ppn in c_conf_ppn (pv_drawing_type => pv_drawing_type
									 , pn_drawing_case => pn_drawing_case) loop
						
					dbms_output.put_line('<<<   ----------------------------------------------------------------------------------------   >>>');	
					if gv$d1 != CV$SIN_VALOR then
						dbms_output.put_line('<<<   POS1: ('||replace(replace(gv$d1,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ POS2: ('||replace(replace(gv$d2,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ POS3: ('||replace(replace(gv$d3,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ POS4: ('||replace(replace(gv$d4,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ POS5: ('||replace(replace(gv$d5,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ POS6: ('||replace(replace(gv$d6,'DIGIT BETWEEN ',NULL),' AND ','-')||') $ rank: ('||gv$decena_rank||')   >>>');													
					end if;
					dbms_output.put_line('<<<   DECENAS POS1: '||lpad(ppn.pos1,2,' ')||'   POS2: '||lpad(ppn.pos2,2,' ')||'   POS3: '||lpad(ppn.pos3,2,' ')||'   POS4: '||lpad(ppn.pos4,2,' ')||'   POS5: '||lpad(ppn.pos5,2,' ')||'   POS6: '||lpad(ppn.pos6,2,' ')||' SEQNO: '||ppn.seq_no||' SORT_EXECUTION: '||ppn.sort_execution||'   >>>');							
					

					--!inicializando arreglos y variables  de listas de numeros
					ltbl$list_array_pos1.delete;
					ltbl$list_array_pos2.delete;
					ltbl$list_array_pos3.delete;
					ltbl$list_array_pos4.delete;
					ltbl$list_array_pos5.delete;
					ltbl$list_array_pos6.delete;
					lv$digit_list_pos1 := null;
					lv$digit_list_pos2 := null;
					lv$digit_list_pos3 := null;
					lv$digit_list_pos4 := null;
					lv$digit_list_pos5 := null;
					lv$digit_list_pos6 := null;
					gv$qry_stmt		   := null;
					gv$qry_where_stmt  := null;
					gv$qry_order_stmt  := null;				


					--!cursor para recuperar la configuracion de la lt basado en plan_jugadas
					for ltMaster in c_lt_pattern_master (pn_drawing_id	       => gn$drawing_id) loop

						dbms_output.put_line('<<<   JUGADA  POS1: '||lpad(ltMaster.pos1_str,2,' ')||' $ POS2: '||
														lpad(ltMaster.pos2_str,2,' ')||' $ POS3: '||
														lpad(ltMaster.pos3_str,2,' ')||' $ POS4: '||
														lpad(ltMaster.pos4_str,2,' ')||' $ POS5: '||
														lpad(ltMaster.pos5_str,2,' ')||' $ POS6: '||
														lpad(ltMaster.pos6_str,2,' ')||'   >>>');					
					--!cursor para recuperar la configuracion de la lt basado en s_gl_ley_tercio_patterns
					for ltIN in c_lt_pattern_detail (pn_percentage_ini     => ltMaster.percentage_ini 
												   , pn_percentage_end     => ltMaster.percentage_end
												   , pn_percentage_seq_ini => ltMaster.seq_ini
												   , pn_percentage_seq_end => ltMaster.seq_end
												   , pv_pos1			   => ltMaster.pos1
												   , pv_pos2			   => ltMaster.pos2
												   , pv_pos3			   => ltMaster.pos3
												   , pv_pos4			   => ltMaster.pos4
												   , pv_pos5			   => ltMaster.pos5
												   , pv_pos6			   => ltMaster.pos6
												   , pn_lt_red_cnt	       => pn_lt_red_cnt
												   , pn_drawing_id	       => gn$drawing_id
												   , pn_drawing_case	   => pn_drawing_case) loop

						--!recupera el ID global de las jugadas listas
						ln$process_id := get_jugadas_listas_seq;
	
						--!proceso para insertar la configuracion a ser utilizada para escoger los numeros a jugar
						ins_s_template_hdr(pn_process_id   => ln$process_id
									     , pn_drawing_case => pn_drawing_case
									     , pn_drawing_id   => ln$next_drawing_id
										 , pn_diferencia_tipo => pn_diferencia_tipo
										 , pn_term_cnt     => pn_term_cnt
										 , pn_lt_red_cnt   => ltIN.red_cnt
									     , pn_seq_no_pct   => ltIN.seq_no_pct
									     , pn_seq_no	   => ltIN.seq_no
									     , pv_fr1		   => lv$fre_IN1
									     , pv_fr2		   => lv$fre_IN2
									     , pv_fr3		   => lv$fre_IN3
									     , pv_fr4		   => lv$fre_IN4
									     , pv_fr5		   => lv$fre_IN5
									     , pv_fr6		   => lv$fre_IN6
									     , pv_lt1		   => ltIN.lt1
									     , pv_lt2		   => ltIN.lt2
									     , pv_lt3		   => ltIN.lt3
									     , pv_lt4		   => ltIN.lt4
									     , pv_lt5		   => ltIN.lt5
									     , pv_lt6		   => ltIN.lt6
									     , pv_pr1		   => ppn.pos1
									     , pv_pr2		   => ppn.pos2
									     , pv_pr3		   => ppn.pos3
									     , pv_pr4		   => ppn.pos4
									     , pv_pr5		   => ppn.pos5
									     , pv_pr6		   => ppn.pos6
										 , pn_pr_sort      => ppn.sort_execution
										  );
						
						--formar query base para GL
						build_query (pv_drawing_type   => pv_drawing_type
								   , pn_select_id      => 7
								   , pn_where_id       => 11			   
								   , xv_qry_stmt   	   => gv$qry_stmt
								   , xv_qry_where_stmt => gv$qry_where_stmt
								   , xv_qry_order_stmt => gv$qry_order_stmt
								   , x_err_code        => x_err_code
									);
						
						dbms_output.put_line('<<<   LT      POS1: '||lpad(ltIN.lt1,2,' ')||' $ POS2: '||
									lpad(ltIN.lt2,2,' ')||' $ POS3: '||
									lpad(ltIN.lt3,2,' ')||' $ POS4: '||
									lpad(ltIN.lt4,2,' ')||' $ POS5: '||
									lpad(ltIN.lt5,2,' ')||' $ POS6: '||
									lpad(ltIN.lt6,2,' ')||' $ SEQ: '||ltIN.seq_no||'   >>>');
/*						dbms_output.put_line('<<<   LT      POS1: '||replace(replace(replace(ltIN.lt1,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' $ POS2:'||
														replace(replace(replace(ltIN.lt2,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' $ POS3:'||
														replace(replace(replace(ltIN.lt3,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' $ POS4:'||
														replace(replace(replace(ltIN.lt4,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' $ POS5:'||
														replace(replace(replace(ltIN.lt5,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' $ POS6:'||
														replace(replace(replace(ltIN.lt6,'(1)','RED'),'(2)','GREEN'),'(3)','BLUE')||' RED_CNT: '||ltIN.red_cnt||'   >>>');														*/
						--!posicion 1
						get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
													  , pn_drawing_id        => gn$drawing_id
													  , pn_digit_pos         => 1														  
													  , pv_fre_IN	         => lv$fre_IN1
													  , pv_lt_IN		     => ltIN.lt1
													  , pv_ca			     => gv$ca1
													  , pv_decena            => gv$d1
													  , pv_next_decena       => gv$d2
													  , pv_conf_ppn          => ppn.pos1 
													  , pv_qry_stmt   	     => gv$qry_stmt
													  , pv_qry_where_stmt    => gv$qry_where_stmt
													  , pv_qry_order_stmt    => gv$qry_order_stmt
													  , pv_numero_primo_list => lv$numero_primo_list
													  , pv_add_primo_enable  => pv_add_primo_enable
													  , pv_chng_posicion_pos => lv$chng_posicion_pos1
													  , xtbl_qry_output   	 => ltbl$list_array_pos1
													  , x_err_code    	     => x_err_code
													   );

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							--!posicion 2
							get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
														  , pn_drawing_id        => gn$drawing_id
														  , pn_digit_pos         => 2														  
														  , pv_fre_IN	         => lv$fre_IN2
														  , pv_lt_IN		     => ltIN.lt2
														  , pv_ca			     => gv$ca2
														  , pv_decena            => gv$d2
														  , pv_next_decena       => gv$d3
														  , pv_conf_ppn          => ppn.pos2
														  , pv_qry_stmt   	     => gv$qry_stmt
														  , pv_qry_where_stmt    => gv$qry_where_stmt
														  , pv_qry_order_stmt    => gv$qry_order_stmt
														  , pv_numero_primo_list => lv$numero_primo_list
														  , pv_add_primo_enable  => pv_add_primo_enable
														  , pv_chng_posicion_pos => lv$chng_posicion_pos2
														  , xtbl_qry_output   	 => ltbl$list_array_pos2
														  , x_err_code    	  	 => x_err_code
														   );

							if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
								--!posicion 3
								get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
															  , pn_drawing_id        => gn$drawing_id
															  , pn_digit_pos         => 3														  
															  , pv_fre_IN	         => lv$fre_IN3
															  , pv_lt_IN		     => ltIN.lt3
															  , pv_ca			     => gv$ca3
															  , pv_decena            => gv$d3
															  , pv_next_decena       => gv$d4
															  , pv_conf_ppn          => ppn.pos3
															  , pv_qry_stmt   	     => gv$qry_stmt
															  , pv_qry_where_stmt    => gv$qry_where_stmt
															  , pv_qry_order_stmt    => gv$qry_order_stmt
															  , pv_numero_primo_list => lv$numero_primo_list
															  , pv_add_primo_enable  => pv_add_primo_enable
															  , pv_chng_posicion_pos => lv$chng_posicion_pos3
															  , xtbl_qry_output   	 => ltbl$list_array_pos3
															  , x_err_code    	  	 => x_err_code
															   );

								if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
									--!posicion 4
									get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
																  , pn_drawing_id        => gn$drawing_id
																  , pn_digit_pos         => 4														  
																  , pv_fre_IN	         => lv$fre_IN4
																  , pv_lt_IN		     => ltIN.lt4
																  , pv_ca			     => gv$ca4
																  , pv_decena            => gv$d4
																  , pv_next_decena       => gv$d5
																  , pv_conf_ppn          => ppn.pos4
																  , pv_qry_stmt   	     => gv$qry_stmt
																  , pv_qry_where_stmt    => gv$qry_where_stmt
																  , pv_qry_order_stmt    => gv$qry_order_stmt
																  , pv_numero_primo_list => lv$numero_primo_list
																  , pv_add_primo_enable  => pv_add_primo_enable
																  , pv_chng_posicion_pos => lv$chng_posicion_pos4
																  , xtbl_qry_output   	 => ltbl$list_array_pos4
																  , x_err_code    	  	 => x_err_code
																   );

									if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
										--!posicion 5
										get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
																	  , pn_drawing_id        => gn$drawing_id
																	  , pn_digit_pos         => 5														  
																	  , pv_fre_IN	         => lv$fre_IN5
																	  , pv_lt_IN		     => ltIN.lt5
																	  , pv_ca			     => gv$ca5
																	  , pv_decena            => gv$d5
																	  , pv_next_decena       => gv$d6
																	  , pv_conf_ppn          => ppn.pos5
																	  , pv_qry_stmt   	     => gv$qry_stmt
																	  , pv_qry_where_stmt    => gv$qry_where_stmt
																	  , pv_qry_order_stmt    => gv$qry_order_stmt
																	  , pv_numero_primo_list => lv$numero_primo_list
																	  , pv_add_primo_enable  => pv_add_primo_enable
																	  , pv_chng_posicion_pos => lv$chng_posicion_pos5
																	  , xtbl_qry_output   	 => ltbl$list_array_pos5
																	  , x_err_code    	  	 => x_err_code
																	   );

										if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
											--!posicion 6
											get_lista_digitos_por_posicion (pv_drawing_type      => pv_drawing_type
																		  , pn_drawing_id        => gn$drawing_id
																		  , pn_digit_pos         => 6														  
																		  , pv_fre_IN	         => lv$fre_IN6
																		  , pv_lt_IN		     => ltIN.lt6
																		  , pv_ca			     => gv$ca6
																		  , pv_decena            => gv$d6
																		  , pv_conf_ppn          => ppn.pos6
																		  , pv_qry_stmt   	     => gv$qry_stmt
																		  , pv_qry_where_stmt    => gv$qry_where_stmt
																		  , pv_qry_order_stmt    => gv$qry_order_stmt
																		  , pv_numero_primo_list => lv$numero_primo_list
																		  , pv_add_primo_enable  => pv_add_primo_enable
																		  , pv_chng_posicion_pos => lv$chng_posicion_pos6
																		  , xtbl_qry_output   	 => ltbl$list_array_pos6
																		  , x_err_code    	  	 => x_err_code
																		   );
																		   
											if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
												--!convertir array de digitos en string separado por comas para cada posicion
												array_digitos_to_coma_string (ptbl$list_array_pos1 => ltbl$list_array_pos1
																			, ptbl$list_array_pos2 => ltbl$list_array_pos2
																			, ptbl$list_array_pos3 => ltbl$list_array_pos3
																			, ptbl$list_array_pos4 => ltbl$list_array_pos4
																			, ptbl$list_array_pos5 => ltbl$list_array_pos5
																			, ptbl$list_array_pos6 => ltbl$list_array_pos6
																			--!listas finales de numeros								
																			, xv_digit_list_pos1   => lv$digit_list_pos1
																			, xv_digit_list_pos2   => lv$digit_list_pos2
																			, xv_digit_list_pos3   => lv$digit_list_pos3
																			, xv_digit_list_pos4   => lv$digit_list_pos4
																			, xv_digit_list_pos5   => lv$digit_list_pos5
																			, xv_digit_list_pos6   => lv$digit_list_pos6						  
																			, x_err_code           => x_err_code
																			 );

												if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
													--!imprimir las jugadas intermedias de gigaloterias
													imprimir_jugadas_gl (pn_drawing_id         => gn$drawing_id
																	   , pn_drawing_case	   => pn_drawing_case
																	   , pv_show_init_values   => pv_show_init_values
																	   , pltbl_list_array_pos1 => ltbl$list_array_pos1
																	   , pltbl_list_array_pos2 => ltbl$list_array_pos2
																	   , pltbl_list_array_pos3 => ltbl$list_array_pos3
																	   , pltbl_list_array_pos4 => ltbl$list_array_pos4
																	   , pltbl_list_array_pos5 => ltbl$list_array_pos5
																	   , pltbl_list_array_pos6 => ltbl$list_array_pos6
																		); 																				

													--|-------------------------------|--
													--|     PROCESOS DEL PANORAMA     |--
													--|-------------------------------|--
													if nvl(length(lv$digit_list_pos1),0) = 0 or 
													   nvl(length(lv$digit_list_pos2),0) = 0 or 
													   nvl(length(lv$digit_list_pos3),0) = 0 or
													   nvl(length(lv$digit_list_pos4),0) = 0 or
													   nvl(length(lv$digit_list_pos5),0) = 0 or
													   nvl(length(lv$digit_list_pos6),0) = 0 then
														
														lv$exception_msg := null;
														
														if nvl(length(lv$digit_list_pos1),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 1 vacia.';
														end if;

														if nvl(length(lv$digit_list_pos2),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 2 vacia.';
														end if;

														if nvl(length(lv$digit_list_pos3),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 3 vacia.';
														end if;
														
														if nvl(length(lv$digit_list_pos4),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 4 vacia.';
														end if;		
														
														if nvl(length(lv$digit_list_pos5),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 5 vacia.';
														end if;

														if nvl(length(lv$digit_list_pos6),0) = 0 then
															lv$exception_msg := lv$exception_msg||CHR(10)||'Lista posicion 6 vacia.';
														end if;
														dbms_output.put_line(lv$exception_msg);
					--									raise ge$numeros_listas_imcompletas;
													else
														if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
															--!proceso principal para obtener los digitos del panorama

															if pv_execution_type = CV$PANORAMA then																
																--formar query base para PANORAMA
																build_query (pv_drawing_type   => pv_drawing_type
																		   , pn_select_id      => 9
																		   , pn_where_id       => 13
																		   , xv_qry_stmt   	   => gv$qry_stmt
																		   , xv_qry_where_stmt => gv$qry_where_stmt
																		   , xv_qry_order_stmt => gv$qry_order_stmt 
																		   , x_err_code        => x_err_code
																			);  
																													
																if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then			
																
																	--!formando el query final
																	olap_sys.w_common_pkg.g_dml_stmt  := gv$qry_stmt||' '||gv$qry_where_stmt; 
																								
																	--!ejecutar el panorama query para obtener los numeros a jugar
																	run_panorama_query_rules(pv_drawing_type            => pv_drawing_type
																						   , pn_drawing_id   			=> gn$drawing_id 
																						   , pn_drawing_case 			=> pn_drawing_case 
																						   , pn_term_cnt                => pn_term_cnt
																						   , pv_qry_stmt     	   		=> olap_sys.w_common_pkg.g_dml_stmt 
																						   , pv_save_qry_enable    		=> 'Y'																						  
																						   , pn_diferencia_tipo			=> pn_diferencia_tipo
																						   , pv_digit_list_pos1   		=> lv$digit_list_pos1
																						   , pv_digit_list_pos2   		=> lv$digit_list_pos2
																						   , pv_digit_list_pos3   		=> lv$digit_list_pos3
																						   , pv_digit_list_pos4   		=> lv$digit_list_pos4
																						   , pv_digit_list_pos5   		=> lv$digit_list_pos5
																						   , pv_digit_list_pos6   		=> lv$digit_list_pos6
																						   , pn_process_id				=> ln$process_id
																						   , x_err_code      	   		=> x_err_code
																							); 			

																	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then																										
																		--!proceso principal que aplicara validaciones adicionales a las jugadas finales
																		extra_validations_handler (pv_drawing_type    => pv_drawing_type
																								 , pn_drawing_case	  => pn_drawing_case
																								 , pn_drawing_id   	  => gn$drawing_id 																			
																								 --!validacion comb_sum
																								 , pv_val_sum_enable  => pv_val_sum_enable
																								 --!validacion gl ca
																								 , pv_val_ca_enable   => pv_val_ca_enable  
																								 , x_err_code         => x_err_code							 
																								  );
																							  
																	end if;			
																end if;																																																					
															end if;
														end if;					
													end if;
												end if;
											end if;
										end if;
									end if;
								end if;
							end if;
						end if;
					end loop;
					end loop;	
				end loop;				
			end if;		
		end if;					
	end if;	
	commit;
	dbms_output.put_line(chr(10)||'$$$EXITO$$$$DINERO$$$$GANO$PREMIO$1ER$LUGAR$MELATE$RETRO$$$COBRO$DINERO$$$INVERTO$DINERO$$$');							
	dbms_output.put_line('Siguiente sorteo: '||ln$next_drawing_id);
	dbms_output.put_line('Drawing Case: '||pn_drawing_case);
	dbms_output.put_line('Diferencia Tipo: '||pn_diferencia_tipo);
	dbms_output.put_line('Execution Type: '||pv_execution_type);
	dbms_output.put_line(' ');
	
	if pv_execution_type = CV$PANORAMA then
		--!imprimir resumen de jugadas presentadas o finales
		imprime_resumen_jugadas (pn_drawing_case => pn_drawing_case
							   , pn_drawing_id	 => ln$next_drawing_id);
	end if;	
exception
  when ge$numeros_listas_imcompletas then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||lv$exception_msg);    
    --raise;     
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end main_posicion_IN_handler;


--|----------------------------------------------------------------------------------------------------------|--
--|                                                                                                          |--
--|	logica con el aproach de actualizar el pla de jugadas con la posicion del digito del ultimo sorteo       |--
--|                                                                                                          |--
--|----------------------------------------------------------------------------------------------------------|--

--!proceso principal para actualziar la posicion del digito del ultimo sorteo
procedure set_plan_jugada_ultimo_sorteo(pv_drawing_type             VARCHAR2
						              , pn_drawing_case             NUMBER
									  , pv_posicion1   				VARCHAR2 DEFAULT NULL
								      , pv_posicion2   				VARCHAR2 DEFAULT NULL
								      , pv_posicion3   				VARCHAR2 DEFAULT NULL
								      , pv_posicion4   				VARCHAR2 DEFAULT NULL
								      , pv_posicion5   				VARCHAR2 DEFAULT NULL
								      , pv_posicion6   				VARCHAR2 DEFAULT NULL								   
								      , x_err_code     IN OUT NOCOPY NUMBER								 
								       ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'set_plan_jugada_ultimo_sorteo';
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pv_posicion1: '||pv_posicion1);
		dbms_output.put_line('pv_posicion2: '||pv_posicion2);
		dbms_output.put_line('pv_posicion3: '||pv_posicion3);
		dbms_output.put_line('pv_posicion4: '||pv_posicion4);
		dbms_output.put_line('pv_posicion5: '||pv_posicion5);
		dbms_output.put_line('pv_posicion6: '||pv_posicion6);
   end if;

	if pn_drawing_case = CN$DECENAS_TODAS then
		update olap_sys.plan_jugadas
		   set pos1 = pv_posicion1
			 , pos2 = pv_posicion2
			 , pos3 = pv_posicion3
			 , pos4 = pv_posicion4
			 , pos5 = pv_posicion5
			 , pos6 = pv_posicion6
		 where drawing_type = pv_drawing_type
		   and description  = 'PATRON_ULTIMO_SORTEO';
	else
		update olap_sys.plan_jugadas
		   set pos1 = pv_posicion1
			 , pos2 = pv_posicion2
			 , pos3 = pv_posicion3
			 , pos4 = pv_posicion4
			 , pos5 = pv_posicion5
			 , pos6 = pv_posicion6
		 where drawing_type = pv_drawing_type
		   and description  = 'PATRON_ULTIMO_SORTEO'
	   and drawing_case = pn_drawing_case;	
	end if;
	
	commit;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end set_plan_jugada_ultimo_sorteo;


--!proceso principal para ubicar posicion de los digitos del ultimo sorteo en base a la decena
procedure configura_posicion_ult_sorteo(pv_drawing_type             VARCHAR2
						              , pn_drawing_case             NUMBER
									  , ptbl$posicion    			gt$posicion_tbl
									  , xv_posicion1  IN OUT NOCOPY VARCHAR2
								      , xv_posicion2  IN OUT NOCOPY VARCHAR2
								      , xv_posicion3  IN OUT NOCOPY VARCHAR2
								      , xv_posicion4  IN OUT NOCOPY VARCHAR2
								      , xv_posicion5  IN OUT NOCOPY VARCHAR2
								      , xv_posicion6  IN OUT NOCOPY VARCHAR2								  
								      , x_err_code    IN OUT NOCOPY NUMBER								 
								       ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'configura_posicion_ult_sorteo';
	
	--!cursor para recuperar los rangos de la decena por posicion
	cursor c_decena_rango (pv_drawing_type             VARCHAR2
					     , pn_drawing_case             NUMBER) is
	select to_number(substr(pos1,1,instr(pos1,'-',1,1)-1)) pos11
		 , to_number(substr(pos1,instr(pos1,'-',1,1)+1)) pos12
		 , to_number(substr(pos2,1,instr(pos2,'-',1,1)-1)) pos21
		 , to_number(substr(pos2,instr(pos2,'-',1,1)+1)) pos22
		 , to_number(substr(pos3,1,instr(pos3,'-',1,1)-1)) pos31
		 , to_number(substr(pos3,instr(pos3,'-',1,1)+1)) pos32
		 , to_number(substr(pos4,1,instr(pos4,'-',1,1)-1)) pos41
		 , to_number(substr(pos4,instr(pos4,'-',1,1)+1)) pos42
		 , to_number(substr(pos5,1,instr(pos5,'-',1,1)-1)) pos51
		 , to_number(substr(pos5,instr(pos5,'-',1,1)+1)) pos52
		 , to_number(substr(pos6,1,instr(pos6,'-',1,1)-1)) pos61
		 , to_number(substr(pos6,instr(pos6,'-',1,1)+1)) pos62
	  from olap_sys.plan_jugadas
	 where drawing_type = pv_drawing_type
	   and description  = 'DECENAS'
	   and status       = 'A'
	   and drawing_case = pn_drawing_case;   
begin
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('ptbl$posicion.count: '||ptbl$posicion.count);
		dbms_output.put_line('ptbl$posicion.first: '||ptbl$posicion.first);
		dbms_output.put_line('ptbl$posicion.last: '||ptbl$posicion.last);
   end if;
	
	--!inicializando los strings
	xv_posicion1 := null;
	xv_posicion2 := null;
	xv_posicion3 := null;
	xv_posicion4 := null;
	xv_posicion5 := null;
	xv_posicion6 := null; 
	if ptbl$posicion.count > 0 then
		for j in ptbl$posicion.first..ptbl$posicion.last loop
			dbms_output.put_line('digito: '||ptbl$posicion(j).digito||' - posicion: '||ptbl$posicion(j).posicion);
			for d in c_decena_rango (pv_drawing_type => pv_drawing_type
								   , pn_drawing_case => pn_drawing_case) loop
				dbms_output.put_line('pn_drawing_case: '||pn_drawing_case||' dedena: '||d.pos11||'-'||d.pos12||' $ '||d.pos21||'-'||d.pos22||' $ '||d.pos31||'-'||d.pos32||' $ '||d.pos41||'-'||d.pos42||' $ '||d.pos51||'-'||d.pos52||' $ '||d.pos61||'-'||d.pos62);							   
				if ptbl$posicion(j).digito between d.pos11 and d.pos12 then				
					xv_posicion1 := xv_posicion1||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito1: '||ptbl$posicion(j).digito||' -> '||xv_posicion1);
				end if;
				if ptbl$posicion(j).digito between d.pos21 and d.pos22 then
					xv_posicion2 := xv_posicion2||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito2: '||ptbl$posicion(j).digito||' -> '||xv_posicion2);
				end if;
				if ptbl$posicion(j).digito between d.pos31 and d.pos32 then
					xv_posicion3 := xv_posicion3||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito3: '||ptbl$posicion(j).digito||' -> '||xv_posicion3);				
				end if;
				if ptbl$posicion(j).digito between d.pos41 and d.pos42 then
					xv_posicion4 := xv_posicion4||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito4: '||ptbl$posicion(j).digito||' -> '||xv_posicion4);				
				end if;
				if ptbl$posicion(j).digito between d.pos51 and d.pos52 then
					xv_posicion5 := xv_posicion5||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito5: '||ptbl$posicion(j).digito||' -> '||xv_posicion5);				
				end if;
				if ptbl$posicion(j).digito between d.pos61 and d.pos62 then
					xv_posicion6 := xv_posicion6||ptbl$posicion(j).posicion||',';
					dbms_output.put_line('digito6: '||ptbl$posicion(j).digito||' -> '||xv_posicion6);				
				end if;	
			end loop;		
		end loop;
	end if;


	--!removiendo la ultima coma del string	
	if xv_posicion1 is not null then
		xv_posicion1 := substr(xv_posicion1,1,length(xv_posicion1)-1);
	end if;	
	
	if xv_posicion2 is not null then
		xv_posicion2 := substr(xv_posicion2,1,length(xv_posicion2)-1);
	end if;	

	if xv_posicion3 is not null then
		xv_posicion3 := substr(xv_posicion3,1,length(xv_posicion3)-1);
	end if;	

	if xv_posicion4 is not null then
		xv_posicion4 := substr(xv_posicion4,1,length(xv_posicion4)-1);
	end if;	

	if xv_posicion5 is not null then
		xv_posicion5 := substr(xv_posicion5,1,length(xv_posicion5)-1);
	end if;	

	if xv_posicion6 is not null then
		xv_posicion6 := substr(xv_posicion6,1,length(xv_posicion6)-1);
	end if;		
/*	
	dbms_output.put_line('xv_posicion1: '||xv_posicion1);
	dbms_output.put_line('xv_posicion2: '||xv_posicion2);
	dbms_output.put_line('xv_posicion3: '||xv_posicion3);
	dbms_output.put_line('xv_posicion4: '||xv_posicion4);
	dbms_output.put_line('xv_posicion5: '||xv_posicion5);
	dbms_output.put_line('xv_posicion6: '||xv_posicion6);
*/	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end configura_posicion_ult_sorteo;	


--proceso para recuperar la posicion del digito del ultimo sorteo
procedure get_posicion_ultimo_sorteo(pv_drawing_type            	VARCHAR2
								   , pn_drawing_id              	NUMBER
								   , pn_pos1           	  			NUMBER
								   , pn_pos2           	  			NUMBER
								   , pn_pos3           	  			NUMBER
								   , pn_pos4           	  			NUMBER
								   , pn_pos5           	  			NUMBER
								   , pn_pos6           	  			NUMBER
								   , xtbl$posicion    IN OUT NOCOPY gt$posicion_tbl
								   , x_err_code   	  IN OUT NOCOPY NUMBER								 
									) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_posicion_ultimo_sorteo';

	cursor c_posicion (pv_drawing_type          VARCHAR2
					 , pn_drawing_id            NUMBER
					 , pn_digit					NUMBER	
					 , pv_b_type				VARCHAR2
					 ) is
	select digit
	     , rango_ley_tercio
	  from olap_sys.s_calculo_stats
	 where drawing_type = pv_drawing_type
	   and drawing_id 	= pn_drawing_id
	   and winner_flag  is not null
	   and digit 		= pn_digit 
	   and b_type 		= pv_b_type;	
begin  
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
   end if;

	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos1
					   , pv_b_type		 => 'B1'
					    );
		fetch c_posicion into xtbl$posicion(1).digito, xtbl$posicion(1).posicion;
		--dbms_output.put_line('fetch 1: '||xtbl$posicion.count);		
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(1).digito	  := 0; 
			xtbl$posicion(1).posicion := 0;
			dbms_output.put_line('no_data_found 1: '||xtbl$posicion(1).digito||' - '||xtbl$posicion(1).posicion);
			close c_posicion;
	end;	

	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos2
					   , pv_b_type		 => 'B2'
					    );
		fetch c_posicion into xtbl$posicion(2).digito, xtbl$posicion(2).posicion;
		--dbms_output.put_line('fetch 2: '||xtbl$posicion.count);			
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(2).digito	  := 0; 
			xtbl$posicion(2).posicion := 0;
			close c_posicion;			
	end;	

	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos3
					   , pv_b_type		 => 'B3'
					    );
		fetch c_posicion into xtbl$posicion(3).digito, xtbl$posicion(3).posicion;
		--dbms_output.put_line('fetch 3: '||xtbl$posicion.count);			
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(3).digito	  := 0; 
			xtbl$posicion(3).posicion := 0;
			close c_posicion;			
	end;
	
	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos4
					   , pv_b_type		 => 'B4'
					    );
		fetch c_posicion into xtbl$posicion(4).digito, xtbl$posicion(4).posicion;
		--dbms_output.put_line('fetch 4: '||xtbl$posicion.count);			
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(4).digito	  := 0; 
			xtbl$posicion(4).posicion := 0;
			close c_posicion;			
	end;	
	
	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos5
					   , pv_b_type		 => 'B5'
					    );
		fetch c_posicion into xtbl$posicion(5).digito, xtbl$posicion(5).posicion;
		--dbms_output.put_line('fetch 5: '||xtbl$posicion.count);			
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(5).digito	  := 0; 
			xtbl$posicion(5).posicion := 0;
			close c_posicion;			
	end;

	begin
		open c_posicion (pv_drawing_type => pv_drawing_type
					   , pn_drawing_id   => pn_drawing_id
					   , pn_digit		 => pn_pos6
					   , pv_b_type		 => 'B6'
					    );
		fetch c_posicion into xtbl$posicion(6).digito, xtbl$posicion(6).posicion;
		--dbms_output.put_line('fetch 6: '||xtbl$posicion.count);			
		close c_posicion;
	exception
		when no_data_found then
			xtbl$posicion(6).digito	  := 0; 
			xtbl$posicion(6).posicion := 0;
			close c_posicion;			
	end;

	--dbms_output.put_line('posiciones ultimo sorteo: '||xtbl$posicion.count);
	if xtbl$posicion.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		dbms_output.put_line('Posiciones para el sorteo '||pn_drawing_id||' no encontrados');
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;
		
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end get_posicion_ultimo_sorteo;


--proceso para recuperar los numeros del ultimo sorteo
procedure get_ultimo_sorteo_info(pv_drawing_type            VARCHAR2
							   , pn_drawing_id              NUMBER
							   , xn_pos1      	IN OUT NOCOPY NUMBER
							   , xn_pos2        IN OUT NOCOPY NUMBER
							   , xn_pos3        IN OUT NOCOPY NUMBER
							   , xn_pos4        IN OUT NOCOPY NUMBER
							   , xn_pos5        IN OUT NOCOPY NUMBER
							   , xn_pos6        IN OUT NOCOPY NUMBER
							   , x_err_code     IN OUT NOCOPY NUMBER								 
								) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_ultimo_sorteo_info';

begin  
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
   end if;
	
	xn_pos1 := null; 
	xn_pos2 := null;
	xn_pos3 := null;
	xn_pos4 := null;
	xn_pos5 := null;
	xn_pos6 := null;
	
	select comb1, comb2, comb3, comb4, comb5, comb6
	  into xn_pos1, xn_pos2, xn_pos3, xn_pos4, xn_pos5, xn_pos6 
	  from olap_sys.sl_gamblings
	 where gambling_type = pv_drawing_type
	   and gambling_id   = pn_drawing_id;

	dbms_output.put_line('ultimo sorteo: '||xn_pos1||' $ '||xn_pos2||' $ '||xn_pos3||' $ '||xn_pos4||' $ '||xn_pos5||' $ '||xn_pos6);
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when no_data_found then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line('Information del sorteo '||pn_drawing_id||' no encontrada');
	raise; 
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end get_ultimo_sorteo_info;
								

--!proceso principal para configurar el patron del ultimo sorteo
procedure main_ultimo_sorteo_handler(pv_drawing_type              VARCHAR2 DEFAULT 'mrtr'
						           , pn_drawing_case              NUMBER
								   , pv_add_enable           	  VARCHAR2 DEFAULT 'Y'  
						           , pv_remove_enable          	  VARCHAR2 DEFAULT 'N'  
								   , x_err_code     IN OUT NOCOPY NUMBER								 
								    ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'main_pisicion_IN_handler';
	ln$pos1           	  		NUMBER := 0;
	ln$pos2           	  		NUMBER := 0;
	ln$pos3           	  		NUMBER := 0;
	ln$pos4           	  		NUMBER := 0;
	ln$pos5           	  		NUMBER := 0;
	ln$pos6						NUMBER := 0;
	lv$digito1           	  	VARCHAR2(100);
	lv$digito2           	  	VARCHAR2(100);
	lv$digito3           	  	VARCHAR2(100);
	lv$digito4           	  	VARCHAR2(100);
	lv$digito5           	  	VARCHAR2(100);
	lv$digito6					VARCHAR2(100);	
	ltbl$posicion    			gt$posicion_tbl;	
begin  
--   if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pv_add_enable: '||pv_add_enable);
		dbms_output.put_line('pv_remove_enable: '||pv_remove_enable);
		dbms_output.put_line('--------------------------------');
--   end if;

	--!haciendo ilimitado el buffer para impimir mensajes
	dbms_output.enable(NULL);  
	
	--!banderas invalidas
	if (pv_add_enable = CV$ENABLE and pv_remove_enable = CV$ENABLE) or
	   (pv_add_enable = CV$DISABLE and pv_remove_enable = CV$DISABLE) then
		dbms_output.put_line('No pueden estar habilitadas ambas banderas');
	--!borra la posicion del digito del ultimo sorteo del plan de jugadas 
	elsif pv_add_enable = CV$DISABLE and pv_remove_enable = CV$ENABLE then
		--!proceso principal para actualziar laposicion del digito del ultimo sorteo
		set_plan_jugada_ultimo_sorteo(pv_drawing_type => pv_drawing_type
									, pn_drawing_case => pn_drawing_case
									, x_err_code   	  => x_err_code 								 
									 );
	--!actualiza el plan de jugadas con la posicion del digito del ultimo sorteo
	elsif pv_add_enable = CV$ENABLE and pv_remove_enable = CV$DISABLE then
		--recuperar el ID del ultimo sorteo
		gn$drawing_id := get_max_drawing_id (pv_drawing_type => pv_drawing_type);

		--proceso para recuperar los numeros del ultimo sorteo
		get_ultimo_sorteo_info(pv_drawing_type => pv_drawing_type
						     , pn_drawing_id   => gn$drawing_id
						     , xn_pos1         => ln$pos1
						     , xn_pos2         => ln$pos2
						     , xn_pos3         => ln$pos3
						     , xn_pos4         => ln$pos4
						     , xn_pos5         => ln$pos5
						     , xn_pos6         => ln$pos6
						     , x_err_code      => x_err_code							 
							  );
		
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then		
			--proceso para recuperar la posicion del digito del ultimo sorteo
			get_posicion_ultimo_sorteo(pv_drawing_type => pv_drawing_type
								     , pn_drawing_id   => gn$drawing_id - 1
								     , pn_pos1         => ln$pos1
								     , pn_pos2         => ln$pos2
								     , pn_pos3         => ln$pos3
								     , pn_pos4         => ln$pos4
								     , pn_pos5         => ln$pos5
								     , pn_pos6         => ln$pos6
								     , xtbl$posicion   => ltbl$posicion
								     , x_err_code      => x_err_code							 
								  	  );

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!proceso principal para ubicar posicion de los digitos del ultimo sorteo en base a la decena
				configura_posicion_ult_sorteo(pv_drawing_type => pv_drawing_type
											, pn_drawing_case => pn_drawing_case
											, ptbl$posicion   => ltbl$posicion
											, xv_posicion1    => lv$digito1
											, xv_posicion2    => lv$digito2
											, xv_posicion3    => lv$digito3
											, xv_posicion4    => lv$digito4
											, xv_posicion5    => lv$digito5
											, xv_posicion6    => lv$digito6												
											, x_err_code   	  => x_err_code							 
								             );

					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						--!proceso principal para actualziar laposicion del digito del ultimo sorteo
						set_plan_jugada_ultimo_sorteo(pv_drawing_type => pv_drawing_type
													, pn_drawing_case => pn_drawing_case
													, pv_posicion1    => lv$digito1
													, pv_posicion2    => lv$digito2
													, pv_posicion3    => lv$digito3
													, pv_posicion4    => lv$digito4
													, pv_posicion5    => lv$digito5
													, pv_posicion6    => lv$digito6								   
													, x_err_code   	  => x_err_code 								 
													 );
					end if;								 
			end if;								 
		end if;											
	end if;

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end main_ultimo_sorteo_handler;

/*
--!proceso principal para copiar metadatos entre cases ID en base a un case ID
procedure copy_metadata_handler(pn_drawing_case              NUMBER DEFAULT 1) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'copy_metadata_handler';

	cursor c_FRECUENCIA_IN (pn_drawing_case 	NUMBER) is
	select drawing_case, pos1, pos2, pos3, pos4, pos5, pos6, status
	  from olap_sys.plan_jugadas
	 where drawing_case = pn_drawing_case 
	   and description = 'FRECUENCIA_IN';

	cursor c_LEY_TERCIO_IN (pn_drawing_case 	NUMBER) is
	select DRAWING_CASE, POS1, POS2, POS3, POS4, POS5, POS6, status
	  from olap_sys.plan_jugadas
	 where DRAWING_CASE = pn_drawing_case 
	   and DESCRIPTION = 'LEY_TERCIO_IN';

	cursor c_DIGITO_POSICION_ORDEN_IN (pn_drawing_case 	NUMBER) is
	select DRAWING_CASE, POS1, POS2, POS3, POS4, POS5, POS6, status
	  from olap_sys.plan_jugadas
	 where DRAWING_CASE = pn_drawing_case 
	   and DESCRIPTION = 'DIGITO_POSICION_ORDEN_IN';

	cursor c_CICLO_APARICION (pn_drawing_case 	NUMBER) is
	select DRAWING_CASE, POS1, POS2, POS3, POS4, POS5, POS6, status
	  from olap_sys.plan_jugadas
	 where DRAWING_CASE = pn_drawing_case 
	   and DESCRIPTION = 'CICLO_APARICION';  
begin  
   if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
   end if;	

  for i in c_FRECUENCIA_IN (pn_drawing_case => pn_drawing_case) loop
    update olap_sys.plan_jugadas
       set pos1 = i.pos1
         , pos2 = i.pos2
         , pos3 = i.pos3
         , pos4 = i.pos4
         , pos5 = i.pos5
         , pos6 = i.pos6
         , status = i.status
     where drawing_case != pn_drawing_case 
       and description = 'FRECUENCIA_IN'; 
    dbms_output.put_line(sql%rowcount||' registros actualizados para FRECUENCIA_IN');   
   end loop;

  for i in c_LEY_TERCIO_IN (pn_drawing_case => pn_drawing_case) loop
    update olap_sys.plan_jugadas
       set pos1 = i.pos1
         , pos2 = i.pos2
         , pos3 = i.pos3
         , pos4 = i.pos4
         , pos5 = i.pos5
         , pos6 = i.pos6
         , status = i.status
     where drawing_case != pn_drawing_case 
       and description = 'LEY_TERCIO_IN';    
       dbms_output.put_line(sql%rowcount||' registros actualizados para LEY_TERCIO_IN');  
   end loop;   
   
  for i in c_DIGITO_POSICION_ORDEN_IN (pn_drawing_case => pn_drawing_case) loop
    update olap_sys.plan_jugadas
       set pos1 = i.pos1
         , pos2 = i.pos2
         , pos3 = i.pos3
         , pos4 = i.pos4
         , pos5 = i.pos5
         , pos6 = i.pos6
     where drawing_case != pn_drawing_case 
       and description = 'DIGITO_POSICION_ORDEN_IN';   
       dbms_output.put_line(sql%rowcount||' registros actualizados para DIGITO_POSICION_ORDEN_IN');  
   end loop;  

  for i in c_CICLO_APARICION (pn_drawing_case => pn_drawing_case) loop
    update olap_sys.plan_jugadas
       set pos1 = i.pos1
         , pos2 = i.pos2
         , pos3 = i.pos3
         , pos4 = i.pos4
         , pos5 = i.pos5
         , pos6 = i.pos6
     where drawing_case != pn_drawing_case 
       and description = 'CICLO_APARICION';   
       dbms_output.put_line(sql%rowcount||' registros actualizados para CICLO_APARICION');  
   end loop; 
   commit;

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end copy_metadata_handler;
*/									

--!proceso imprimir el resultado del calculo de los ciclos
procedure imprimir_resultado_ciclos (pn_gambling_id				  NUMBER
								   , pn_decena_rank				  NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'imprimir_resultado_ciclos';
	CN$MINIMO_CICLO		  constant number(1) := 2;
	ln$max_drawing_id			   number := 0;
	ln$diferencia			   	   number := 0;
	lv$texto					   varchar2(50);
	
	--!rank 1
	cursor c_rank1 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador1) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank 
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo1)) decena_ciclo1 from (
	select decena_rank, decena_ciclo1, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo1
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo1,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank; 	

	--!rank 2
	cursor c_rank2 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador2) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank 
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo2)) decena_ciclo2 from (
	select decena_rank, decena_ciclo2, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo2
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo2,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank; 

	--!rank 3
	cursor c_rank3 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador3) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank 
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo3)) decena_ciclo3 from (
	select decena_rank, decena_ciclo3, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo3
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo3,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank; 

	--!rank 4
	cursor c_rank4 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador4) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank 
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo4)) decena_ciclo4 from (
	select decena_rank, decena_ciclo4, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo4
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo4,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank; 

	--!rank 5
	cursor c_rank5 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador5) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo5)) decena_ciclo5 from (
	select decena_rank, decena_ciclo5, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo5
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo5,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank;

	--!rank 6
	cursor c_rank6 (pn_decena_rank   NUMBER) is
	with decena_max_tbl as (
	select decena_rank
		 , decena_string
		 , min(year) min_year
		 , max(year) max_year
		 , max(decena_contador6) max_cnt
		 , max(drawing_id) max_drawing_id
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank
	 group by decena_rank 
		 , decena_string
	), decena_ciclo_tbl as (
	select decena_rank, round(avg(decena_ciclo6)) decena_ciclo6 from (
	select decena_rank, decena_ciclo6, count(1) cnt
	  from olap_sys.decenas_ciclos_aparicion
	 where decena_rank = pn_decena_rank 
	 group by decena_rank, decena_ciclo6
	 having count(1) >= CN$MINIMO_CICLO
	 order by cnt desc)
	 group by decena_rank
	) select c.decena_rank
		   , lpad(c.decena_ciclo6,2,'0') ciclo
		   , lpad(m.max_cnt,2,'0') max_cnt
		   , m.min_year
		   , m.max_year
		   , m.decena_string
		   , m.max_drawing_id
		from decena_max_tbl m
		   , decena_ciclo_tbl c
	   where m.decena_rank = c.decena_rank; 	   
begin
	
	--!obteniendo el maximo drawing_id
	ln$max_drawing_id := pn_gambling_id;
	  
	--!rank 1
	if pn_decena_rank = 1 then
		ln$diferencia := 0;
		for r in c_rank1 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;

	--!rank 2
	if pn_decena_rank = 2 then
		ln$diferencia := 0;
		for r in c_rank2 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;	 
	
	--!rank 3
	if pn_decena_rank = 3 then
		ln$diferencia := 0;
		for r in c_rank3 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;	

	--!rank 4
	if pn_decena_rank = 4 then
		ln$diferencia := 0;
		for r in c_rank4 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;	

	--!rank 5
	if pn_decena_rank = 5 then
		ln$diferencia := 0;
		for r in c_rank5 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;	

	--!rank 6
	if pn_decena_rank = 6 then
		ln$diferencia := 0;
		for r in c_rank6 (pn_decena_rank => pn_decena_rank) loop
			ln$diferencia := ln$max_drawing_id - r.max_drawing_id;
			lv$texto := null;
			if ln$diferencia < to_number(r.ciclo) then
				lv$texto := '. No jugar en este sorteo';
			end if;
			dbms_output.put_line('decena: '||r.decena_string||' decena rank: '||r.decena_rank||' min year: '||r.min_year||' max year: '||r.max_year||' ciclo: '||r.ciclo||' contador: '||r.max_cnt||' max id: '||ln$max_drawing_id||' max decena id: '||r.max_drawing_id||' diferencia: '||ln$diferencia||lv$texto);
		end loop;
	end if;		
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end imprimir_resultado_ciclos;

--!proceso calcular el contador del ranking de la decena
procedure calcula_contador_ranking (pn_decena_rank				 NUMBER
								  , x_err_code     IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'calcula_contador_ranking';
  ln$decena_cnt					   number := 1;
  ln$decena_ciclo				   number := 0;	
	cursor c_main is
	select *
	  from olap_sys.decenas_ciclos_aparicion
	 order by drawing_id for update;	
begin
	--!rank 1
	for t in c_main loop
--		dbms_output.put_line('id: '||t.DRAWING_ID||' rank: '||t.DECENA_RANK||' ciclo: '||ln$decena_ciclo);
		if t.decena_rank = pn_decena_rank then
--			dbms_output.put_line('dentro IF id: '||t.DRAWING_ID||' rank: '||t.DECENA_RANK||' ciclo: '||ln$decena_ciclo);
			if pn_decena_rank = 1 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador1 = ln$decena_cnt
				     , decena_ciclo1 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;
	
	--!rank 2
	ln$decena_cnt	:= 1;
	ln$decena_ciclo	:= 0;		
	for t in c_main loop
		if t.decena_rank = pn_decena_rank then
			if pn_decena_rank = 2 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador2 = ln$decena_cnt
				     , decena_ciclo2 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;	

	--!rank 3
	ln$decena_cnt	:= 1;
	ln$decena_ciclo	:= 0;		
	for t in c_main loop
		if t.decena_rank = pn_decena_rank then
			if pn_decena_rank = 3 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador3 = ln$decena_cnt
				     , decena_ciclo3 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;

	--!rank 4
	ln$decena_cnt	:= 1;
	ln$decena_ciclo	:= 0;		
	for t in c_main loop
		if t.decena_rank = pn_decena_rank then
			if pn_decena_rank = 4 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador4 = ln$decena_cnt
				     , decena_ciclo4 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;

	--!rank 5
	ln$decena_cnt	:= 1;
	ln$decena_ciclo	:= 0;		
	for t in c_main loop
		if t.decena_rank = pn_decena_rank then
			if pn_decena_rank = 5 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador5 = ln$decena_cnt
				     , decena_ciclo5 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;

	--!rank 6
	ln$decena_cnt	:= 1;
	ln$decena_ciclo	:= 0;		
	for t in c_main loop
		if t.decena_rank = pn_decena_rank then
			if pn_decena_rank = 6 then
				update olap_sys.decenas_ciclos_aparicion
				   set decena_contador6 = ln$decena_cnt
				     , decena_ciclo6 = ln$decena_ciclo
				 where current of c_main;

				ln$decena_ciclo := 0;				 
			end if;			
			ln$decena_cnt := ln$decena_cnt + 1;
		end if;
		ln$decena_ciclo := ln$decena_ciclo + 1;
	end loop;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION; 
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end calcula_contador_ranking;

									
--!proceso insertar datos en el staging table
procedure ins_decenas_ciclos_aparicion (pn_year						 NUMBER
									  , pn_primos_cnt		 		 NUMBER
									  , x_err_code     IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_decenas_ciclos_aparicion';
begin
	delete olap_sys.decenas_ciclos_aparicion;
	
	insert into olap_sys.decenas_ciclos_aparicion (year, month, drawing_id, decena_string, decena_rank) 
	select year
	     , to_char(to_date(gambling_date,'dd-mm-yyyy'),'mm') mon
		 , gambling_id
		 , replace(d1,'1-9','01-09')||','||replace(d2,'1-9','01-09')||','||d3||','||d4||','||d5||','||d6 decena_string
		 , dr
	  from olap_sys.pm_mr_resultados_v2
	 where to_number(year) >= decode(pn_year,0,to_number(year),pn_year) 
	   and pn_cnt = decode(pn_primos_cnt,0,pn_cnt,pn_primos_cnt)  
	 order by gambling_id;
	
	commit;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION; 
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end ins_decenas_ciclos_aparicion;
									
									
--!proceso principal para calcular los ciclos de aparicion de una decena
procedure main_decena_ciclos_handler (pn_year					   NUMBER
									, pn_primos_cnt		 		   NUMBER DEFAULT 2
									, x_err_code     IN OUT NOCOPY NUMBER
									 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'main_decena_ciclos_handler';
begin

	--recuperar el ID del ultimo sorteo
	gn$drawing_id := get_max_drawing_id (pv_drawing_type => 'mrtr');
	  
	--!proceso insertar datos en el staging table
	ins_decenas_ciclos_aparicion (pn_year	 	=> pn_year
								, pn_primos_cnt => pn_primos_cnt
								, x_err_code 	=> x_err_code);
								
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then							
		for k in 1..6 loop
			--!proceso calcular el contador del ranking de la decena
			calcula_contador_ranking (pn_decena_rank => k
									, x_err_code=> x_err_code);	
			
				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!proceso imprimir el resultado del calculo de los ciclos
					imprimir_resultado_ciclos (pn_gambling_id => gn$drawing_id
											 , pn_decena_rank => k);
				end if;	

		end loop;		
	end if;							
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end main_decena_ciclos_handler;


--!proceso principal para limpiar las configuraciones del plan de jugadas
procedure limpiar_plan_jugadas_handler (pn_drawing_case              NUMBER DEFAULT 0) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'limpiar_plan_jugadas_handler';
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('Actualizando MULTIPLO_3');
	update olap_sys.plan_jugadas
       set pos1 = 'N'
         , pos2 = 'N'
         , pos3 = 'N'
         , pos4 = 'N'
         , pos5 = 'N'
         , pos6 = 'N'
     where description = 'MULTIPLO_3'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando MULTIPLO_4');
	update olap_sys.plan_jugadas
       set pos1 = 'N'
         , pos2 = 'N'
         , pos3 = 'N'
         , pos4 = 'N'
         , pos5 = 'N'
         , pos6 = 'N'
     where description = 'MULTIPLO_4'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando MULTIPLO_5');
	update olap_sys.plan_jugadas
       set pos1 = 'N'
         , pos2 = 'N'
         , pos3 = 'N'
         , pos4 = 'N'
         , pos5 = 'N'
         , pos6 = 'N'
     where description = 'MULTIPLO_5'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);  
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando MULTIPLO_7');
	update olap_sys.plan_jugadas
       set pos1 = 'N'
         , pos2 = 'N'
         , pos3 = 'N'
         , pos4 = 'N'
         , pos5 = 'N'
         , pos6 = 'N'
     where description = 'MULTIPLO_7'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando LEY_TERCIO');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'LEY_TERCIO'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando FRECUENCIA');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'FRECUENCIA'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando PATRON_NUMEROS');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'PATRON_NUMEROS'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando FRECUENCIA_IN');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'FRECUENCIA_IN'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando LEY_TERCIO_IN');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'LEY_TERCIO_IN'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando DIGITO_POSICION_ORDEN_IN');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'DIGITO_POSICION_ORDEN_IN'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');  
	
	dbms_output.put_line('Actualizando PATRON_ULTIMO_SORTEO');
	update olap_sys.plan_jugadas
       set pos1 = NULL
         , pos2 = NULL
         , pos3 = NULL
         , pos4 = NULL
         , pos5 = NULL
         , pos6 = NULL
     where description = 'PATRON_ULTIMO_SORTEO'
	   and drawing_case = decode(pn_drawing_case, 0, drawing_case, pn_drawing_case);   	 
    dbms_output.put_line(sql%rowcount||' registros actualizados');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end limpiar_plan_jugadas_handler;


--!proceso para imprimir los resultados de los conteos
procedure imprimir_resultados (pn_master_id				    NUMBER
							 , pn_drawing_id				NUMBER
							 , pv_gl_type					VARCHAR2
						     , x_err_code     IN OUT NOCOPY NUMBER
						      ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'imprimir_resultados';
  lv$diferencia					   varchar2(1);
  lv$b_type_prev				   varchar2(5);	
  ln$lt_prev_cnt				   number := 0;
  ln$lt_curr_cnt				   number := 0;

	cursor c_sorteo (pn_master_id				NUMBER
				   , pn_drawing_id				NUMBER) is
	select gl_type
	     , drawing_id_ini drawing_id_ini
		 , b_type_ini
		 , decode(gl_color_ini,1,'R',2,'G',3,'B',0,' ') gl_color_ini
		 , lpad(gl_cnt_ini,2,'0') gl_cnt_ini
		 , drawing_id_end drawing_id_end
		 , b_type_end
		 , decode(gl_color_end,1,'R',2,'G',3,'B',0,' ') gl_color_end
		 , lpad(gl_cnt_end,2,'0') gl_cnt_end
		 , gl_output 
		 , case when gl_cnt_end <= 1 and gl_output = '<' then 'war1' 
                when gl_cnt_end <= 1 and gl_output != '<' then 'war2' 
				when gl_cnt_end <= 2 and gl_output = '<' then 'war3' 
				when gl_cnt_end <= 2 and gl_output != '<' then 'war4' end flag
	  from olap_sys.s_gl_mapas_fre_lt_cnt 
	 where drawing_id_ini = pn_drawing_id
	   and gl_type        = pv_gl_type
	 order by seq_no;  
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_master_id: '||pn_master_id);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);		
	end if; 

	for w in c_sorteo (pn_master_id	 => pn_master_id
				     , pn_drawing_id => pn_drawing_id) loop
		dbms_output.put_line(w.gl_type 
					  ||' '||w.drawing_id_ini 
					  ||' '||w.b_type_ini 
					  ||' '||w.gl_color_ini 
					  ||' '||w.gl_cnt_ini 
					  ||' '||w.drawing_id_end 
					  ||' '||w.b_type_end 
					  ||' '||w.gl_color_end 
					  ||' '||w.gl_cnt_end 
					  ||' '||w.gl_output
					  ||' '||w.flag); 
	end loop;

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end imprimir_resultados;


--!proceso para actualizar informacion en la tabla s_gl_mapas_fre_lt_cnt
procedure upd_s_gl_mapas_fre_lt_cnt (pn_master_id				  NUMBER
								   , pn_drawing_id				  NUMBER
								   , pv_gl_type					  VARCHAR2 default 'LT'
								   , x_err_code     IN OUT NOCOPY NUMBER
									) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_s_gl_mapas_fre_lt_cnt';
  ln$drawing_id_ini				   number := pn_drawing_id;	
  ln$drawing_id_end				   number := pn_drawing_id + 1;
  
	cursor c_main (pn_drawing_id	  NUMBER) is
	select b_type
		 , color_ley_tercio lt
		 , count(1) cnt
	  from olap_sys.s_calculo_stats
	 where drawing_id = pn_drawing_id
	 group by b_type
		 , color_ley_tercio
	 order by b_type
	     , color_ley_tercio; 

	cursor c_actual (pn_master_id			  NUMBER
				   , pn_drawing_id		  NUMBER) is
	select master_id, gl_type, drawing_id_ini, b_type_ini, sum(gl_cnt_ini) sum_cnt_ini 
	  from olap_sys.s_gl_mapas_fre_lt_cnt 
	 where master_id 	  = pn_master_id 
	   and gl_type  	  = 'LT' 
	   and drawing_id_ini = pn_drawing_id
	   and b_type_ini in ('B1','B2','B3','B4','B5','B6')
	 group by master_id, gl_type, drawing_id_ini, b_type_ini; 
	
	cursor c_siguiente (pn_master_id			  NUMBER
				      , pn_drawing_id			  NUMBER) is
	select master_id, gl_type, drawing_id_end, b_type_end, sum(gl_cnt_end) sum_cnt_end 
	  from olap_sys.s_gl_mapas_fre_lt_cnt 
	 where master_id 	  = pn_master_id 
	   and gl_type  	  = 'LT' 
	   and drawing_id_end = pn_drawing_id
	   and b_type_end in ('B1','B2','B3','B4','B5','B6')
	 group by master_id, gl_type, drawing_id_end, b_type_end;  	

	cursor c_output (pn_master_id			  NUMBER
				   , pn_drawing_id			  NUMBER) is
	 select master_id, gl_type, seq_no, drawing_id_end, gl_cnt_ini, gl_cnt_end, case when gl_cnt_end > gl_cnt_ini then '>' else case when gl_cnt_end = gl_cnt_ini then '=' else '<' end end gl_output
	  from olap_sys.s_gl_mapas_fre_lt_cnt 
	 where master_id 	  = pn_master_id  
	   and drawing_id_end = pn_drawing_id
     for update; 

	cursor c_anterior (pn_drawing_id   number) is
	select distinct drawing_id_end, b_type_end, gl_color_end, predicted_manual_end, predicted_flag_end, winner_flag_end
	  from olap_sys.s_gl_mapas_fre_lt_cnt 
	 where gl_type = 'LT'
       and drawing_id_end = pn_drawing_id;	 
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
	end if;


	--!sorteo actual
	--!actualizando contadores por cada b_type
	for t in c_main (pn_drawing_id => ln$drawing_id_ini) loop
--		dbms_output.put_line(t.b_type||' | '||t.lt||' | '||t.cnt);
		update olap_sys.s_gl_mapas_fre_lt_cnt
		   set gl_cnt_ini     = t.cnt
		 where master_id      = pn_master_id
		   and gl_type 		  = pv_gl_type
		   and drawing_id_ini = ln$drawing_id_ini
		   and b_type_ini     = t.b_type
		   and gl_color_ini   = t.lt;
	end loop;

	--!actualizando totales para cada b_type
	for k in c_actual (pn_master_id  => pn_master_id
					   , pn_drawing_id => ln$drawing_id_ini) loop
--		dbms_output.put_line(k.master_id||' | '||k.gl_type||' | '||k.drawing_id_ini||' | '||k.b_type_ini||' | '||k.sum_cnt_ini);
		update olap_sys.s_gl_mapas_fre_lt_cnt
		   set gl_color_ini   = 0
		     , gl_cnt_ini     = k.sum_cnt_ini
		 where master_id      = pn_master_id
		   and gl_type 		  = 'TOTAL'
		   and drawing_id_ini = ln$drawing_id_ini
		   and b_type_ini     = k.b_type_ini;			  							  
	end loop;						  

	
	--!sorteo siguiente
	--!actualizando contadores por cada b_type
	for t in c_main (pn_drawing_id => ln$drawing_id_end) loop
--		dbms_output.put_line(t.b_type||' | '||t.lt||' | '||t.cnt);
		update olap_sys.s_gl_mapas_fre_lt_cnt
		   set gl_cnt_end     = t.cnt
		 where master_id      = pn_master_id
		   and gl_type 		  = pv_gl_type
		   and drawing_id_end = ln$drawing_id_end
		   and b_type_end     = t.b_type
		   and gl_color_end   = t.lt;
	end loop;

	--!actualizando totales para cada b_type
	for q in c_siguiente (pn_master_id  => pn_master_id
				        , pn_drawing_id => ln$drawing_id_end) loop
--		dbms_output.put_line(q.master_id||' | '||q.gl_type||' | '||q.drawing_id_end||' | '||q.b_type_end||' | '||q.sum_cnt_end);
		update olap_sys.s_gl_mapas_fre_lt_cnt
		   set gl_color_end   = 0
		     , gl_cnt_end     = q.sum_cnt_end
		 where master_id      = pn_master_id
		   and gl_type 		  = 'TOTAL'
		   and drawing_id_end = ln$drawing_id_end
		   and b_type_end     = q.b_type_end;						  							  
	end loop;

	--!identificando si los contadores de la jugada actual son mayores, iguales o menores
	for a in c_output (pn_master_id  => pn_master_id
				     , pn_drawing_id => ln$drawing_id_end) loop
		update olap_sys.s_gl_mapas_fre_lt_cnt
		   set gl_output      = a.gl_output
		 where master_id      = pn_master_id
		   and gl_type 		  = a.gl_type
		   and drawing_id_end = ln$drawing_id_end
		   and seq_no     	  = a.seq_no;					 
	end loop;			 

    for k in  c_anterior (pn_drawing_id => ln$drawing_id_ini) loop
        update olap_sys.s_gl_mapas_fre_lt_cnt 
           set predicted_manual_ini = k.predicted_manual_end
             , predicted_flag_ini = k.predicted_flag_end
             , winner_flag_ini = k.winner_flag_end
         where gl_type = 'LT'
           and drawing_id_ini = k.drawing_id_end
           and b_type_ini = k.b_type_end
           and gl_color_ini = k.gl_color_end;
    end loop;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end upd_s_gl_mapas_fre_lt_cnt;


--!insertar registros dummy en la tabla s_gl_mapas_fre_lt_cnt
procedure ins_s_gl_mapas_fre_lt_cnt(pv_gl_type                  VARCHAR2 DEFAULT 'LT'
								  , pn_master_id				NUMBER
								  , pn_drawing_id				NUMBER
								  , x_err_code    IN OUT NOCOPY NUMBER
								   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_s_gl_mapas_fre_lt_cnt';
  CV$INS_STMT			  constant varchar2(500):= 'INSERT INTO OLAP_SYS.S_GL_MAPAS_FRE_LT_CNT (MASTER_ID,GL_TYPE,SEQ_NO,DRAWING_ID_INI,B_TYPE_INI,GL_COLOR_INI,GL_CNT_INI,DRAWING_ID_END,B_TYPE_END,GL_COLOR_END,GL_CNT_END,GL_OUTPUT) VALUES (';
  ln$seq_no						   number := 1;  
  ln$drawing_id_ini				   number := pn_drawing_id;	
  ln$drawing_id_end				   number := pn_drawing_id + 1;
  ln$b_type_cnt					   number := 1;
  ln$gl_color_cnt 				   number := 1;		
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_master_id: '||pn_master_id);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);		
	end if;

	while ln$b_type_cnt <= 6 loop
--	dbms_output.put_line('ln$b_type_cnt: '||ln$b_type_cnt);
		ln$gl_color_cnt :=  1;
		while ln$gl_color_cnt <= 4 loop
--			dbms_output.put_line('ln$gl_color_cnt: '||ln$gl_color_cnt);
			olap_sys.w_common_pkg.g_dml_stmt := CV$INS_STMT;
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||pn_master_id||',';	
			if ln$gl_color_cnt = 4 then
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||chr(39)||'TOTAL'||chr(39)||',';
			else	
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||chr(39)||pv_gl_type||chr(39)||',';	
			end if;
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||ln$seq_no||',';	
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||ln$drawing_id_ini||',';			
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||chr(39)||'B'||to_char(ln$b_type_cnt)||chr(39)||',';	
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||ln$gl_color_cnt||',';	
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||0||',';	
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||ln$drawing_id_end||',';	
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||chr(39)||'B'||to_char(ln$b_type_cnt)||chr(39)||',';		
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||ln$gl_color_cnt||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||0||',';		
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'NULL)';	

			--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);			
--			begin
				execute immediate olap_sys.w_common_pkg.g_dml_stmt;
				ln$gl_color_cnt :=  ln$gl_color_cnt + 1;
				ln$seq_no :=  ln$seq_no + 1;
--			exception
--				when dup_val_on_index then
--					null; --let the process keep running
--			end;		
		end loop;
		ln$b_type_cnt := ln$b_type_cnt + 1;
	end loop;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	raise;
end ins_s_gl_mapas_fre_lt_cnt;
	

--!proceso para generar conteos de lt types para los dos ultimos sorteos
procedure generar_lt_counts_handler (pv_drawing_type			  VARCHAR2 DEFAULT 'mrtr'
								   , pn_drawing_id				  NUMBER
								   , pv_gl_type					  VARCHAR2 DEFAULT 'LT' 
								   , pv_resultado_type			  VARCHAR2 DEFAULT 'PREV'
								   , x_err_code     IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'generar_lt_counts_handler';
  ln$master_id					   number := 0;
  ln$sorteo_previo				   number := 0;
  ln$sorteo_actual				   number := 0;
  ln$sorteo				   		   number := 0;
  ln$drawing_id					   number := 0;
  ltbl$lt_curr						   gt$lt_tbl;
  ltbl$lt_prev						   gt$lt_tbl;
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);		
	end if; 
	dbms_output.put_line('--------------------------------');

	if pn_drawing_id = 0 then
		--!recuperar el ID ultimo sorteo
		ln$drawing_id := get_max_drawing_id (pv_drawing_type => pv_drawing_type);
--		dbms_output.put_line('get_max_drawing_id');
	else
		ln$drawing_id := pn_drawing_id;
--		dbms_output.put_line('igualacion');
	end if;

--	dbms_output.put_line('ln$drawing_id: '||ln$drawing_id);
	ln$sorteo_actual := ln$drawing_id;
	ln$sorteo_previo := ln$drawing_id - 1;
	--!sorteo previo
	if pv_resultado_type = 'PREV' then		
		ln$sorteo := ln$sorteo_previo;
	--!sorteo actual
	else 
		ln$sorteo := ln$sorteo_actual;
	end if;
	
	--!funcion para recuperar el master_id de la tabla olap_sys.s_gl_mapas
	ln$master_id := olap_sys.w_common_pkg.get_gl_mapa_master_id (pn_xrownum    => 1
															   , pn_seq_no	   => 1
															   , pn_drawing_id => ln$sorteo);
--	dbms_output.put_line('ln$master_id: '||ln$master_id);
	
	--!insertar registros dummy en la tabla s_gl_mapas_fre_lt_cnt
	ins_s_gl_mapas_fre_lt_cnt(pn_master_id  => ln$master_id
							, pn_drawing_id	=> ln$sorteo
						    , x_err_code    => x_err_code
						     );

	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!proceso para actualizar informacion en la tabla s_gl_mapas_fre_lt_cnt
			upd_s_gl_mapas_fre_lt_cnt (pn_master_id	 => ln$master_id
								     , pn_drawing_id => ln$sorteo
								     , x_err_code    => x_err_code
									  );
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then	
				--!proceso para imprimir los resultados de los conteos
				imprimir_resultados (pn_master_id  => ln$master_id
								   , pn_drawing_id => ln$sorteo_previo --ln$sorteo
								   , pv_gl_type    => pv_gl_type
								   , x_err_code    => x_err_code
								    );
			end if;										
	end if;

exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end generar_lt_counts_handler;


--!proceso para recuparar valores para DECENA_CNT, LAST_ID, ID_LOOPS en base a resultados de los sorteos
procedure get_resultado_sorteo_info (pv_drawing_type			  VARCHAR2 DEFAULT 'mrtr'
								   , pn_drawing_case           	  NUMBER DEFAULT 0
								   , pv_decena1					  VARCHAR2
								   , pv_decena2					  VARCHAR2
								   , pv_decena3					  VARCHAR2
								   , pv_decena4					  VARCHAR2
								   , pv_decena5					  VARCHAR2
								   , pv_decena6					  VARCHAR2
								   , pn_primo1_pos				  NUMBER
								   , pn_primo2_pos				  NUMBER
								   , xn_decena_cnt	IN OUT NOCOPY NUMBER
								   , xn_current_id	IN OUT NOCOPY NUMBER
								   , xn_last_id		IN OUT NOCOPY NUMBER
								   , xn_id_loops	IN OUT NOCOPY NUMBER
							       , x_err_code     IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_resultado_sorteo_info';
  lrc$ref_cursor          		   SYS_REFCURSOR;
begin
	dbms_output.put_line('pn_primo1_pos: '||pn_primo1_pos||'  pn_primo2_pos: '||pn_primo2_pos);
	olap_sys.w_common_pkg.g_dml_stmt := 'with resultados_tbl as (select max(gambling_id) max_id';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , count(1) cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.pm_mr_resultados_v2';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where pn_cnt = 2';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (par_cnt,none_cnt) in ((3,1),(2,2))';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (d1,d2,d3,d4,d5,d6) in ';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' ((('||chr(39)||pv_decena1||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena2||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena3||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena4||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena5||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena6||chr(39)||')))'; 
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and olap_sys.w_common_pkg.is_prime_number (COMB'||pn_primo1_pos||') = 1';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and olap_sys.w_common_pkg.is_prime_number (COMB'||pn_primo2_pos||') = 1';  
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' ) select (select max(gambling_id) from olap_sys.pm_mr_resultados_v2) global_id';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , max_id';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2) - max_id id_loops';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , cnt';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from resultados_tbl';

	--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt); 
	--!recuperar el la pareja de numeros primos del query de arriba
	open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
		fetch lrc$ref_cursor into xn_current_id, xn_last_id, xn_id_loops, xn_decena_cnt;		
	close lrc$ref_cursor;
    
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end get_resultado_sorteo_info;


--!proceso para actualizar tabla plan_jugadas R_CA_INI, R_CA_END, R_CA_COUNT en base a resultados de los sorteos
procedure plan_jugadas_upd_ca (pv_drawing_type			          VARCHAR2
							 , pn_drawing_case 					  VARCHAR2	
							 , pv_decena1	   					  VARCHAR2
							 , pv_decena2	   					  VARCHAR2
							 , pv_decena3	   					  VARCHAR2
							 , pv_decena4	   					  VARCHAR2
							 , pv_decena5	   					  VARCHAR2
							 , pv_decena6						  VARCHAR2
							 , x_err_code     		IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'plan_jugadas_upd_ca';
    lrc$ref_cursor          		 SYS_REFCURSOR; 
begin

	--dbms_output.put_line('pn_primo1_pos: '||pn_primo1_pos||'  pn_primo2_pos: '||pn_primo2_pos);
	olap_sys.w_common_pkg.g_dml_stmt := 'with r_ca_tbl as ( select gambling_id, sum_ca';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.pm_mr_resultados_v2';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where gambling_id > '||CN$MIN_GL_DRAWING_ID;
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and pn_cnt = '||CN$DOS_NUMEROS_PRIMOS;
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (par_cnt,none_cnt) in ((3,1),(2,2))';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (d1,d2,d3,d4,d5,d6) in ';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' ((('||chr(39)||pv_decena1||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena2||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena3||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena4||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena5||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena6||chr(39)||')))'; 
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||') select round(avg(sum_ca) - stddev(sum_ca)) - '||CN$HOLGURA_GL_CA||' ini';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , round(avg(sum_ca) + stddev(sum_ca)) + '||CN$HOLGURA_GL_CA||' end';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' , count(1) cnt from r_ca_tbl';	


	--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt); 
	--!recuperar el la pareja de numeros primos del query de arriba
	open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
		fetch lrc$ref_cursor into gn$value1, gn$value2, gn$value3;

		update olap_sys.plan_jugadas
		   set r_ca_ini   = gn$value1
			 , r_ca_end   = gn$value2
			 , r_ca_count = gn$value3
		 where drawing_type = pv_drawing_type
		   and description  = 'DECENAS'
		   and drawing_case = pn_drawing_case;
	close lrc$ref_cursor;	
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	

end plan_jugadas_upd_ca;


--!proceso para calcular el total de jugadas en base a decena y par de numeros primos
procedure get_jugada_sorteo_info (pv_drawing_type			      VARCHAR2 DEFAULT 'mrtr'								   
							    , pv_decena1					  VARCHAR2
							    , pv_decena2					  VARCHAR2
							    , pv_decena3					  VARCHAR2
							    , pv_decena4					  VARCHAR2
							    , pv_decena5					  VARCHAR2
							    , pv_decena6					  VARCHAR2
							    , pn_primo1_pos				  	  NUMBER
							    , pn_primo2_pos				  	  NUMBER
							    , xn_dec_primo_cnt	IN OUT NOCOPY NUMBER
							    , x_err_code        IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_jugada_sorteo_info';
  lrc$ref_cursor          		   SYS_REFCURSOR;
begin
	dbms_output.put_line('pn_primo1_pos: '||pn_primo1_pos||'  pn_primo2_pos: '||pn_primo2_pos);
	olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) cnt';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' from olap_sys.w_combination_responses_fs';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where pn_cnt = 2';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (par_cnt,none_cnt) in ((3,1),(2,2))';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and (d1,d2,d3,d4,d5,d6) in ';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' ((('||chr(39)||pv_decena1||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena2||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena3||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena4||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena5||chr(39)||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'  ,('||chr(39)||pv_decena6||chr(39)||')))'; 
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and olap_sys.w_common_pkg.is_prime_number (COMB'||pn_primo1_pos||') = 1';
    olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and olap_sys.w_common_pkg.is_prime_number (COMB'||pn_primo2_pos||') = 1';  
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and status = '||chr(39)||'Y'||chr(39);

	--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);
	--!recuperar el la pareja de numeros primos del query de arriba
	open lrc$ref_cursor for olap_sys.w_common_pkg.g_dml_stmt;
		fetch lrc$ref_cursor into xn_dec_primo_cnt;		
	close lrc$ref_cursor;	
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end get_jugada_sorteo_info;


--!!proceso para actualizar tabla plan_jugadas J_DECENA_PRIMO_CNT en base las jugadas activas en la tabla W_COMBINATION_RESPONSES_FS
procedure plan_jugadas_trg_handler (pv_drawing_type			       VARCHAR2 DEFAULT 'mrtr'
							      , pn_drawing_case                NUMBER 
								  , pn_id					       NUMBER
							      , x_err_code       IN OUT NOCOPY NUMBER)  is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'plan_jugadas_trg_handler';
	ln$decena_cnt				   number := 0;
	ln$last_id					   number := 0;
	ln$id_loops					   number := 0;	
	ln$current_id				   number := 0;	
	ln$primo1_pos				   number := 0;	
	ln$primo2_pos				   number := 0;	
	ln$decena_primo_cnt			   number := 0;	
	--pragma autonomous_transaction;
	
	cursor c_decenas (pv_drawing_type			VARCHAR2
					, pn_drawing_case           NUMBER) is
	select pos1
		 , pos2
		 , pos3
		 , pos4
		 , pos5
		 , pos6
		 , drawing_case
	  from olap_sys.plan_jugadas
	 where drawing_type = pv_drawing_type
	   and description  = 'DECENAS'
	   --and status       = 'A'
	   and drawing_case = decode(pn_drawing_case,0,drawing_case,pn_drawing_case) --!0: todos los drawing_case
	 order by drawing_case; 

	cursor c_config_primos (pv_drawing_type			  VARCHAR2
						  , pn_drawing_case           NUMBER
						  , pn_id					  NUMBER) is
	select drawing_case
         , id
         , case when pos1 = 'PR' then 1 when pos1 = '%' then 0 else 0 end pos1
		 , case when pos2 = 'PR' then 1 when pos2 = '%' then 0 else 0 end pos2
		 , case when pos3 = 'PR' then 1 when pos3 = '%' then 0 else 0 end pos3
		 , case when pos4 = 'PR' then 1 when pos4 = '%' then 0 else 0 end pos4
		 , case when pos5 = 'PR' then 1 when pos5 = '%' then 0 else 0 end pos5
		 , case when pos6 = 'PR' then 1 when pos6 = '%' then 0 else 0 end pos6
		 , seq_no
	  from olap_sys.plan_jugadas
	 where drawing_type = pv_drawing_type
	   and description  = 'CONFIG_PRIMOS_PARES_NONES'
	   --and status       = 'A'
	   and drawing_case = pn_drawing_case
	   and id           = decode(pn_id,0,id,pn_id); --!0: todos los id
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('--------------------------------');
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pn_id: '||pn_id);
	end if; 
	
	for i in c_decenas (pv_drawing_type	=> pv_drawing_type
					  , pn_drawing_case => pn_drawing_case) loop
		dbms_output.put_line(i.pos1||'  '||i.pos2||'  '||i.pos3||'  '||i.pos4||'  '||i.pos5||'  '||i.pos6);
		
		for	k in c_config_primos (pv_drawing_type => pv_drawing_type
								, pn_drawing_case => i.drawing_case
								, pn_id           => pn_id) loop	
			dbms_output.put_line(k.pos1||'  '||k.pos2||'  '||k.pos3||'  '||k.pos4||'  '||k.pos5||'  '||k.pos6);					

			if k.pos1 = 1 then
				ln$primo1_pos := 1;
				if k.pos2 = 1 then
					ln$primo2_pos := 2;
				end if;
				if k.pos3 = 1 then
					ln$primo2_pos := 3;
				end if;	
				if k.pos4 = 1 then
					ln$primo2_pos := 4;
				end if;
				if k.pos5 = 1 then
					ln$primo2_pos := 5;
				end if;	
				if k.pos6 = 1 then
					ln$primo2_pos := 6;
				end if;
			else
				if k.pos2 = 1 then
					ln$primo1_pos := 2;
					if k.pos3 = 1 then
						ln$primo2_pos := 3;
					end if;	
					if k.pos4 = 1 then
						ln$primo2_pos := 4;
					end if;
					if k.pos5 = 1 then
						ln$primo2_pos := 5;
					end if;	
					if k.pos6 = 1 then
						ln$primo2_pos := 6;
					end if;												
				else
					if k.pos3 = 1 then
						ln$primo1_pos := 3;
						if k.pos4 = 1 then
							ln$primo2_pos := 4;
						end if;
						if k.pos5 = 1 then
							ln$primo2_pos := 5;
						end if;	
						if k.pos6 = 1 then
							ln$primo2_pos := 6;
						end if;										
					else
						if k.pos5 = 1 then
							ln$primo1_pos := 5;
							if k.pos6 = 1 then
								ln$primo2_pos := 6;
							end if;	
						end if;						
					end if;					
				end if;				
			end if; 	

			ln$decena_primo_cnt := 0;
			--!proceso para calcular el total de jugadas en base a decena y par de numeros primos
			get_jugada_sorteo_info (pv_drawing_type    => pv_drawing_type							   
								  , pv_decena1	     => i.pos1
								  , pv_decena2	     => i.pos2
								  , pv_decena3	     => i.pos3
								  , pv_decena4	     => i.pos4
								  , pv_decena5	     => i.pos5
								  , pv_decena6	     => i.pos6
								  , pn_primo1_pos    => ln$primo1_pos
								  , pn_primo2_pos    => ln$primo2_pos
								  , xn_dec_primo_cnt => ln$decena_primo_cnt
								  , x_err_code       => x_err_code);
			dbms_output.put_line('xn_dec_primo_cnt|| : '||ln$decena_primo_cnt);						 

			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				if nvl(ln$decena_primo_cnt,0) > 0 then
					update olap_sys.plan_jugadas
					   set j_decena_primo_cnt = ln$decena_primo_cnt
					 where drawing_case = k.drawing_case
					   and id = k.id;
   
					x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
				else
					update olap_sys.plan_jugadas
					   set j_decena_primo_cnt = 0
					 where drawing_case = k.drawing_case
					   and id = k.id;
					
					x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
				end if;
			end if;	
		end loop; 					
	end loop;				  

	commit;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;   
end plan_jugadas_trg_handler;


--!proceso para desactivar/activar CONFIG_PRIMOS_PARES_NONES en base a las decenas
procedure plan_jugadas_conf_pr_handler (pv_drawing_type			           VARCHAR2 DEFAULT 'mrtr'
							          , pn_drawing_case                    NUMBER 
								      , pv_config_primos_list			   VARCHAR2
									  , pv_just_disable					   VARCHAR2 DEFAULT 'N'	
							          , x_err_code           IN OUT NOCOPY NUMBER) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'plan_jugadas_conf_pr_handler';
  lv$pos1						   varchar2(5);
  lv$pos2						   varchar2(5);
  lv$pos3						   varchar2(5);
  lv$pos4						   varchar2(5);
  lv$pos5						   varchar2(5);
  lv$pos6						   varchar2(5);
  cursor c_config_primos (pv_config_primos_list  VARCHAR2) is
  select replace(regexp_substr(pv_config_primos_list,'[^,]+',1,level),chr(39),null) str
    from dual 
 connect by level <= length(pv_config_primos_list)-length(replace(pv_config_primos_list,',',''))+1;  
begin
	--!desactivando todos los registros relacionados a CONFIG_PRIMOS_PARES_NONES
	update olap_sys.plan_jugadas
	   set status = 'I'
	 where drawing_type = pv_drawing_type
	   and description  = 'CONFIG_PRIMOS_PARES_NONES'
	   and drawing_case = decode(pn_drawing_case,0,drawing_case,pn_drawing_case);
	
	if pv_just_disable = CV$DISABLE then
		for t in c_config_primos (pv_config_primos_list => pv_config_primos_list) loop
			
			--!procedimiento para extraer el valor de cada posicion de config_ppn_description con este formato PR-%-%-PR-%-%
			olap_sys.w_common_pkg.read_config_primos_string (pv_config_ppn_description => t.str
														   , xv_pos1    => lv$pos1
														   , xv_pos2    => lv$pos2
														   , xv_pos3    => lv$pos3
														   , xv_pos4    => lv$pos4 
														   , xv_pos5    => lv$pos5
														   , xv_pos6    => lv$pos6);			
			
			dbms_output.put_line(lv$pos1||'  '||lv$pos2||'  '||lv$pos3||'  '||lv$pos4||'  '||lv$pos5||'  '||lv$pos6);
			--!variable usada como bandera para saber si el procedimiento esta regresando los valores esperados
			if lv$pos1 is not null then
				update olap_sys.plan_jugadas
				   set status = 'A'
				 where drawing_type = pv_drawing_type
				   and description  = 'CONFIG_PRIMOS_PARES_NONES'
				   and drawing_case = decode(pn_drawing_case,0,drawing_case,pn_drawing_case)
				   and pos1 = lv$pos1
				   and pos2 = lv$pos2
				   and pos3 = lv$pos3
				   and pos4 = lv$pos4
				   and pos5 = lv$pos5
				   and pos6 = lv$pos6;
			end if;	   
		end loop;	
	end if;
	commit;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end plan_jugadas_conf_pr_handler;   


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
									  , x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'valida_ley_tercio_in_handler';									  
    lv$pos1             	OLAP_SYS.PLAN_JUGADAS.POS1%TYPE;
    lv$pos2             	OLAP_SYS.PLAN_JUGADAS.POS2%TYPE;
    lv$pos3             	OLAP_SYS.PLAN_JUGADAS.POS3%TYPE;
    lv$pos4             	OLAP_SYS.PLAN_JUGADAS.POS4%TYPE;
    lv$pos5             	OLAP_SYS.PLAN_JUGADAS.POS5%TYPE;
    lv$pos6             	OLAP_SYS.PLAN_JUGADAS.POS6%TYPE;    
	
	pragma autonomous_transaction;
	
	cursor C_PLAN_JUGADAS (pv_pos1						   VARCHAR2
					     , pv_pos2						   VARCHAR2
					     , pv_pos3						   VARCHAR2
					     , pv_pos4						   VARCHAR2
					     , pv_pos5						   VARCHAR2
					     , pv_pos6						   VARCHAR2) is
	select DECODE(PV_POS1, null, ' AND 1=1', case when INSTR(PV_POS1,',') = 0 then ' AND LT1 IN ('||chr(39)||PV_POS1||chr(39)||')' else ' AND LT1 IN ('||chr(39)||replace(PV_POS1,',',chr(39)||','||chr(39))||chr(39)||')' end) POS1
	     , DECODE(PV_POS2, null, ' AND 1=1', case when INSTR(PV_POS2,',') = 0 then ' AND LT2 IN ('||chr(39)||PV_POS2||chr(39)||')' else ' AND LT2 IN ('||chr(39)||replace(PV_POS2,',',chr(39)||','||chr(39))||chr(39)||')' end) POS2
		 , DECODE(PV_POS3, null, ' AND 1=1', case when INSTR(PV_POS3,',') = 0 then ' AND LT3 IN ('||chr(39)||PV_POS3||chr(39)||')' else ' AND LT3 IN ('||chr(39)||replace(PV_POS3,',',chr(39)||','||chr(39))||chr(39)||')' end) POS3
		 , DECODE(PV_POS4, null, ' AND 1=1', case when INSTR(PV_POS4,',') = 0 then ' AND LT4 IN ('||chr(39)||PV_POS4||chr(39)||')' else ' AND LT4 IN ('||chr(39)||replace(PV_POS4,',',chr(39)||','||chr(39))||chr(39)||')' end) POS4
		 , DECODE(PV_POS5, null, ' AND 1=1', case when INSTR(PV_POS5,',') = 0 then ' AND LT5 IN ('||chr(39)||PV_POS5||chr(39)||')' else ' AND LT5 IN ('||chr(39)||replace(PV_POS5,',',chr(39)||','||chr(39))||chr(39)||')' end) POS5
		 , DECODE(PV_POS6, null, ' AND 1=1', case when INSTR(PV_POS6,',') = 0 then ' AND LT6 IN ('||chr(39)||PV_POS6||chr(39)||')' else ' AND LT6 IN ('||chr(39)||replace(PV_POS6,',',chr(39)||','||chr(39))||chr(39)||')' end) POS6
      from dual;		 
		 
		 
begin

	if upper(pv_description) = 'LEY_TERCIO_IN' then
		olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) CNT from OLAP_SYS.S_GL_LEY_TERCIO_PATTERNS where USE_FLAG = '||CHR(39)||'Y'||CHR(39);
		olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and MATCH_CNT = 0 and INSTR(DRAWING_CASE,:1) > 0 ';

		begin
			open C_PLAN_JUGADAS(pv_pos1	=> pv_pos1
							  , pv_pos2	=> pv_pos2
							  , pv_pos3	=> pv_pos3
							  , pv_pos4	=> pv_pos4
							  , pv_pos5	=> pv_pos5
							  , pv_pos6	=> pv_pos6);
			fetch C_PLAN_JUGADAS into lv$pos1, lv$pos2, lv$pos3, lv$pos4, lv$pos5, lv$pos6;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos1;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos2;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos3;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos4;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos5;
				olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||lv$pos6;
				--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);
				execute immediate olap_sys.w_common_pkg.g_dml_stmt into xn_record_cnt using pn_drawing_case;
				
			close C_PLAN_JUGADAS;
		exception
			when no_data_found then
				xn_record_cnt := 0;
				close C_PLAN_JUGADAS;
		end;								  
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end valida_ley_tercio_in_handler;


--!insertar registros dummy para actualizar la info posteriormente
procedure ins_dummy_s_gl_mapas_cnt(pv_gl_type					  VARCHAR2
								 , pn_xrownum					  NUMBER
								 , pn_drawing_id				  NUMBER
								 , pn_master_id					  NUMBER
								 , pv_auto_commit				  VARCHAR2	
								 , x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_dummy_s_gl_mapas_cnt';
	CV$WINNER_FLAG			constant varchar2(1) := 'N';
	ln$gl_color						 number := 0;
	
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
		dbms_output.put_line('pn_xrownum: '||pn_xrownum);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
		dbms_output.put_line('pn_master_id: '||pn_master_id);		
	end if;	

	olap_sys.w_common_pkg.g_index := 1;
	for t in 1..4 loop
		if t = 1 then
			ln$gl_color := 1;
		elsif t = 2 then
			ln$gl_color := 2; 	
		elsif t = 3 then
			ln$gl_color := 3; 	
		elsif t = 4 then
			ln$gl_color := 0; 	
		end if;
		
		INSERT INTO OLAP_SYS.S_GL_MAPAS_CNT (MASTER_ID,GL_TYPE,XROWNUM,SEQ_NO,DRAWING_ID,B1,GL_COLOR1,GL_CNT1,WINNER_FLAG1,B2,GL_COLOR2,GL_CNT2,WINNER_FLAG2,B3,GL_COLOR3,GL_CNT3,WINNER_FLAG3,B4,GL_COLOR4,GL_CNT4,WINNER_FLAG4,B5,GL_COLOR5,GL_CNT5,WINNER_FLAG5,B6,GL_COLOR6,GL_CNT6,WINNER_FLAG6)
		values (pn_master_id,pv_gl_type, pn_xrownum,olap_sys.w_common_pkg.g_index,pn_drawing_id,'B1',ln$gl_color,0,CV$WINNER_FLAG,'B2',ln$gl_color,0,CV$WINNER_FLAG,'B3',ln$gl_color,0,CV$WINNER_FLAG,'B4',ln$gl_color,0,CV$WINNER_FLAG,'B5',ln$gl_color, 0,CV$WINNER_FLAG,'B6',ln$gl_color,0,CV$WINNER_FLAG);
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
	end loop;
	if pv_auto_commit = 'Y' then
		commit;
	end if;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end ins_dummy_s_gl_mapas_cnt;


--!proceso recuperas el resultado del sorteo correspondiente
procedure get_resultado_sorteo(pv_gl_type        			   VARCHAR2
							 , pn_drawing_id				   NUMBER						 
							 , xtbl_hist_pattern IN OUT NOCOPY gt$fre_lt_cnt_tbl
							 , x_err_code       IN OUT NOCOPY NUMBER
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_resultado_sorteo';	
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	end if;	
	--!borrando datos anteriores del arreglo
	xtbl_hist_pattern.delete;
	
	with resultado_sorteo_tbl as (
		select g.GAMBLING_ID     
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
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null)) fr1
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null)) fr2
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null)) fr3
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null)) fr4
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null)) fr5
			 , to_char((select gs.color_ubicacion from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null)) fr6
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb1 and gs.b_type='B1' and gs.winner_flag is not null)) lt1
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb2 and gs.b_type='B2' and gs.winner_flag is not null)) lt2
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb3 and gs.b_type='B3' and gs.winner_flag is not null)) lt3
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb4 and gs.b_type='B4' and gs.winner_flag is not null)) lt4
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb5 and gs.b_type='B5' and gs.winner_flag is not null)) lt5
			 , to_char((select gs.color_ley_tercio from olap_sys.s_calculo_stats gs where gs.drawing_type=g.gambling_type and gs.drawing_id=g.gambling_id-1 and gs.digit=g.comb6 and gs.b_type='B6' and gs.winner_flag is not null)) lt6
  		  from olap_sys.sl_gamblings g
			 , olap_sys.w_combination_responses_fs cr
		 where g.gambling_type = cr.attribute3
		   and g.seq_id = cr.seq_id
		   and g.gambling_id = pn_drawing_id
	) select gambling_id
		   , nvl(decode(pv_gl_type,'LT',clt1,cu1),'X') gl1
		   , nvl(decode(pv_gl_type,'LT',clt2,cu2),'X') gl2
		   , nvl(decode(pv_gl_type,'LT',clt3,cu3),'X') gl3
		   , nvl(decode(pv_gl_type,'LT',clt4,cu4),'X') gl4
		   , nvl(decode(pv_gl_type,'LT',clt5,cu5),'X') gl5
		   , nvl(decode(pv_gl_type,'LT',clt6,cu6),'X') gl6
		   , nvl(decode(pv_gl_type,'LT',lt1,fr1),'0') wf1
		   , nvl(decode(pv_gl_type,'LT',lt2,fr2),'0') wf2
		   , nvl(decode(pv_gl_type,'LT',lt3,fr3),'0') wf3
		   , nvl(decode(pv_gl_type,'LT',lt4,fr4),'0') wf4
		   , nvl(decode(pv_gl_type,'LT',lt5,fr5),'0') wf5
		   , nvl(decode(pv_gl_type,'LT',lt6,fr6),'0') wf6
		bulk collect into xtbl_hist_pattern
	    from resultado_sorteo_tbl;

	if xtbl_hist_pattern.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
		if GB$SHOW_PROC_NAME then
			for t in xtbl_hist_pattern.first..xtbl_hist_pattern.last loop
				dbms_output.put_line(xtbl_hist_pattern(t).gl1
							  ||' '||xtbl_hist_pattern(t).gl2
							  ||' '||xtbl_hist_pattern(t).gl3
							  ||' '||xtbl_hist_pattern(t).gl4
							  ||' '||xtbl_hist_pattern(t).gl5
							  ||' '||xtbl_hist_pattern(t).gl6
							  ||' '||xtbl_hist_pattern(t).wf1
							  ||' '||xtbl_hist_pattern(t).wf2
							  ||' '||xtbl_hist_pattern(t).wf3
							  ||' '||xtbl_hist_pattern(t).wf4
							  ||' '||xtbl_hist_pattern(t).wf5
							  ||' '||xtbl_hist_pattern(t).wf6);
			end loop;
		end if;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;

exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_resultado_sorteo;

--!funcion para recuperar el master_id existente o uno nuevo en based a una nueva secuencia
function get_master_id (pv_gl_type        			   VARCHAR2
					  , pn_drawing_id				   NUMBER
					  , pv_return_nextval			   VARCHAR2 DEFAULT 'Y'
					  ) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_master_id';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	end if;	
	begin
		select master_id
		  into olap_sys.w_common_pkg.g_data_found
		  from olap_sys.s_gl_mapas
		 where seq_no     = 1
		   and xrownum    = 1
		   and gl_type    = pv_gl_type
		   and drawing_id = pn_drawing_id;
		if GB$SHOW_PROC_NAME then
			dbms_output.put_line('existente');	
		end if;	
	exception
		when no_data_found then
			if pv_return_nextval = CV$ENABLE then
				select olap_sys.s_gl_mapas_seq.nextval
				  into olap_sys.w_common_pkg.g_data_found
				  from dual;
				if GB$SHOW_PROC_NAME then	
					dbms_output.put_line('nueva secuencia');
				end if;
			else
				olap_sys.w_common_pkg.g_data_found := 0;
			end if;	
	end;
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line('master_id: '||olap_sys.w_common_pkg.g_data_found);
	end if;		
	return olap_sys.w_common_pkg.g_data_found;			  
end get_master_id;


--!proceso recuperas las ultimas n jugadas de los sorteos
procedure get_last_pattern(pv_gl_type        			   VARCHAR2
						 , pn_rownum					   NUMBER
						 , pn_drawing_id				   NUMBER						 
						 , pv_auto_commit				   VARCHAR2
						 , xn_master_id	     IN OUT NOCOPY NUMBER	
						 , xtbl_last_pattern IN OUT NOCOPY gt$fre_lt_tbl
						 , x_err_code       IN OUT NOCOPY NUMBER
						  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_last_pattern';	
	CV$INS_STMT				constant varchar2(200):= 'INSERT INTO OLAP_SYS.S_GL_MAPAS (MASTER_ID,GL_TYPE,XROWNUM,SEQ_NO,DRAWING_ID,DRAWING_IDS,GL_COLOR1,GL_COLOR2,GL_COLOR3,GL_COLOR4,GL_COLOR5,GL_COLOR6) VALUES (';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

	with last_pattern_tbl as (
	select gambling_id, cu1, cu2, cu3, cu4, cu5, cu6, clt1, clt2, clt3, clt4, clt5, clt6
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id  between decode(pn_drawing_id,null,(select max(sg.gambling_id)-100 from olap_sys.sl_gamblings sg), pn_drawing_id-100) and decode(pn_drawing_id,null,(select max(sg.gambling_id) from olap_sys.sl_gamblings sg), pn_drawing_id)
	 order by gambling_id desc 
	) select gambling_id
		   , nvl(decode(pv_gl_type,'LT',clt1,cu1),CV$GL_NULL) gl1
		   , nvl(decode(pv_gl_type,'LT',clt2,cu2),CV$GL_NULL) gl2
		   , nvl(decode(pv_gl_type,'LT',clt3,cu3),CV$GL_NULL) gl3
		   , nvl(decode(pv_gl_type,'LT',clt4,cu4),CV$GL_NULL) gl4
		   , nvl(decode(pv_gl_type,'LT',clt5,cu5),CV$GL_NULL) gl5
		   , nvl(decode(pv_gl_type,'LT',clt6,cu6),CV$GL_NULL) gl6
		bulk collect into xtbl_last_pattern
		from last_pattern_tbl
	   where rownum <= pn_rownum;

	if xtbl_last_pattern.count > 0 then
		--!funcion para recuperar el master_id existente o uno nuevo en based a una nueva secuencia
		xn_master_id := get_master_id (pv_gl_type   => pv_gl_type
									 , pn_drawing_id => pn_drawing_id);		
		
		--dbms_output.put_line('xtbl_last_pattern.count: '||xtbl_last_pattern.count);
		for i in xtbl_last_pattern.first..xtbl_last_pattern.last loop
			--dbms_output.put_line(xn_master_id||'  '||pv_gl_type||'  '||pn_rownum||'  '||i||'  '||xtbl_last_pattern(i).drawing_id||'  '||xtbl_last_pattern(i).gl1||'  '||xtbl_last_pattern(i).gl2||'  '||xtbl_last_pattern(i).gl3||'  '||xtbl_last_pattern(i).gl4||'  '||xtbl_last_pattern(i).gl5||'  '||xtbl_last_pattern(i).gl6);
			olap_sys.w_common_pkg.g_dml_stmt := CV$INS_STMT;
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||xn_master_id||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||chr(39)||pv_gl_type||chr(39)||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||pn_rownum||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||i||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||pn_drawing_id||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||xtbl_last_pattern(i).drawing_id||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl1||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl2||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl3||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl4||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl5||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||',';
			olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||'decode('||chr(39)||xtbl_last_pattern(i).gl6||chr(39)||','||chr(39)||'R'||chr(39)||',1,'||chr(39)||'G'||chr(39)||',2,'||chr(39)||'B'||chr(39)||',3,0)'||')';
			--ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);
			execute immediate olap_sys.w_common_pkg.g_dml_stmt;
			if pv_auto_commit = 'Y' then
				commit;
			end if;	
		end loop;
		--x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;

		--!insertar registros dummy para actualizar la info posteriormente
		ins_dummy_s_gl_mapas_cnt(pv_gl_type	    => pv_gl_type
							   , pn_xrownum	    => pn_rownum
							   , pn_drawing_id  => pn_drawing_id
							   , pn_master_id	=> xn_master_id 
							   , pv_auto_commit => pv_auto_commit
							   , x_err_code	    => x_err_code);
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;

exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_last_pattern;


--!proceso para recuperar la historia de los patrones de gl
procedure get_history_pattern(pv_gl_type        			  VARCHAR2
						    , pn_rownum					      NUMBER
							, pn_drawing_id				      NUMBER							
						    , xtbl_hist_pattern IN OUT NOCOPY gt$fre_lt_cnt_tbl
						    , x_err_code       IN OUT NOCOPY  NUMBER
						     ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_history_pattern';		
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

	with last_pattern_tbl as (
		select g.GAMBLING_ID     
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
			 , ' ' wf1
			 , ' ' wf2
			 , ' ' wf3
			 , ' ' wf4
			 , ' ' wf5
			 , ' ' wf6  		  
  		  from olap_sys.sl_gamblings g
			 , olap_sys.w_combination_responses_fs cr
		 where g.gambling_type = cr.attribute3
		   and g.seq_id = cr.seq_id
		   and g.gambling_id  between decode(pn_drawing_id,null,(select max(sg.gambling_id)-99 from olap_sys.sl_gamblings sg), pn_drawing_id-99) and decode(pn_drawing_id,null,(select max(sg.gambling_id) from olap_sys.sl_gamblings sg), pn_drawing_id)
		 order by g.gambling_id desc 
	), sorted_tbl as (
	  select gambling_id
		   , nvl(decode(pv_gl_type,'LT',clt1,cu1),CV$GL_NULL) gl1
		   , nvl(decode(pv_gl_type,'LT',clt2,cu2),CV$GL_NULL) gl2
		   , nvl(decode(pv_gl_type,'LT',clt3,cu3),CV$GL_NULL) gl3
		   , nvl(decode(pv_gl_type,'LT',clt4,cu4),CV$GL_NULL) gl4
		   , nvl(decode(pv_gl_type,'LT',clt5,cu5),CV$GL_NULL) gl5
		   , nvl(decode(pv_gl_type,'LT',clt6,cu6),CV$GL_NULL) gl6
		   , wf1
		   , wf2
		   , wf3
		   , wf4
		   , wf5
		   , wf6
		from last_pattern_tbl
	  minus
	  select gambling_id
		   , nvl(decode(pv_gl_type,'LT',clt1,cu1),CV$GL_NULL) gl1
		   , nvl(decode(pv_gl_type,'LT',clt2,cu2),CV$GL_NULL) gl2
		   , nvl(decode(pv_gl_type,'LT',clt3,cu3),CV$GL_NULL) gl3
		   , nvl(decode(pv_gl_type,'LT',clt4,cu4),CV$GL_NULL) gl4
		   , nvl(decode(pv_gl_type,'LT',clt5,cu5),CV$GL_NULL) gl5
		   , nvl(decode(pv_gl_type,'LT',clt6,cu6),CV$GL_NULL) gl6
		   , wf1
		   , wf2
		   , wf3
		   , wf4
		   , wf5
		   , wf6		   
		from last_pattern_tbl
       where rownum <= pn_rownum	  
	) select *
		bulk collect into xtbl_hist_pattern
	    from sorted_tbl
	   order by gambling_id desc;

	if xtbl_hist_pattern.count > 0 then
		--dbms_output.put_line('xtbl_hist_pattern.count: '||xtbl_hist_pattern.count);
--		for i in xtbl_hist_pattern.first..xtbl_hist_pattern.last loop
--			dbms_output.put_line(xtbl_hist_pattern(i).drawing_id||'  '||xtbl_hist_pattern(i).gl1||'  '||xtbl_hist_pattern(i).gl2||'  '||xtbl_hist_pattern(i).gl3||'  '||xtbl_hist_pattern(i).gl4||'  '||xtbl_hist_pattern(i).gl5||'  '||xtbl_hist_pattern(i).gl6);
--		end loop;
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;

exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_history_pattern;


--!reinicia contadores
procedure reset_pattern_cnt(xtbl$gl_tbl IN OUT NOCOPY gt$gl_cnt_tbl) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'reset_pattern_cnt';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	
	--!inicializando tabla de conteos
	xtbl$gl_tbl(1).gl   := 'R';
	xtbl$gl_tbl(1).cnt  := 0;
	xtbl$gl_tbl(1).flag := ' ';
	xtbl$gl_tbl(2).gl   := 'G';
	xtbl$gl_tbl(2).cnt  := 0;
	xtbl$gl_tbl(2).flag := ' ';	
	xtbl$gl_tbl(3).gl   := 'B';
	xtbl$gl_tbl(3).cnt  := 0;
	xtbl$gl_tbl(3).flag := ' ';	
	xtbl$gl_tbl(4).gl   := 'X';
	xtbl$gl_tbl(4).cnt  := 0;
	xtbl$gl_tbl(4).flag := ' ';
	
	--dbms_output.put_line('----------------------------------');
	--for k in xtbl$gl_tbl.first..xtbl$gl_tbl.last loop
	--	dbms_output.put_line(xtbl$gl_tbl(k).gl||'  '||xtbl$gl_tbl(k).cnt);	
	--end loop;	
end reset_pattern_cnt;


--!actualizar las diferentes secciones del registro
procedure upd_s_gl_mapas_cnt (pv_gl_type        			  VARCHAR2
							, pn_rownum					      NUMBER
							, pn_seq_no						  NUMBER
							, pn_drawing_id				      NUMBER
						    , pn_master_id					  NUMBER					
							, pv_gl_color					  VARCHAR2 DEFAULT NULL
							, pn_gl_cnt						  NUMBER   DEFAULT NULL		
							, pv_col_color					  VARCHAR2 DEFAULT NULL
							, pn_col_gl_cnt					  VARCHAR2 DEFAULT NULL	
							, pv_col_winner_flag			  VARCHAR2 DEFAULT NULL 
							, x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_s_gl_mapas_cnt';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_seq_no: '||pn_seq_no);
		dbms_output.put_line('pv_gl_color: '||pv_gl_color);
		dbms_output.put_line('pv_col_winner_flag: '||pv_col_winner_flag);		
	end if;
	
	olap_sys.w_common_pkg.g_dml_stmt := 'update OLAP_SYS.S_GL_MAPAS_CNT';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' set ';
	if pv_col_winner_flag is null then
		olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||pn_col_gl_cnt||' = '||pn_gl_cnt;
	else
		olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||pv_col_winner_flag||' = '||chr(39)||'Y'||chr(39);
	end if;	
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' where  gl_type = '||chr(39)||pv_gl_type||chr(39);
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and xrownum = '||pn_rownum;
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and seq_no = decode('||pn_seq_no||',0,4,'||pn_seq_no||')';
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and drawing_id = '||pn_drawing_id;	
	olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt ||' and master_id = '||pn_master_id;

--	ins_tmp_testing (pv_valor => olap_sys.w_common_pkg.g_dml_stmt);	

	execute immediate olap_sys.w_common_pkg.g_dml_stmt;
--dbms_output.put_line('paso 20');
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
end upd_s_gl_mapas_cnt;


--!proceso para contar las repeticiones de los patrones
procedure get_pattern_cnt(pv_gl_type        			  VARCHAR2
						, pn_rownum					      NUMBER
						, pn_drawing_id				      NUMBER
						, pn_master_id					  NUMBER						
						, ptbl_last_pattern 			 gt$fre_lt_tbl
						, ptbl_hist_pattern 			 gt$fre_lt_cnt_tbl
						, x_err_code       IN OUT NOCOPY NUMBER
						 ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_pattern_cnt';
	CN$ROWNUM1				constant number(1) := 1;
	CN$ROWNUM2				constant number(1) := 2;
	ln$loop_index			number := ptbl_hist_pattern.first;
	ln$prev_index			number := 0;
	ln$next_index			number := 0;
	ln$seq_no				number := 0;
	ltbl$gl_tbl				gt$gl_cnt_tbl;
	ltbl$results            dbms_sql.varchar2_table;
    CV$INS_STMT				constant varchar2(500):= 'INSERT INTO OLAP_SYS.S_GL_MAPAS_CNT (GL_TYPE,XROWNUM,SEQ_NO,DRAWING_ID,B1,GL_COLOR1,GL_CNT1,WINNER_FLAG1,B2,GL_COLOR2,GL_CNT2,WINNER_FLAG2,B3,GL_COLOR3,GL_CNT3,WINNER_FLAG3,B4,GL_COLOR4,GL_CNT4,WINNER_FLAG4,B5,GL_COLOR5,GL_CNT5,WINNER_FLAG5,B6,GL_COLOR6,GL_CNT6,WINNER_FLAG6)';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;
	
	if pn_rownum = 1 then
		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B1
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl1||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl1 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl1 = ptbl_hist_pattern(ln$loop_index).gl1 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt||'  '||ltbl$gl_tbl(1).change_flag);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt||'  '||ltbl$gl_tbl(2).change_flag);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt||'  '||ltbl$gl_tbl(3).change_flag);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt  := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt||'  '||ltbl$gl_tbl(4).change_flag);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;

		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B1: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR1'
							  , pn_col_gl_cnt	   => 'GL_CNT1'
							  , x_err_code 		   => x_err_code);			
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B2
		ln$loop_index := ptbl_hist_pattern.first;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl2 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl2 = ptbl_hist_pattern(ln$loop_index).gl2 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B2: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR2'
							  , pn_col_gl_cnt	   => 'GL_CNT2'
							  , x_err_code 		   => x_err_code);

		end loop;	

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B3
		ln$loop_index := ptbl_hist_pattern.first;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl3 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl3 = ptbl_hist_pattern(ln$loop_index).gl3 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B3: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;							  
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;							  
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;							  
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR3'
							  , pn_col_gl_cnt	   => 'GL_CNT3'
							  , x_err_code 		   => x_err_code);				
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B4
		ln$loop_index := ptbl_hist_pattern.first;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl4 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl4 = ptbl_hist_pattern(ln$loop_index).gl4 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt  := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B4: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;				
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR4'
							  , pn_col_gl_cnt	   => 'GL_CNT4'
							  , x_err_code 		   => x_err_code);	
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B5
		ln$loop_index := ptbl_hist_pattern.first;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl5 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl5 = ptbl_hist_pattern(ln$loop_index).gl5 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B5: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;	
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;	
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;		
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;	
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR5'
							  , pn_col_gl_cnt	   => 'GL_CNT5'
							  , x_err_code 		   => x_err_code);				
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B6
		ln$loop_index := ptbl_hist_pattern.first;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6);
			--if ptbl_hist_pattern(ln$loop_index).gl6 != ltbl$gl_tbl(4).gl then
				if ptbl_last_pattern(CN$ROWNUM1).gl6 = ptbl_hist_pattern(ln$loop_index).gl6 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B6: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR6'
							  , pn_col_gl_cnt	   => 'GL_CNT6'
							  , x_err_code 		   => x_err_code);	
		end loop;	

	elsif pn_rownum = 2 then
		--dbms_output.put_line('ptbl_last_pattern(CN$ROWNUM1).gl1: '||ptbl_last_pattern(CN$ROWNUM1).gl1);
		--dbms_output.put_line('ptbl_last_pattern(CN$ROWNUM2).gl1: '||ptbl_last_pattern(CN$ROWNUM2).gl1);
		
		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B1
		ln$loop_index := ptbl_hist_pattern.first;
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl1||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;	
			--if ptbl_hist_pattern(ln$loop_index).gl1 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl1 != ltbl$gl_tbl(4).gl then				
				--dbms_output.put_line('step 10');
				--dbms_output.put_line(ptbl_last_pattern(CN$ROWNUM1).gl1||' = '||ptbl_hist_pattern(ln$loop_index).gl1||'  loop_index: '||ln$loop_index||'  i: '||i);
				--dbms_output.put_line(ptbl_last_pattern(CN$ROWNUM2).gl1||' = '||ptbl_hist_pattern(ln$next_index).gl1||'  ln$next_index: '||ln$next_index||'  i: '||i);
				if ptbl_last_pattern(CN$ROWNUM1).gl1 = ptbl_hist_pattern(ln$loop_index).gl1 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl1 = ptbl_hist_pattern(ln$next_index).gl1 then
					--dbms_output.put_line('step 20');
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index - 1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt||'  '||ltbl$gl_tbl(1).change_flag);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt||'  '||ltbl$gl_tbl(2).change_flag);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt||'  '||ltbl$gl_tbl(3).change_flag);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl1||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl1 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt  := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf1;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt||'  '||ltbl$gl_tbl(4).change_flag);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;

		ln$seq_no := 0;
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B1: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := 'B1: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;	
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR1'
							  , pn_col_gl_cnt	   => 'GL_CNT1'
							  , x_err_code 		   => x_err_code);			
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B2
		ln$loop_index := ptbl_hist_pattern.first; 
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl2||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;
			--if ptbl_hist_pattern(ln$loop_index).gl2 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl2 != ltbl$gl_tbl(4).gl then
			--	dbms_output.put_line('step 10');
			--	dbms_output.put_line(ptbl_last_pattern(CN$ROWNUM1).gl2||' = '||ptbl_hist_pattern(ln$loop_index).gl2||'  loop_index: '||ln$loop_index||'  i: '||i);
			--	dbms_output.put_line(ptbl_last_pattern(CN$ROWNUM2).gl2||' = '||ptbl_hist_pattern(ln$next_index).gl2||'  ln$next_index: '||ln$next_index||'  i: '||i);
				if ptbl_last_pattern(CN$ROWNUM1).gl2 = ptbl_hist_pattern(ln$loop_index).gl2 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl2 = ptbl_hist_pattern(ln$next_index).gl2 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line('loop_index: '||ln$loop_index||'  '||ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl2||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl2 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf2;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;
			ln$loop_index :=  ln$loop_index + 1;
		end loop;

		ln$seq_no := 0;		
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B2: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B2: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
				--!actualizar las diferentes secciones del registro
				upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
								  , pn_rownum		   => pn_rownum
								  , pn_seq_no		   => ln$seq_no
								  , pn_drawing_id	   => pn_drawing_id
								  , pn_master_id	   => pn_master_id	
								  , pv_gl_color		   => ltbl$gl_tbl(k).gl
								  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
								  , pv_col_color	   => 'GL_COLOR2'
								  , pn_col_gl_cnt	   => 'GL_CNT2'
								  , x_err_code 		   => x_err_code);	
		end loop;	

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B3
		ln$loop_index := ptbl_hist_pattern.first;
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl3||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;
			--if ptbl_hist_pattern(ln$loop_index).gl3 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl3 != ltbl$gl_tbl(4).gl then
				--dbms_output.put_line('next_index: '||ln$next_index);
				if ptbl_last_pattern(CN$ROWNUM1).gl3 = ptbl_hist_pattern(ln$loop_index).gl3 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl3 = ptbl_hist_pattern(ln$next_index).gl3 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl3||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl3 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt  := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf3;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;	
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B3: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B3: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR3'
							  , pn_col_gl_cnt	   => 'GL_CNT3'
							  , x_err_code 		   => x_err_code);
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
	
		--!B4
		ln$loop_index := ptbl_hist_pattern.first;
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl4||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;
			--if ptbl_hist_pattern(ln$loop_index).gl4 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl4 != ltbl$gl_tbl(4).gl then
				--dbms_output.put_line('next_index: '||ln$next_index);
				if ptbl_last_pattern(CN$ROWNUM1).gl4 = ptbl_hist_pattern(ln$loop_index).gl4 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl4 = ptbl_hist_pattern(ln$next_index).gl4 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt  := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl4||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl4 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf4;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;	
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B4: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B4: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR4'
							  , pn_col_gl_cnt	   => 'GL_CNT4'
							  , x_err_code 		   => x_err_code);
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B5
		ln$loop_index := ptbl_hist_pattern.first;
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl5||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;
			--if ptbl_hist_pattern(ln$loop_index).gl5 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl5 != ltbl$gl_tbl(4).gl then
				--dbms_output.put_line('next_index: '||ln$next_index);
				if ptbl_last_pattern(CN$ROWNUM1).gl5 = ptbl_hist_pattern(ln$loop_index).gl5 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl5 = ptbl_hist_pattern(ln$next_index).gl5 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt  := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt  := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl5||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl5 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt  := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf5;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;	
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B5: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B5: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR5'
							  , pn_col_gl_cnt	   => 'GL_CNT5'
							  , x_err_code 		   => x_err_code);
		end loop;

		--!reinicia contadores
		reset_pattern_cnt(xtbl$gl_tbl => ltbl$gl_tbl);
		
		--!B6
		ln$loop_index := ptbl_hist_pattern.first;
		ln$next_index := 0;
		for i in ptbl_hist_pattern.first..ptbl_hist_pattern.last loop
			--dbms_output.put_line(i||' - > index: '||ln$loop_index||'  '||ptbl_hist_pattern(i).drawing_id||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6||'  '||ptbl_hist_pattern(i).gl6);
			if ln$next_index < ptbl_hist_pattern.count then
				ln$next_index := ln$loop_index + 1;
			end if;
			--if ptbl_hist_pattern(ln$loop_index).gl6 != ltbl$gl_tbl(4).gl and 
			--   ptbl_hist_pattern(ln$next_index).gl6 != ltbl$gl_tbl(4).gl then
				--dbms_output.put_line('next_index: '||ln$next_index);
				if ptbl_last_pattern(CN$ROWNUM1).gl6 = ptbl_hist_pattern(ln$loop_index).gl6 and 
				   ptbl_last_pattern(CN$ROWNUM2).gl6 = ptbl_hist_pattern(ln$next_index).gl6 then
					if ln$loop_index > 1 then
						ln$prev_index := ln$loop_index-1;
						--dbms_output.put_line('prev_index: '||ln$prev_index);
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(1).gl);
						--!red
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(1).gl then
							ltbl$gl_tbl(1).cnt := ltbl$gl_tbl(1).cnt + 1;
							ltbl$gl_tbl(1).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(1).gl||'  '||ltbl$gl_tbl(1).cnt);
						end if;
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(2).gl);
						--!green
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(2).gl then
							ltbl$gl_tbl(2).cnt := ltbl$gl_tbl(2).cnt + 1;
							ltbl$gl_tbl(2).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(2).gl||'  '||ltbl$gl_tbl(2).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(3).gl);
						--!blue
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(3).gl then
							ltbl$gl_tbl(3).cnt := ltbl$gl_tbl(3).cnt + 1;
							ltbl$gl_tbl(3).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(3).gl||'  '||ltbl$gl_tbl(3).cnt);
						end if;	
						--dbms_output.put_line(ptbl_hist_pattern(ln$prev_index).gl6||' = '||ltbl$gl_tbl(4).gl);
						--!null
						if ptbl_hist_pattern(ln$prev_index).gl6 = ltbl$gl_tbl(4).gl then
							ltbl$gl_tbl(4).cnt := ltbl$gl_tbl(4).cnt + 1;
							ltbl$gl_tbl(4).flag := ptbl_hist_pattern(ln$prev_index).wf6;
							--dbms_output.put_line(ltbl$gl_tbl(4).gl||'  '||ltbl$gl_tbl(4).cnt);
						end if;						
					end if;			
				end if;
			--end if;	
			ln$loop_index :=  ln$loop_index + 1;
		end loop;
		
		ln$seq_no := 0;	
		for k in ltbl$gl_tbl.first..ltbl$gl_tbl.last loop
			--dbms_output.put_line('B6: '||ltbl$gl_tbl(k).gl||'  '||ltbl$gl_tbl(k).cnt);	
			if ltbl$gl_tbl(k).gl = 'R' then
				ltbl$results(1) := ltbl$results(1)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;
				ln$seq_no := 1;
			elsif ltbl$gl_tbl(k).gl = 'G' then
				ltbl$results(2) := ltbl$results(2)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 2;
			elsif ltbl$gl_tbl(k).gl = 'B' then
				ltbl$results(3) := ltbl$results(3)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 3;
			elsif ltbl$gl_tbl(k).gl = 'X' then
				ltbl$results(4) := ltbl$results(4)||'  B6: '||ltbl$gl_tbl(k).gl||' '||lpad(ltbl$gl_tbl(k).cnt,2,'0')||' '||ltbl$gl_tbl(k).flag;	
				ln$seq_no := 4;
			end if;	
			--!actualizar las diferentes secciones del registro
			upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
							  , pn_rownum		   => pn_rownum
							  , pn_seq_no		   => ln$seq_no
							  , pn_drawing_id	   => pn_drawing_id
							  , pn_master_id	   => pn_master_id	
							  , pv_gl_color		   => ltbl$gl_tbl(k).gl
							  , pn_gl_cnt		   => ltbl$gl_tbl(k).cnt
							  , pv_col_color	   => 'GL_COLOR6'
							  , pn_col_gl_cnt	   => 'GL_CNT6'
							  , x_err_code 		   => x_err_code);
		end loop;	
	end if;

	--!imprimiendo los valores finales
	--dbms_output.put_line('----------------------------------------------------------');
	--dbms_output.put_line(ltbl$results(1));
	--dbms_output.put_line(ltbl$results(2));
	--dbms_output.put_line(ltbl$results(3));
	--dbms_output.put_line(ltbl$results(4));

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_pattern_cnt;


--!proceso para actualizar el winner flag en el historico del patron
procedure upd_hist_pattern(pv_gl_type        			  VARCHAR2
						 , pn_rownum					  NUMBER
						 , pn_drawing_id				  NUMBER
						 , pn_master_id	   				  NUMBER
						 , ptbl_result_sorteo 			  gt$fre_lt_cnt_tbl
						 , x_err_code       IN OUT NOCOPY NUMBER
						  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_hist_pattern';
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||upper(pv_gl_type));
		dbms_output.put_line('pn_rownum: '||pn_rownum);	
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);	
		dbms_output.put_line('pn_master_id: '||pn_master_id);
		dbms_output.put_line('ptbl_result_sorteo.count: '||ptbl_result_sorteo.count);			
	end if;	
	
	for t in ptbl_result_sorteo.first..ptbl_result_sorteo.last loop
		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf1)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl1
						  , pv_col_winner_flag => 'WINNER_FLAG1'
						  , x_err_code 		   => x_err_code);	

		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf2)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl2
						  , pv_col_winner_flag => 'WINNER_FLAG2'
						  , x_err_code 		   => x_err_code);							  

		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf3)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl3
						  , pv_col_winner_flag => 'WINNER_FLAG3'
						  , x_err_code 		   => x_err_code);	

		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf4)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl4
						  , pv_col_winner_flag => 'WINNER_FLAG4'
						  , x_err_code 		   => x_err_code);	

		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf5)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl5
						  , pv_col_winner_flag => 'WINNER_FLAG5'
						  , x_err_code 		   => x_err_code);							  

		--!actualizar las diferentes secciones del registro
		upd_s_gl_mapas_cnt (pv_gl_type         => pv_gl_type
						  , pn_rownum		   => pn_rownum
						  , pn_seq_no		   => to_number(ptbl_result_sorteo(t).wf6)
						  , pn_drawing_id	   => pn_drawing_id
						  , pn_master_id	   => pn_master_id
						  , pv_gl_color		   => ptbl_result_sorteo(t).gl6
						  , pv_col_winner_flag => 'WINNER_FLAG6'
						  , x_err_code 		   => x_err_code);	
	end loop;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end upd_hist_pattern;

--!proceso para imprimir los resultados
procedure imprime_resultados(pv_gl_type        			  VARCHAR2
						   , pn_rownum					  NUMBER
						   , pn_drawing_id				  NUMBER
						   , pn_master_id				  NUMBER
						    ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'imprime_resultados';
	cursor c_mapas (pv_gl_type        			  VARCHAR2
			      , pn_rownum					  NUMBER
			      , pn_drawing_id				  NUMBER						 
				   ) is
	select xrownum
         , seq_no
         , drawing_id
		 , gl_type
		 , DECODE(GL_COLOR1,1,'R',2,'G',3,'B','X') CLR1
		 , DECODE(GL_COLOR2,1,'R',2,'G',3,'B','X') CLR2
		 , DECODE(GL_COLOR3,1,'R',2,'G',3,'B','X') CLR3
		 , DECODE(GL_COLOR4,1,'R',2,'G',3,'B','X') CLR4
		 , DECODE(GL_COLOR5,1,'R',2,'G',3,'B','X') CLR5
		 , DECODE(GL_COLOR6,1,'R',2,'G',3,'B','X') CLR6
	  from OLAP_SYS.S_GL_MAPAS
	 where gl_type = pv_gl_type
	   and xrownum = pn_rownum
	   and drawing_id = pn_drawing_id
	   and master_id = pn_master_id	
	union
	select xrownum
         , seq_no
         , drawing_id
		 , gl_type
		 , DECODE(GL_COLOR1,1,'R',2,'G',3,'B','X') CLR1
		 , DECODE(GL_COLOR2,1,'R',2,'G',3,'B','X') CLR2
		 , DECODE(GL_COLOR3,1,'R',2,'G',3,'B','X') CLR3
		 , DECODE(GL_COLOR4,1,'R',2,'G',3,'B','X') CLR4
		 , DECODE(GL_COLOR5,1,'R',2,'G',3,'B','X') CLR5
		 , DECODE(GL_COLOR6,1,'R',2,'G',3,'B','X') CLR6
	  from OLAP_SYS.S_GL_MAPAS
	 where gl_type = pv_gl_type
	   and xrownum = pn_rownum
	   and drawing_id = pn_drawing_id -1
	   and master_id = pn_master_id	   
	 order by xrownum, seq_no;	   

	cursor c_mapas_cnt (pv_gl_type        			  VARCHAR2
					  , pn_rownum					  NUMBER
					  , pn_drawing_id				  NUMBER						 
					   ) is
	select DRAWING_ID
	     , GL_TYPE
		 , B1, DECODE(GL_COLOR1,1,'R',2,'G',3,'B','X') CLR1, LPAD(GL_CNT1,2,'0') CNT1, NVL(WINNER_FLAG1,' ') WF1
		 , B2, DECODE(GL_COLOR2,1,'R',2,'G',3,'B','X') CLR2, LPAD(GL_CNT2,2,'0') CNT2, NVL(WINNER_FLAG2,' ') WF2
		 , B3, DECODE(GL_COLOR3,1,'R',2,'G',3,'B','X') CLR3, LPAD(GL_CNT3,2,'0') CNT3, NVL(WINNER_FLAG3,' ') WF3
		 , B4, DECODE(GL_COLOR4,1,'R',2,'G',3,'B','X') CLR4, LPAD(GL_CNT4,2,'0') CNT4, NVL(WINNER_FLAG4,' ') WF4
		 , B5, DECODE(GL_COLOR5,1,'R',2,'G',3,'B','X') CLR5, LPAD(GL_CNT5,2,'0') CNT5, NVL(WINNER_FLAG5,' ') WF5
		 , B6, DECODE(GL_COLOR6,1,'R',2,'G',3,'B','X') CLR6, LPAD(GL_CNT6,2,'0') CNT6, NVL(WINNER_FLAG6,' ') WF6
	  from olap_sys.s_gl_mapas_cnt
	 where gl_type = pv_gl_type
	   and xrownum = pn_rownum
	   and drawing_id = pn_drawing_id
	   and master_id = pn_master_id
	 order by xrownum, seq_no;	
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;
	
		for r in c_mapas (pv_gl_type    => pv_gl_type
						, pn_rownum	 => pn_rownum
						, pn_drawing_id => pn_drawing_id							 
						 ) loop
			dbms_output.put_line(r.GL_TYPE
						  ||' '||r.DRAWING_ID
						  ||' '||r.CLR1
						  ||' '||r.CLR2
						  ||' '||r.CLR3
						  ||' '||r.CLR4
						  ||' '||r.CLR5
						  ||' '||r.CLR6
								);				  
		end loop;					  

		for r in c_mapas_cnt (pv_gl_type    => pv_gl_type
							, pn_rownum	 => pn_rownum
							, pn_drawing_id => pn_drawing_id							 
							 ) loop
			dbms_output.put_line(r.GL_TYPE||' '||r.DRAWING_ID
						  ||' '||r.B1||' '||r.CLR1||' '||r.CNT1||' '||r.WF1
						  ||' '||r.B2||' '||r.CLR2||' '||r.CNT2||' '||r.WF2
						  ||' '||r.B3||' '||r.CLR3||' '||r.CNT3||' '||r.WF3
						  ||' '||r.B4||' '||r.CLR4||' '||r.CNT4||' '||r.WF4
						  ||' '||r.B5||' '||r.CLR5||' '||r.CNT5||' '||r.WF5
						  ||' '||r.B6||' '||r.CLR6||' '||r.CNT6||' '||r.WF6
			);				  
		end loop;					  
	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end imprime_resultados;
		   
	
--!proceso para realizar conteos de LT y FR de los ultimoas 100 jugadas en base a la ultima jugadas
procedure get_frec_lt_count_handler(pv_gl_type                     VARCHAR2 DEFAULT 'LT'
								  , pn_rownum					   NUMBER
								  , pn_drawing_id				   NUMBER DEFAULT NULL
								  , pv_auto_commit				   VARCHAR2 DEFAULT 'N'	
								  , pv_get_resultado			   VARCHAR2 DEFAULT 'Y'	
								  , pv_insert_pattern			   VARCHAR2 DEFAULT 'Y'
								  , pv_resultado_type			   VARCHAR2 DEFAULT 'PREV'
								  , x_err_code       IN OUT NOCOPY NUMBER	
									) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_frec_lt_count_handler';
	ln$master_id					number := 0;
	ln$sorteo_actual				number := 0;
	ln$sorteo_previo				number := 0;
	ln$sorteo				        number := 0;
	ltbl$last_pattern		gt$fre_lt_tbl;
	ltbl$hist_pattern		gt$fre_lt_cnt_tbl;
	le$last_pattern                 exception;
	le$hist_pattern                 exception;	
	le$resultado_sorteo             exception;	
	le$master_id_sorteo             exception;
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||upper(pv_gl_type));
		dbms_output.put_line('pn_rownum: '||pn_rownum);		
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);		
		dbms_output.put_line('pv_auto_commit: '||pv_auto_commit);		
		dbms_output.put_line('pv_get_resultado: '||pv_get_resultado);		
		dbms_output.put_line('pv_insert_pattern: '||pv_insert_pattern);	
		dbms_output.put_line('pv_resultado_type: '||pv_resultado_type);			
	end if;

	ln$sorteo_actual := pn_drawing_id;
	--!sorteo previo
	if pv_resultado_type = 'PREV' then
		ln$sorteo_previo := pn_drawing_id;
		ln$sorteo        := ln$sorteo_previo;
	--!sorteo actual
	else 
		ln$sorteo := ln$sorteo_actual;
	end if;
		
	if pv_insert_pattern = CV$ENABLE then
--dbms_output.put_line('xln$sorteo: '||ln$sorteo);		
		--!proceso recuperas las ultimas n jugadas de los sorteos
		get_last_pattern(pv_gl_type        => upper(pv_gl_type)
					   , pn_rownum		   => pn_rownum
					   , pn_drawing_id	   => ln$sorteo
					   , pv_auto_commit    => pv_auto_commit
					   , xtbl_last_pattern => ltbl$last_pattern
					   , xn_master_id      => ln$master_id
					   , x_err_code        => x_err_code
						);

		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				--!proceso para recuperar la historia de los patrones de gl
				get_history_pattern(pv_gl_type        => upper(pv_gl_type)
								  , pn_rownum		  => pn_rownum
								  , pn_drawing_id	  => ln$sorteo
								  , xtbl_hist_pattern => ltbl$hist_pattern
								  , x_err_code        => x_err_code
								   );
			
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
				if ltbl$last_pattern.count > 0 then
					if ltbl$hist_pattern.count > 0 then
						--!proceso para contar las repeticiones de los patrones
						get_pattern_cnt(pv_gl_type        => upper(pv_gl_type)
									  , pn_rownum		  => pn_rownum
									  , pn_drawing_id	  => ln$sorteo
									  , pn_master_id      => ln$master_id
									  , ptbl_last_pattern => ltbl$last_pattern
									  , ptbl_hist_pattern => ltbl$hist_pattern
									  , x_err_code        => x_err_code
									   );
						
						if upper(pv_auto_commit) = 'Y' then
							commit;
						end if;	

						if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
							if pv_get_resultado = CV$ENABLE and ln$sorteo_previo > 0 then
								ln$sorteo_actual := pn_drawing_id;
								--!proceso recuperas el resultado del sorteo correspondiente
								get_resultado_sorteo(pv_gl_type        => upper(pv_gl_type)
												   , pn_drawing_id	   => ln$sorteo_actual
												   , xtbl_hist_pattern => ltbl$hist_pattern
												   , x_err_code        => x_err_code
													);						

								if ltbl$hist_pattern.count > 0 then
									--!proceso para actualizar el winner flag en el historico del patron
									upd_hist_pattern(pv_gl_type        	=> upper(pv_gl_type)
												   , pn_rownum			=> pn_rownum
												   , pn_drawing_id		=> ln$sorteo_previo
												   , pn_master_id       => ln$master_id
												   , ptbl_result_sorteo => ltbl$hist_pattern
												   , x_err_code         => x_err_code
													);
													
									if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then				
										--!proceso para imprimir los resultados
										imprime_resultados(pv_gl_type    => upper(pv_gl_type)
														 , pn_rownum	 => pn_rownum
														 , pn_drawing_id => ln$sorteo_previo
														 , pn_master_id  => ln$master_id												 
														  ); 
									end if;
								else									
									raise le$resultado_sorteo;
								end if;	
							else
								if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then				
									--!proceso para imprimir los resultados
									imprime_resultados(pv_gl_type    => upper(pv_gl_type)
													 , pn_rownum	 => pn_rownum
													 , pn_drawing_id => ln$sorteo
													 , pn_master_id  => ln$master_id												 
													  ); 
								end if;						
							end if;
						end if;		  
					else
						raise le$hist_pattern;
					end if;
				else
					raise le$last_pattern;
				end if;	
			end if;					 
		end if;
	else		
		--!sorteo actual
		--!funcion para recuperar el master_id del sorteo actual
		ln$master_id := get_master_id (pv_gl_type        => upper(pv_gl_type)
									 , pn_drawing_id	 => ln$sorteo_actual
									 , pv_return_nextval => 'N'
									  ); 
		
		if pv_insert_pattern = CV$DISABLE then
			--!el master ID del sorteo actual existe
			if ln$master_id > 0 then
				--!proceso recuperas el resultado del sorteo correspondiente
				get_resultado_sorteo(pv_gl_type        => upper(pv_gl_type)
								   , pn_drawing_id	   => ln$sorteo_actual
								   , xtbl_hist_pattern => ltbl$hist_pattern
								   , x_err_code        => x_err_code
									);						

				if ltbl$hist_pattern.count > 0 then
					--!proceso para actualizar el winner flag en el historico del patron
					upd_hist_pattern(pv_gl_type        	=> upper(pv_gl_type)
								   , pn_rownum			=> pn_rownum
								   , pn_drawing_id		=> ln$sorteo_actual
								   , pn_master_id       => ln$master_id
								   , ptbl_result_sorteo => ltbl$hist_pattern
								   , x_err_code         => x_err_code
									);
									
					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then				
						--!proceso para imprimir los resultados
						imprime_resultados(pv_gl_type    => upper(pv_gl_type)
										 , pn_rownum	 => pn_rownum
										 , pn_drawing_id => ln$sorteo_actual
										 , pn_master_id  => ln$master_id												 
										  ); 
					end if;
				else
					raise le$resultado_sorteo;
				end if;	
			else
				raise le$master_id_sorteo;
			end if;
		elsif pv_insert_pattern = CV$PRINT_ONLY then
			--!el master ID del sorteo actual existe
			if ln$master_id > 0 then
				--!proceso para imprimir los resultados
				imprime_resultados(pv_gl_type    => upper(pv_gl_type)
								 , pn_rownum	 => pn_rownum
								 , pn_drawing_id => ln$sorteo_actual
								 , pn_master_id  => ln$master_id												 
								  ); 
			else
				raise le$master_id_sorteo;
			end if;							  
		end if;
	end if;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when le$master_id_sorteo then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line('Master Id not found. Drawing Id: '||to_char(ln$sorteo_actual));  	
    raise;   
  when le$resultado_sorteo then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line('No information found for last drawing result. Drawing Id: '||to_char(ln$sorteo_actual));  
	rollback;	
    raise; 	  
  when le$last_pattern then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line('No records found into array ltbl$last_pattern');    
    raise; 	
  when le$hist_pattern then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line('No records found into array ltbl$hist_pattern');    
    raise; 
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_frec_lt_count_handler;


--!proceso general para realizar conteos de LT y FR de los ultimoas 100 jugadas en base a la ultima jugadas
procedure get_frec_lt_count_wrapper(pn_drawing_id				   NUMBER DEFAULT NULL
								  , pv_auto_commit				   VARCHAR2 DEFAULT 'N'	
								  , pv_insert_pattern			   VARCHAR2 DEFAULT 'Y'	
								  , x_err_code       IN OUT NOCOPY NUMBER	
									) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_frec_lt_count_wrapper';
	lv$gl_type      varchar2(2); 
begin
	/*for p in 1..2 loop
		if p = 1 then lv$gl_type := 'FR'; else lv$gl_type := 'LT'; end if;
		for t in 1..2 loop
			olap_sys.w_pick_panorama_pkg.get_frec_lt_count_handler (pv_gl_type        => lv$gl_type
																  , pn_rownum	      => t
																  , pn_drawing_id     => pn_drawing_id
																  , pv_auto_commit    => 'N'
																  , pv_insert_pattern => pv_insert_pattern
																  , x_err_code        => x_err_code);
		end loop;                                                              
	end loop;*/  

	olap_sys.w_pick_panorama_pkg.get_frec_lt_count_handler (pv_gl_type        => 'LT'
														  , pn_rownum	      => 1
														  , pn_drawing_id     => pn_drawing_id
														  , pv_auto_commit    => 'N'
														  , pv_insert_pattern => pv_insert_pattern
														  , x_err_code        => x_err_code);
	
	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION and pv_insert_pattern = CV$ENABLE then
		--!proceso para generar conteos de lt types para los dos ultimos sorteos
		olap_sys.w_pick_panorama_pkg.generar_lt_counts_handler (pn_drawing_id => pn_drawing_id
															  , x_err_code    => x_err_code);  
	end if;    
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_frec_lt_count_wrapper;


--!proceso recuperas las ultimas n jugadas de los sorteos
procedure get_last_pattern(pv_gl_type        			      VARCHAR2
						 , pn_rownum					      NUMBER
						 , pn_drawing_id				      NUMBER	
						 , xn_drawing_id_search IN OUT NOCOPY NUMBER
						 , xtbl_color_tbl       IN OUT NOCOPY gt$color_tbl
						 , x_err_code           IN OUT NOCOPY NUMBER
						  ) is
	LV$PROCEDURE_NAME       CONSTANT VARCHAR2(30) := 'get_last_pattern';	
	ltbl$gl_pattern_tbl				 gt$history_cnt_tbl;
	ln$drawing_id_ini				 number := 0;
	ln$drawing_id_end				 number := 0;
	
	cursor c_last_pattern (pv_gl_type        			   VARCHAR2
						 , pn_rownum					   NUMBER
						 , pn_drawing_id_ini			   NUMBER
						 , pn_drawing_id_end			   NUMBER						 
						  ) is
	with last_pattern_tbl as (
	select gambling_id, cu1, cu2, cu3, cu4, cu5, cu6, clt1, clt2, clt3, clt4, clt5, clt6
	  from olap_sys.mr_resultados_summary
	 where gambling_id  between pn_drawing_id_ini and pn_drawing_id_end 
	 order by gambling_id desc 
	) select gambling_id
		   , decode(decode(pv_gl_type,'LT',clt1,cu1),1,'R',2,'G',3,'B','X') prn_gl1
		   , decode(decode(pv_gl_type,'LT',clt2,cu2),1,'R',2,'G',3,'B','X') prn_gl2
		   , decode(decode(pv_gl_type,'LT',clt3,cu3),1,'R',2,'G',3,'B','X') prn_gl3
		   , decode(decode(pv_gl_type,'LT',clt4,cu4),1,'R',2,'G',3,'B','X') prn_gl4
		   , decode(decode(pv_gl_type,'LT',clt5,cu5),1,'R',2,'G',3,'B','X') prn_gl5
		   , decode(decode(pv_gl_type,'LT',clt6,cu6),1,'R',2,'G',3,'B','X') prn_gl6		   
		   , decode(pv_gl_type,'LT',clt1,cu1) gl1
		   , decode(pv_gl_type,'LT',clt2,cu2) gl2
		   , decode(pv_gl_type,'LT',clt3,cu3) gl3
		   , decode(pv_gl_type,'LT',clt4,cu4) gl4
		   , decode(pv_gl_type,'LT',clt5,cu5) gl5
		   , decode(pv_gl_type,'LT',clt6,cu6) gl6		   
		from last_pattern_tbl
	   where rownum <= pn_rownum
	   order by gambling_id desc;						  
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);	
	end if;	

	ln$drawing_id_ini := (pn_drawing_id - pn_rownum) + 1;
	ln$drawing_id_end := pn_drawing_id;
	
--	dbms_output.put_line('2.xn_drawing_id_ini: '||xn_drawing_id_ini);
--	dbms_output.put_line('2.xn_drawing_id_end: '||xn_drawing_id_end);

	
	olap_sys.w_common_pkg.g_index := 1;
	for i in c_last_pattern (pv_gl_type        => pv_gl_type
						   , pn_rownum	       => pn_rownum
						   , pn_drawing_id_ini => ln$drawing_id_ini
						   , pn_drawing_id_end => ln$drawing_id_end
						    ) loop
			dbms_output.put_line(i.gambling_id||'|'||i.prn_gl1||'|'||i.prn_gl2||'|'||i.prn_gl3||'|'||i.prn_gl4||'|'||i.prn_gl5||'|'||i.prn_gl6);		
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).drawing_id := i.gambling_id;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos1 	  := i.gl1;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos2 	  := i.gl2;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos3 	  := i.gl3;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos4 	  := i.gl4;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos5 	  := i.gl5;
			ltbl$gl_pattern_tbl(olap_sys.w_common_pkg.g_index).pos6 	  := i.gl6;	
			xn_drawing_id_search	              					      := i.gambling_id;		
		olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
	end loop;

	--!estableciendo la jugada base para la busqueda
	xn_drawing_id_search := xn_drawing_id_search - 1;
--	dbms_output.put_line('xn_drawing_id_search: '||xn_drawing_id_search);
	
	--!almacenar los contadores en un arreglo
	if ltbl$gl_pattern_tbl.count > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
		for k in ltbl$gl_pattern_tbl.first..ltbl$gl_pattern_tbl.last loop
			if k = 1 then
				xtbl_color_tbl(1).color_ultimo := ltbl$gl_pattern_tbl(k).pos1;
				xtbl_color_tbl(2).color_ultimo := ltbl$gl_pattern_tbl(k).pos2;
				xtbl_color_tbl(3).color_ultimo := ltbl$gl_pattern_tbl(k).pos3;
				xtbl_color_tbl(4).color_ultimo := ltbl$gl_pattern_tbl(k).pos4;
				xtbl_color_tbl(5).color_ultimo := ltbl$gl_pattern_tbl(k).pos5;
				xtbl_color_tbl(6).color_ultimo := ltbl$gl_pattern_tbl(k).pos6;
			elsif k = 2 then
				xtbl_color_tbl(1).color_previo := ltbl$gl_pattern_tbl(k).pos1;
				xtbl_color_tbl(2).color_previo := ltbl$gl_pattern_tbl(k).pos2;
				xtbl_color_tbl(3).color_previo := ltbl$gl_pattern_tbl(k).pos3;
				xtbl_color_tbl(4).color_previo := ltbl$gl_pattern_tbl(k).pos4;
				xtbl_color_tbl(5).color_previo := ltbl$gl_pattern_tbl(k).pos5;
				xtbl_color_tbl(6).color_previo := ltbl$gl_pattern_tbl(k).pos6;
			end if;
		end loop;
		
--		for p in xtbl_color_tbl.first..xtbl_color_tbl.last loop
--			dbms_output.put_line(xtbl_color_tbl(p).color_ultimo||'  '||xtbl_color_tbl(p).color_previo);
--		end loop;
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	else
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	end if;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end get_last_pattern;


--!funcion para recuperar el master_id de un patron de la tabla s_gl_mapas
procedure load_history_mr_resultados (pv_gl_type					     VARCHAR2
								    , pn_drawing_id				         NUMBER
									, pv_full_scan						 VARCHAR2
									, xn_drawing_id_ini	   IN OUT NOCOPY NUMBER
								    , xn_drawing_id_end	   IN OUT NOCOPY NUMBER
								    , xtbl_history_cnt_tbl IN OUT NOCOPY gt$history_cnt_tbl
								    , x_err_code           IN OUT NOCOPY NUMBER
								     ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'load_history_mr_resultados';	
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
		dbms_output.put_line('pn_drawing_id: '||pn_drawing_id);
	end if;	

	--!calculado el rango de fecha de consulta
	if pv_full_scan = CV$ENABLE then
		xn_drawing_id_ini := CN$BASE_DRAWING_ID;
		xn_drawing_id_end := pn_drawing_id;
	else
		xn_drawing_id_ini := pn_drawing_id  - CN$DIFERENCIA;
		xn_drawing_id_end := pn_drawing_id;
	end if;
		
	select gambling_id
		 , decode(pv_gl_type,'FR',cu1,clt1) pos1
		 , decode(pv_gl_type,'FR',cu2,clt2) pos2
		 , decode(pv_gl_type,'FR',cu3,clt3) pos3
		 , decode(pv_gl_type,'FR',cu4,clt4) pos4
		 , decode(pv_gl_type,'FR',cu5,clt5) pos5
		 , decode(pv_gl_type,'FR',cu6,clt6) pos6
	  bulk collect into xtbl_history_cnt_tbl
 	  from olap_sys.mr_resultados_summary
	 where gambling_id between xn_drawing_id_ini and xn_drawing_id_end
	 order by gambling_id desc; 

--	dbms_output.put_line('xtbl_history_cnt_tbl.count: '||xtbl_history_cnt_tbl.count);	
	if xtbl_history_cnt_tbl.count = 0 then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	else	
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
	end if;	
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end load_history_mr_resultados;


--!proceso para actualizar los templates con la info de las coincidencias
procedure upd_search_templates(pv_gl_type					  VARCHAR2
							 , pv_b_type			   		  VARCHAR2
							 , pn_rownum					  NUMBER
							 , pn_color_ultimo			   	  NUMBER
							 , pn_color_previo			   	  NUMBER DEFAULT NULL
							 , pn_color_siguiente			  NUMBER
							 , pn_drawing_id_siguiente		  NUMBER 
							 , pn_drawing_id_ini   	   		  NUMBER
							 , pn_drawing_id_end   	   		  NUMBER
							 , pn_sorteos_espera_cnt		  NUMBER
							 , x_err_code       IN OUT NOCOPY NUMBER
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'upd_search_templates';
	ln$template_id					 number := 0;	
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pv_gl_type: '||pv_gl_type);
		dbms_output.put_line('pv_b_type: '||pv_b_type);
		dbms_output.put_line('pn_rownum: '||pn_rownum);		
		dbms_output.put_line('pn_color_ultimo: '||pn_color_ultimo);
		dbms_output.put_line('pn_color_previo: '||pn_color_previo);
		dbms_output.put_line('pn_color_siguiente: '||pn_color_siguiente);		
		dbms_output.put_line('pn_drawing_id_siguiente: '||pn_drawing_id_siguiente);
		dbms_output.put_line('pn_drawing_id_ini: '||pn_drawing_id_ini);
		dbms_output.put_line('pn_drawing_id_end: '||pn_drawing_id_end);
	end if;
	


	if pn_rownum = 1 then
		--!actualizando la lista de resultados que hacen match
		update olap_sys.s_gl_search_templates
		   set drawing_list = drawing_list||pn_drawing_id_siguiente||'~'
			 , select_flag  = 'Y'
		 where gl_type   = pv_gl_type
		   and xrownum   = pn_rownum
		   and b_type    = pv_b_type
		   and gl_color1 = pn_color_ultimo 
		   returning template_id into ln$template_id;	

	elsif pn_rownum = 2 then
		--!actualizando la lista de resultados que hacen match
		update olap_sys.s_gl_search_templates
		   set drawing_list = drawing_list||pn_drawing_id_siguiente||'~'
			 , select_flag  = 'Y'
		 where gl_type   = pv_gl_type
		   and xrownum   = pn_rownum
		   and b_type    = pv_b_type
		   and gl_color1 = pn_color_ultimo
		   and gl_color2 = pn_color_previo			   
		   returning template_id into ln$template_id;
	end if;

--dbms_output.put_line(sql%rowcount||' rows updated  , ln$template_id: '||ln$template_id);

	if ln$template_id != 0 then
		update olap_sys.s_gl_search_template_cnt
		   set gl_cnt         = gl_cnt+ 1
			 , drawing_id_ini = pn_drawing_id_ini
			 , drawing_id_end = pn_drawing_id_end
			 , drawing_list   = drawing_list||pn_drawing_id_siguiente||'~'
			 --, cycles_list    = cycles_list||pn_sorteos_espera_cnt||'~'
		 where template_id = ln$template_id
           and b_type      = pv_b_type
		   and gl_color1   = pn_color_siguiente;
--dbms_output.put_line(sql%rowcount||' rows updated');	
	end if;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end upd_search_templates;


--!proceso para limpiar las columnas de las tablas
procedure clean_table_columns is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'clean_table_columns';
begin
	update olap_sys.s_gl_search_templates
	   set drawing_list = null
		 , select_flag  = null
		 , gl_cnt       = 0
	 where select_flag is not null;	

	update olap_sys.s_gl_search_template_cnt
	   set gl_cnt         = 0
		 , drawing_id_ini = null
		 , drawing_id_end = null
		 , drawing_list   = null
		 , drawing_waiting_avg = null
		 , last_drawing_cnt = null 
	 where gl_cnt > 0;

end clean_table_columns;

									  
--!procedimiento para encontrar patrones en los mapas y actualizar datos en la tabla
procedure search_pattern_data (pv_gl_type					  VARCHAR2
							 , pn_rownum					  NUMBER
							 , pn_drawing_id_ini   			  NUMBER
							 , pn_drawing_id_end   			  NUMBER
							 , ptbl_color_tbl  		  		  gt$color_tbl
							 , ptbl_history_cnt_tbl           gt$history_cnt_tbl
							 , x_err_code       IN OUT NOCOPY NUMBER	
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'search_pattern_data';	
	ln$index_siguiente				 number := 1;
	ln$index_ultimo					 number := 1;
	ln$index_previo					 number := 1;
	ln$b_type_cnt					 number := 1;
	ln$sorteos_espera_cnt			 number := 1;
	ln$coincidencias_cnt			 number := 0;
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	
	
	--!proceso para limpiar las columnas de las tablas
	clean_table_columns;
	 
	olap_sys.w_common_pkg.g_rowcnt := 1;
	for p in ptbl_color_tbl.first..ptbl_color_tbl.last loop		
--		dbms_output.put_line('B'||ln$b_type_cnt||'   color_ultimo: '||ptbl_color_tbl(p).color_ultimo||' color_previo: '||ptbl_color_tbl(p).color_previo);
		ln$index_siguiente    := 1;
		ln$index_ultimo       := 1;
		ln$index_previo    	  := 1;
		ln$sorteos_espera_cnt := 1;
		ln$coincidencias_cnt  := 0;		
		for m in ptbl_history_cnt_tbl.first..ptbl_history_cnt_tbl.last loop			
--			dbms_output.put_line('ln$index_siguiente: '||ln$index_siguiente||'  ln$index_ultimo: '||ln$index_ultimo||'  ln$index_previo: '||ln$index_previo||' # '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||'  '||ptbl_history_cnt_tbl(ln$index_ultimo).drawing_id||'  '||ptbl_history_cnt_tbl(ln$index_previo).drawing_id);
			if ln$index_ultimo > 1 then
				if pn_rownum = 1 then
					--!B1
					if ln$b_type_cnt = 1 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos1 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos1
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos1
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B1 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;
					
					--!B2
					if ln$b_type_cnt = 2 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos2 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos2
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos2
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B2 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if; 
					end if;
					
					--!B3
					if ln$b_type_cnt = 3 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos3 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos3
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos3
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B3 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;				

					--!B4
					if ln$b_type_cnt = 4 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos4 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos4
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos4
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B4 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;

					--!B5
					if ln$b_type_cnt = 5 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos5 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos5
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos5
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B5 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;

					--!B6
					if ln$b_type_cnt = 6 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos6 = ptbl_color_tbl(p).color_ultimo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos6
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos6
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 						
--							dbms_output.put_line('B6 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 0;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;		
				elsif pn_rownum = 2 then
					--!B1
					if ln$b_type_cnt = 1 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos1 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos1 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos1
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos1
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos1
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B1 rownum = 2 B1 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;
					
					--!B2
					if ln$b_type_cnt = 2 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos2 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos2 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos2
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos2
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos2
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 							
--							dbms_output.put_line('B2 rownum = 2 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;
					
					--!B3
					if ln$b_type_cnt = 3 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos3 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos3 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos3
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos3
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos3
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B3 rownum = 2 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;				

					--!B4
					if ln$b_type_cnt = 4 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos4 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos4 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos4
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos4
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos4
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 							
--							dbms_output.put_line('B4 rownum = 2 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;

					--!B5
					if ln$b_type_cnt = 5 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos5 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos5 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos5
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos5
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos5
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 							
--							dbms_output.put_line('B5 rownum = 2 match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;

					--!B6
					if ln$b_type_cnt = 6 then
						if ptbl_history_cnt_tbl(ln$index_ultimo).pos6 = ptbl_color_tbl(p).color_ultimo and 
						   ptbl_history_cnt_tbl(ln$index_previo).pos6 = ptbl_color_tbl(p).color_previo then
							--!proceso para actualizar los templates con la info de las coincidencias
							upd_search_templates(pv_gl_type				 => pv_gl_type
											   , pv_b_type			   	 => 'B'||ln$b_type_cnt
											   , pn_rownum				 => pn_rownum
											   , pn_color_ultimo		 => ptbl_history_cnt_tbl(ln$index_ultimo).pos6
											   , pn_color_previo		 => ptbl_history_cnt_tbl(ln$index_previo).pos6
											   , pn_color_siguiente		 => ptbl_history_cnt_tbl(ln$index_siguiente).pos6
											   , pn_drawing_id_siguiente => ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id
											   , pn_drawing_id_ini   	 => pn_drawing_id_ini
											   , pn_drawing_id_end   	 => pn_drawing_id_end
											   , pn_sorteos_espera_cnt   => ln$sorteos_espera_cnt
											   , x_err_code              => x_err_code); 
--							dbms_output.put_line('B6 rownum = 2  match gambling_id: '||ptbl_history_cnt_tbl(ln$index_siguiente).drawing_id||' espera: '||ln$sorteos_espera_cnt);
							ln$sorteos_espera_cnt := 1;
							ln$coincidencias_cnt := ln$coincidencias_cnt + 1;
						end if;
					end if;										
				end if;
			end if; 
			exit when ln$index_previo >= ptbl_history_cnt_tbl.last;
			
			ln$index_ultimo := ln$index_ultimo + 1;
			ln$index_previo := ln$index_ultimo + 1;
			ln$index_siguiente := ln$index_ultimo - 1;
			ln$sorteos_espera_cnt := ln$sorteos_espera_cnt + 1;
		end loop;	
--		dbms_output.put_line('B'||ln$b_type_cnt||'  coincidencias: '||ln$coincidencias_cnt);
--		dbms_output.put_line('---------------');
		ln$b_type_cnt :=  ln$b_type_cnt + 1;
	end loop;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end search_pattern_data;							  


--!procedimiento para calcular el total de sorteos por patron
procedure compute_drawing_cnts(pv_gl_type					  VARCHAR2
							 , pn_rownum					  NUMBER
							 , x_err_code       IN OUT NOCOPY NUMBER
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'compute_drawing_cnts';
	ln$gl_cnt					 	 number := 0;
	cursor c_template (pv_gl_type	VARCHAR2
				     , pn_rownum	NUMBER) is	
    select template_id
		 , drawing_list 
	  from olap_sys.s_gl_search_templates 
	 where gl_type = pv_gl_type
       and xrownum = pn_rownum
	   and drawing_list is not null; 
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

	--!calculando el total de sorteos por patron
	for p in c_template (pv_gl_type	=> pv_gl_type
				       , pn_rownum  => pn_rownum) loop
		with list_tbl as (   
		select to_number(regexp_substr(p.drawing_list,'[^,~]+',1,level)) drawing_ids
						   from dual 
						 connect by level <= length(p.drawing_list)-length(replace(p.drawing_list,'~',''))+1
		) select count(drawing_ids) gl_cnt
			into ln$gl_cnt
		  from list_tbl;
		  
		update olap_sys.s_gl_search_templates  
		   set gl_cnt = ln$gl_cnt
		 where template_id = p.template_id; 
		 
		update olap_sys.s_gl_search_template_cnt
		   set last_drawing_cnt = drawing_id_end - to_number(substr(drawing_list, 1, instr(drawing_list,'~',1,1)-1))
		 where template_id = p.template_id;   
	end loop;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end compute_drawing_cnts;	
	

--!proceso para calcular el promedio de sorteos a esperar para que pase una ocurrencia
procedure compute_drawing_avg (pv_gl_type					  VARCHAR2
							 , pn_rownum					  NUMBER
							 , x_err_code       IN OUT NOCOPY NUMBER
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'compute_drawing_avg';
    type gt$diferencia_rec is table of number index by binary_integer;    
    ltbl$load_tbl               gt$diferencia_rec;
    ln$diferencia               number := 0;
    ln$diferencia_accum         number := 0;
    lv$diferencia_list          varchar2(4000);
    ln$index                    number := 1;
    cursor c_template is 
    select tc.template_id
         , tc.id
         , tc.seq_no
         , tc.b_type
         , tc.drawing_list
     from olap_sys.s_gl_search_templates  t
        , olap_sys.s_gl_search_template_cnt tc
    where t.template_id = tc.template_id
      and t.select_flag is not null;
   
    cursor c_list (pv_drawing_list   varchar2) is
    select regexp_substr(pv_drawing_list,'[^,~]+',1,level) xlist
              from dual 
           connect by level <= length(pv_drawing_list)-length(replace(pv_drawing_list,'~',''))+1;
begin
 	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

   for p in c_template loop
        ltbl$load_tbl.delete;
        ln$index := 1;
        ln$diferencia := 0;
        ln$diferencia_accum := 0;
		lv$diferencia_list := NULL;
--        dbms_output.put_line(p.template_id||' '||p.id||' '||p.seq_no||' '||p.b_type);
        for t in c_list (pv_drawing_list => p.drawing_list) loop
            if t.xlist is not null then
                ltbl$load_tbl(ln$index) := t.xlist;
                ln$index := ln$index + 1;
            end if;
        end loop;
--        dbms_output.put_line('rows: '||ltbl$load_tbl.count);
        if ltbl$load_tbl.count > 0 then
			ln$index := 1;
			for x in ltbl$load_tbl.first..ltbl$load_tbl.last loop
				ln$index := x;
	--            dbms_output.put_line(ltbl$load_tbl(ln$index));
				if x > 1 then
					ln$diferencia := ltbl$load_tbl(ln$index-1) - ltbl$load_tbl(ln$index);
					ln$diferencia_accum := ln$diferencia_accum + ln$diferencia;
					lv$diferencia_list := lv$diferencia_list ||ln$diferencia||'~';
					--dbms_output.put_line(x||' prev: '||ltbl$load_tbl(ln$index-1)||' cur: '||ltbl$load_tbl(ln$index)||' dif: '||ln$diferencia||' accum: '||ln$diferencia_accum);
		--            dbms_output.put_line(ln$diferencia);
				end if;
				ln$index := ln$index + 1;
			end loop; 
			lv$diferencia_list := substr(lv$diferencia_list,1,length(lv$diferencia_list)-1);
--			dbms_output.put_line('len: '||length(lv$diferencia_list));
--			dbms_output.put_line('cnt: '||ltbl$load_tbl.count);
	--		ln$diferencia := ln$diferencia_accum/(ltbl$load_tbl.count-1);
	--		dbms_output.put_line('dif avg: '||ln$diferencia);
	--		ins_tmp_testing (pv_valor => lv$diferencia_list);
			--!calculando el promedio de los sorteos a esperar para que pase una ocurrencia
			--!en base a la 
			with avg_tbl as (
			select to_number(regexp_substr(lv$diferencia_list,'[^,~]+',1,level)) nlist
					  from dual 
				   connect by level <= length(lv$diferencia_list)-length(replace(lv$diferencia_list,'~',''))+1
			), low_tbl as (
				select round(avg(nlist) - stddev(nlist)) nlow
				from avg_tbl
			), high_tbl as (
				select round(avg(nlist) + stddev(nlist)) nhigh
				from avg_tbl
			) select round(avg(nlist)) nlist
				into ln$diferencia
				from avg_tbl
			   where nlist between (select nlow from low_tbl) and (select nhigh from high_tbl)
			;
--			 dbms_output.put_line('dif std avg: '||ln$diferencia); 
			 
			update olap_sys.s_gl_search_template_cnt
			   set drawing_waiting_avg = round(ln$diferencia)
			 where template_id = p.template_id
			   and id 		   = p.id
			   and seq_no 	   = p.seq_no
			   and b_type 	   = p.b_type;
		end if;	
    end loop;

	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end compute_drawing_avg;	


--!proceso para imprimir los resultados
procedure print_output(pv_gl_type					  VARCHAR2
				     , pn_rownum					  NUMBER
					  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'print_output';
	cursor c_template (pv_gl_type	VARCHAR2
				     , pn_rownum	NUMBER) is
	with output_tbl as (
	select gl_type
		 , xrownum
		 , b_type
		 , decode(gl_color1,1,'R',2,'G',3,'B','X') gl_color1
		 , decode(gl_color2,1,'R',2,'G',3,'B','X') gl_color2
		 , gl_cnt
		 , substr(drawing_list,1,50) drawing_list
		 , template_id 		 
	  from olap_sys.s_gl_search_templates 
	 where gl_type = pv_gl_type
	   and xrownum = pn_rownum
	   and drawing_list is not null
	union
	select gl_type
		 , xrownum
		 , b_type
		 , null
		 , null
		 , 0
		 , null
		 , null
	  from olap_sys.s_gl_search_templates 
	 where gl_type = pv_gl_type
	   and xrownum = pn_rownum
	   and gl_color1 = 0
	 order by b_type
	)select gl_type
		  , xrownum
		  , b_type
		  , gl_color1
		  , gl_color2
		  , gl_cnt
		  , drawing_list
		  , nvl(template_id,0) template_id
	   from (
	select gl_type
		  , xrownum
		  , b_type
		  , nvl(gl_color1,'#') gl_color1
		  , nvl(gl_color2,'#') gl_color2
		  , gl_cnt
		  , nvl(drawing_list,'#') drawing_list
		  , template_id 	
		  , rank() over (partition by b_type order by gl_cnt desc) gl_rank 
		from output_tbl ot
	) where gl_rank = 1
	 order by b_type;

	cursor c_template_cnt (pn_template_id    		NUMBER
						 , pv_gl_type				VARCHAR2
						 , pv_b_type				VARCHAR2
	) is
	with output_tbl as (
		select b_type
			 , decode(gl_color1,1,'R',2,'G',3,'B','X') gl_color1
			 , gl_cnt
			 , drawing_id_ini
			 , drawing_id_end
			 , last_drawing_cnt
			 , substr(drawing_list,1,50) drawing_list
			 , template_id
			 , drawing_waiting_avg
		  from olap_sys.s_gl_search_template_cnt 
		 where template_id = pn_template_id
	   union
		select b_type
			 , decode(gl_color1,1,'R',2,'G',3,'B','X') gl_color1
			 , gl_cnt
			 , drawing_id_ini
			 , drawing_id_end
			 , last_drawing_cnt
			 , substr(drawing_list,1,50) drawing_list
			 , template_id
			 , drawing_waiting_avg
		  from olap_sys.s_gl_output_templates 
		 where template_id = pn_template_id
		   and b_type = pv_b_type
		   and gl_type = pv_gl_type
	) select b_type
		   , gl_color1
		   , gl_cnt
		   , drawing_id_ini
		   , drawing_id_end
		   , last_drawing_cnt
		   , drawing_list
		   , template_id
		   , drawing_waiting_avg
		   , sort_by
	   from (   
	  select b_type
		   , gl_color1
		   , gl_cnt
		   , drawing_id_ini
		   , drawing_id_end
		   , last_drawing_cnt
		   , drawing_list
		   , template_id
		   , drawing_waiting_avg
		   , rank() over (partition by b_type order by template_id desc) template_rank
           , decode(gl_color1,'R',1,'G',2,'B',3,'X',4) sort_by		   
		from output_tbl
	) where template_rank = 1
	  order by sort_by;  
begin
 	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

	for t in c_template (pv_gl_type	=> pv_gl_type
				       , pn_rownum	=> pn_rownum) loop
		dbms_output.put_line(t.gl_type||'|'||t.xrownum||'|'||t.b_type||'|'||t.gl_color1||'|'||t.gl_color2||'|'||t.gl_cnt||'|||drawings: '||t.drawing_list);				
		for tc in c_template_cnt (pn_template_id => t.template_id
								, pv_gl_type	 => t.gl_type
								, pv_b_type      => t.b_type
						 
		) loop
			if tc.drawing_list is not null then
				dbms_output.put_line('     |'||tc.b_type||'|'||tc.gl_color1||'|'||tc.gl_cnt||'|'||tc.drawing_id_ini||'|'||tc.drawing_id_end||'|'||tc.last_drawing_cnt||'|'||tc.drawing_waiting_avg||'|drawings: '||tc.drawing_list);	
			else
				dbms_output.put_line('     |'||tc.b_type||'|'||tc.gl_color1||'|'||tc.gl_cnt||'|'||tc.drawing_id_ini||'|'||tc.drawing_id_end||'|'||tc.last_drawing_cnt||'|'||tc.drawing_waiting_avg);			
			end if;				   
		end loop;
	end loop;				   
end print_output;

	
--!proceso para buscar patrones en el historico de mapas de conteo
procedure search_pattern_handler(pv_drawing_type			    VARCHAR2 DEFAULT 'mrtr'
							   , pn_drawing_id				    NUMBER DEFAULT 0
							   , pv_gl_type					    VARCHAR2 DEFAULT 'LT'
							   , pn_rownum						NUMBER DEFAULT 2
							   , pv_full_scan			        VARCHAR2 DEFAULT 'N'
							   , x_err_code       IN OUT NOCOPY NUMBER	
								) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'search_pattern_handler';								
	ln$drawing_id			NUMBER := 0;
	ln$drawing_id_search	NUMBER := 0;
	ln$drawing_id_ini		NUMBER := 0;
	ln$drawing_id_end		NUMBER := 0;
	ltbl$color_tbl		    gt$color_tbl;
	ltbl$history_cnt_tbl	gt$history_cnt_tbl;
begin
	if GB$SHOW_PROC_NAME then
		dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;	

	if pn_drawing_id = 0 then
		--!recuperar el ID ultimo sorteo
		ln$drawing_id := get_max_drawing_id (pv_drawing_type => pv_drawing_type);
	else
		ln$drawing_id := pn_drawing_id;
	end if;
	
	--!proceso recuperas las ultimas n jugadas de los sorteos
	get_last_pattern(pv_gl_type        	  => pv_gl_type
				   , pn_rownum			  => pn_rownum
				   , pn_drawing_id		  => ln$drawing_id
				   , xn_drawing_id_search => ln$drawing_id_search
				   , xtbl_color_tbl       => ltbl$color_tbl
				   , x_err_code           => x_err_code
				    );
--dbms_output.put_line('ln$drawing_id_search: '||ln$drawing_id_search);
--dbms_output.put_line('ltbl$color_tbl.count: '||ltbl$color_tbl.count);

	if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then	
		--!funcion para recuperar el master_id de un patron de la tabla s_gl_mapas
		load_history_mr_resultados (pv_gl_type			 => pv_gl_type
								  , pn_drawing_id		 => ln$drawing_id_search
								  , pv_full_scan         => pv_full_scan
								  , xn_drawing_id_ini	 => ln$drawing_id_ini
								  , xn_drawing_id_end	 => ln$drawing_id_end
								  , xtbl_history_cnt_tbl => ltbl$history_cnt_tbl
								  , x_err_code           => x_err_code
								   );
--dbms_output.put_line('ln$drawing_id_ini: '||ln$drawing_id_ini);	
--dbms_output.put_line('ln$drawing_id_end: '||ln$drawing_id_end);	
		if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then							  
			--!procedimiento para encontrar patrones en los mapas y actualizar datos en la tabla
			search_pattern_data (pv_gl_type		      => pv_gl_type
							   , pn_rownum		      => pn_rownum
							   , pn_drawing_id_ini    => ln$drawing_id_ini
							   , pn_drawing_id_end 	  => ln$drawing_id_end
							   , ptbl_color_tbl  	  => ltbl$color_tbl
							   , ptbl_history_cnt_tbl => ltbl$history_cnt_tbl
							   , x_err_code           => x_err_code);
			if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then			
				--!procedimiento para calcular el total de sorteos por patron
				compute_drawing_cnts(pv_gl_type	=> pv_gl_type
								   , pn_rownum	=> pn_rownum
								   , x_err_code => x_err_code);
				
				if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
					--!proceso para calcular el promedio de sorteos a esperar para que pase una ocurrencia
					compute_drawing_avg (pv_gl_type	=> pv_gl_type
									   , pn_rownum	=> pn_rownum
									   , x_err_code => x_err_code); 
					
					if x_err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
						commit;
						--!proceso para imprimir los resultados
						print_output(pv_gl_type => pv_gl_type
								   , pn_rownum  => pn_rownum); 	   
					end if;		   
				end if;			   
			end if;	
		end if;						
	end if;							
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end search_pattern_handler;


--!proceso para validar la repeticion de los patrones de la ley del tercio
procedure validate_lt_pattern_history  is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'search_pattern_handler';		

	cursor c_last_gamgling is
	with max_id_tbl as (
		select max(gambling_id) max_id from olap_sys.gl_lt_pattern_history
	)select id
		  , gambling_date
		  , pos1
		  , pos2
		  , pos3
		  , pos4
		  , pos5
		  , pos6
		  , gambling_id
		  from olap_sys.gl_lt_pattern_history
		 where gambling_id = (select max_id from max_id_tbl);	
	
	cursor c_previous_gambling (pv_gambling_date	olap_sys.gl_lt_pattern_history.gambling_date%type
							  , pv_pos1				olap_sys.gl_lt_pattern_history.pos1%type
							  , pv_pos2				olap_sys.gl_lt_pattern_history.pos2%type
							  , pv_pos3				olap_sys.gl_lt_pattern_history.pos3%type
							  , pv_pos4				olap_sys.gl_lt_pattern_history.pos4%type
							  , pv_pos5				olap_sys.gl_lt_pattern_history.pos5%type
							  , pv_pos6				olap_sys.gl_lt_pattern_history.pos6%type
							  , pn_gambling_id		olap_sys.gl_lt_pattern_history.gambling_id%type) is
	select id
	  from olap_sys.gl_lt_pattern_history
	 where gambling_date = pv_gambling_date
	   and pos1 = pv_pos1
	   and pos2 = pv_pos2
	   and pos3 = pv_pos3
	   and pos4 = pv_pos4
	   and pos5 = pv_pos5
	   and pos6 = pv_pos6
	   and gambling_id < pn_gambling_id
	 order by gambling_id desc;				  
begin

	for t in c_last_gamgling loop
		dbms_output.put_line('pn_id: '||t.id);
		dbms_output.put_line('pv_gambling_date: '||t.gambling_date);
		dbms_output.put_line('pv_pos1: '||t.pos1);
		dbms_output.put_line('pv_pos2: '||t.pos2);
		dbms_output.put_line('pv_pos3: '||t.pos3);
		dbms_output.put_line('pv_pos4: '||t.pos4);
		dbms_output.put_line('pv_pos5: '||t.pos5);
		dbms_output.put_line('pv_pos6: '||t.pos6);
		dbms_output.put_line('pn_gambling_id: '||t.gambling_id);

		for i in c_previous_gambling (pv_gambling_date => t.gambling_date
								    , pv_pos1 => t.pos1
								    , pv_pos2 => t.pos2
								    , pv_pos3 => t.pos3
								    , pv_pos4 => t.pos4
								    , pv_pos5 => t.pos5
								    , pv_pos6 => t.pos6
								    , pn_gambling_id => t.gambling_id) loop
	dbms_output.put_line('i.id: '||i.id);				   
			update olap_sys.gl_lt_pattern_history
			   set parent_id = i.id
			 where id = t.id; 
	dbms_output.put_line(sql%rowcount||' rows updated');		 
			exit;
		end loop;
	end loop;
		
	commit;
end validate_lt_pattern_history;									 


procedure ins_pm_contador_digitos (pn_year					  NUMBER
								 , pn_primos				  NUMBER
								 , pn_comb					  NUMBER
								 , pn_percentile			  NUMBER
								 , x_err_code   IN OUT NOCOPY NUMBER
								 ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'inicializa_arreglos';	

begin
	insert into olap_sys.pm_contador_digitos (ryear, rprimos, rcomb, percentile, play_flag)
	values(pn_year, pn_primos, pn_comb, pn_percentile, CV$DISABLE);
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 			
end ins_pm_contador_digitos;


--!inicializa numeros inpares, pares y primos
procedure inicializa_arreglos (pn_year_end					  NUMBER
						     , pn_years_back				  NUMBER
						     , pn_primo_cnt					  NUMBER
						     , pn_is_primo     				  NUMBER
							 , pn_percentile				  NUMBER
							 , xtbl_inpar_par   IN OUT NOCOPY DBMS_SQL.NUMBER_TABLE
							 , xtbl_primos		IN OUT NOCOPY DBMS_SQL.NUMBER_TABLE
							 , x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'inicializa_arreglos';	
	ln$err_code_cnt		NUMBER := 0;
	ln$year_ini	number := pn_year_end - pn_years_back;
begin
	--!numeros inpares y pares
	if pn_is_primo = 0 then
		xtbl_inpar_par(1) := 4;
		xtbl_inpar_par(2) := 6;
		xtbl_inpar_par(3) := 8;
		xtbl_inpar_par(4) := 9;
		xtbl_inpar_par(5) := 10;
		xtbl_inpar_par(6) := 12;
		xtbl_inpar_par(7) := 14;
		xtbl_inpar_par(8) := 15;
		xtbl_inpar_par(9) := 16;
		xtbl_inpar_par(10) := 18;
		xtbl_inpar_par(11) := 20;
		xtbl_inpar_par(12) := 21;
		xtbl_inpar_par(13) := 22;
		xtbl_inpar_par(14) := 24;
		xtbl_inpar_par(15) := 25;
		xtbl_inpar_par(16) := 26;
		xtbl_inpar_par(17) := 27;
		xtbl_inpar_par(18) := 28;
		xtbl_inpar_par(19) := 30;
		xtbl_inpar_par(20) := 32;
		xtbl_inpar_par(21) := 33;
		xtbl_inpar_par(22) := 34;
		xtbl_inpar_par(23) := 35;
		xtbl_inpar_par(24) := 36;
		xtbl_inpar_par(25) := 38;
		xtbl_inpar_par(26) := 39;
	end if;
	
	--!numeros primos
	if pn_is_primo = 1 then
		xtbl_primos(1) := 1;
		xtbl_primos(2) := 2;
		xtbl_primos(3) := 3;
		xtbl_primos(4) := 5;
		xtbl_primos(5) := 7;
		xtbl_primos(6) := 11;
		xtbl_primos(7) := 13;
		xtbl_primos(8) := 17;
		xtbl_primos(9) := 19;
		xtbl_primos(10) := 23;
		xtbl_primos(11) := 29;
		xtbl_primos(12) := 31;
		xtbl_primos(13) := 37;		
	end if;
	
	for k in ln$year_ini..pn_year_end loop
		--!numeros inpares y pares
		if pn_is_primo = 0 then 
			for t in xtbl_inpar_par.first..xtbl_inpar_par.last loop				
				ins_pm_contador_digitos (pn_year       => k
									   , pn_primos     => pn_primo_cnt 
									   , pn_comb       => xtbl_inpar_par(t)
									   , pn_percentile => pn_percentile
									   , x_err_code    => x_err_code);
				ln$err_code_cnt	:= ln$err_code_cnt + x_err_code;					   
			end loop;				
		end if;

		--!numeros primos
		if pn_is_primo = 1 then 
			for t in xtbl_primos.first..xtbl_primos.last loop
				ins_pm_contador_digitos (pn_year       => k
									   , pn_primos     => pn_primo_cnt 
									   , pn_comb       => xtbl_primos(t)
									   , pn_percentile => pn_percentile
									   , x_err_code    => x_err_code);	
				ln$err_code_cnt	:= ln$err_code_cnt + x_err_code;							
			end loop;				
		end if;			
	end loop;
	if ln$err_code_cnt > 0 then
		x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	else	
		x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	end if;	
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 		
end inicializa_arreglos;
						  

procedure actualiza_tabla_contador (pn_is_primo					   NUMBER
								  , x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'actualiza_tabla_contador';
	cursor c_main is
	select distinct ryear
         , rprimos
      from olap_sys.pm_contador_digitos;
  
	cursor c_counts (pn_ryear		number
			       , pn_rcnt			number
			       , pn_is_primo	number) is
	--recupera el conteo de los numeros inpares y pares en base a 2 primos y al anio
	with par_inpar_tbl as (
	select year
		 , pn_cnt primos
		 , comb1 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb1) = nvl(pn_is_primo,0)
	union all
	select year
		 , pn_cnt primos
		 , comb2 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb2) = nvl(pn_is_primo,0) 
	union all
	select year
		 , pn_cnt primos
		 , comb3 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb3) = nvl(pn_is_primo,0)    
	union all
	select year
		 , pn_cnt primos
		 , comb4 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb4) = nvl(pn_is_primo,0)
	union all
	select year
		 , pn_cnt primos
		 , comb5 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb5) = nvl(pn_is_primo,0)
	union all
	select year
		 , pn_cnt primos
		 , comb6 comb
	  from olap_sys.pm_mr_resultados_v2
	 where year = pn_ryear
	   and pn_cnt = pn_rcnt
	   and olap_sys.w_common_pkg.is_prime_number(comb6) = nvl(pn_is_primo,0)
	), par_inpar_cnt_tbl as (
	  select year
		   , primos
		   , comb
		   , count(1) rcnt
		from par_inpar_tbl
	   group by year
		   , comb
		   , primos
	)select pi.year, pi.primos, pi.comb, pi.rcnt, (select max(mr.GAMBLING_ID) from olap_sys.pm_mr_resultados_v2 mr where mr.year = pi.year and mr.pn_cnt = pi.primos and pi.comb in (mr.comb1,mr.comb2,mr.comb3,mr.comb4,mr.comb5,mr.comb6)) maxrid
		from par_inpar_cnt_tbl pi
	   order by pi.rcnt desc
		   , pi.comb;	
begin
	for m in c_main loop
--dbms_output.put_line('c_main   year: '||m.ryear||' - primos: '||m.rprimos||' - is_primo: '||pn_is_primo);	
		for p in c_counts (pn_ryear    => m.ryear
					     , pn_rcnt     => m.rprimos
					     , pn_is_primo => pn_is_primo) loop			
--dbms_output.put_line('c_counts   year: '||p.year||' - primos: '||p.primos||' - is_primo: '||pn_is_primo||' - comb: '||p.comb||' - cnt: '||p.rcnt);
			update olap_sys.pm_contador_digitos
			   set rcnt = p.rcnt
				 , rdecena = case when p.comb between 1  and 9  then '1-9'
								  when p.comb between 10 and 19 then '10-19'
								  when p.comb between 20 and 29 then '20-29'
								  when p.comb between 30 and 39 then '30-39' end
				 , rlast_drawing_id = p.maxrid				  
			 where ryear   = m.ryear
			   and rprimos = m.rprimos
			   and rcomb   = p.comb;	
--			dbms_output.put_line(sql%rowcount||' registros actualizados');   
		end loop;
	end loop;
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end actualiza_tabla_contador;


procedure actualiza_estadisticas(pn_primo_cnt		number
							  , pn_drawing_id       number
							  , pn_comb    number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'actualiza_estadisticas';
	ln$min_cnt				number := 0;
	ln$avg_cnt				number := 0;
	ln$max_cnt				number := 0;
	ln$max_year				number := 0;
begin
	--!calculando estadisticas
	select min(rcnt)  min_cnt
		 , round(avg(rcnt))  avg_cnt
		 , max(rcnt)  max_cnt
		 , max(ryear) max_year
	  into ln$min_cnt	
		 , ln$avg_cnt
		 , ln$max_cnt
		 , ln$max_year
	  from olap_sys.pm_contador_digitos
	 where rcnt > 0
	   and rcomb = pn_comb;
--dbms_output.put_line('comb: '||pn_comb||' min: '||ln$min_cnt||' avg: '||ln$avg_cnt||' max: '||ln$max_cnt||' year: '||ln$max_year);	
	--!actualizando	info para todos los records		
	update olap_sys.pm_contador_digitos
	   set rcnt_min = ln$min_cnt
		 , rcnt_avg = ln$avg_cnt
		 , rcnt_max	= ln$max_cnt
		 , rcnt_dif = case when rcnt > 0 then ln$avg_cnt - rcnt end				 
	 where rprimos = pn_primo_cnt
	   and rcomb   = pn_comb;	

	--!actualizando	info para el ultimo anio
	update olap_sys.pm_contador_digitos
	   set rdrawing_id_dif = case when rcnt > 0 then pn_drawing_id - rlast_drawing_id end
		 , rcurrent_drawing_id = case when rcnt > 0 then pn_drawing_id end
	 where rprimos = pn_primo_cnt
	   and RYEAR   = ln$max_year
	   and rcomb   = pn_comb;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end actualiza_estadisticas;

--!calcula las estadisticas de digito para todos los anios
procedure calcula_estadisticas (pn_primo_cnt		number
							  , pn_drawing_id       number
							  , ptbl_inpar_par		DBMS_SQL.NUMBER_TABLE
							  , ptbl_primos 		DBMS_SQL.NUMBER_TABLE
							  , x_err_code       IN OUT NOCOPY NUMBER) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'calcula_estadisticas';	
begin
	--!numeros inpares y pares
	if ptbl_inpar_par.count > 0 then
		for r in ptbl_inpar_par.first..ptbl_inpar_par.last loop
			actualiza_estadisticas(pn_primo_cnt	 => pn_primo_cnt
							     , pn_drawing_id => pn_drawing_id
							     , pn_comb       => ptbl_inpar_par(r));
		end loop;
	end if;   

	--!numeros primmos
	if ptbl_primos.count > 0 then
		for q in ptbl_primos.first..ptbl_primos.last loop
			actualiza_estadisticas(pn_primo_cnt	 => pn_primo_cnt
							     , pn_drawing_id => pn_drawing_id
							     , pn_comb       => ptbl_primos(q));
		end loop;
	end if;   
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
exception
  when others then
    x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end calcula_estadisticas;


--!procedimiento para marcar los digitos que se encuentre por encima del valor del percentil
procedure marca_numeros_para_jugar (pn_percentile    number
                                  , pn_min_rcnt_dif  number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'marca_numeros_para_jugar';
	
	cursor c_main (pn_percentile    number
	             , pn_min_rcnt_dif  number) is
	with max_year_tbl as (           
	select max(ryear) max_ryear
	  from olap_sys.pm_contador_digitos
	), percentil_tbl as (
	select percentile_disc(pn_percentile) within group (order by rdrawing_id_dif) percentil
	  from olap_sys.pm_contador_digitos
	 where ryear = (select max_ryear from max_year_tbl)
	   and rcnt > 0
	) select *
		from olap_sys.pm_contador_digitos
	   where ryear = (select max_ryear from max_year_tbl)
		 and rcnt > 0 
		 and rdrawing_id_dif > (select percentil from percentil_tbl)
		 and rcnt_dif >= pn_min_rcnt_dif
	     for update; 		 
begin
	for w in c_main (pn_percentile => pn_percentile
	               , pn_min_rcnt_dif => pn_min_rcnt_dif) loop
		update olap_sys.pm_contador_digitos
		   set play_flag = CV$ENABLE
		 where current of c_main;
	end loop;
exception
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end marca_numeros_para_jugar;


--!procedimiento para contar numeros inpares, pares y primos en base al anio
procedure par_inpar_primo_cnt_handler (pn_year_end		number default 2022
									 , pn_years_back	number default 5
									 , pn_primo_cnt		number default 2
									 , pn_is_primo		number default 0
									 , pn_percentile    number default 0.5
									 , pn_min_rcnt_dif  number default 2) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'par_inpar_primo_cnt_handler';
	ln$err_code        		number := -1;
	ltbl$inpar_par 			DBMS_SQL.NUMBER_TABLE;
	ltbl$primos				DBMS_SQL.NUMBER_TABLE;		
begin
	delete olap_sys.pm_contador_digitos;
	
	--recuperar el ID del ultimo sorteo
	gn$drawing_id := get_max_drawing_id (pv_drawing_type => 'mrtr');
	
	--!inicializa los arreglos de numeros inpares, pares y primos
	inicializa_arreglos (pn_year_end	=> pn_year_end
					   , pn_years_back	=> pn_years_back
					   , pn_primo_cnt	=> pn_primo_cnt
					   , pn_is_primo    => pn_is_primo
					   , pn_percentile  => pn_percentile
					   , xtbl_inpar_par => ltbl$inpar_par
					   , xtbl_primos    => ltbl$primos
					   , x_err_code     => ln$err_code);
	
	if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		actualiza_tabla_contador (pn_is_primo  => pn_is_primo
								, x_err_code   => ln$err_code);	
		
		if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then		
			--!calcula las estadisticas de digito para todos los anios
			calcula_estadisticas (pn_primo_cnt	 => pn_primo_cnt
							    , pn_drawing_id  => gn$drawing_id
								, ptbl_inpar_par => ltbl$inpar_par
							    , ptbl_primos 	 => ltbl$primos
							    , x_err_code     => ln$err_code);
		
			if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then			
				--!procedimiento para marcar los digitos que se encuentre por encima del valor del percentil
				marca_numeros_para_jugar (pn_percentile   => pn_percentile
										, pn_min_rcnt_dif => pn_min_rcnt_dif);	
			end if;								
		end if;					  
	end if;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 		
end par_inpar_primo_cnt_handler;


--!procedimiento para insertar registros en la tabla plan_jugada_details
procedure ins_plan_jugada_details(pv_drawing_type		VARCHAR2
								, pn_plan_jugada_id		NUMBER
								, pv_pos1				VARCHAR2
								, pv_pos2				VARCHAR2
								, pv_pos3				VARCHAR2
								, pv_pos4				VARCHAR2
								, pv_pos5				VARCHAR2
								, pv_pos6				VARCHAR2
								, pn_lt_pattern_id		NUMBER
							 	 ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_plan_jugada_details';
begin
	if pv_pos1 is null and
	   pv_pos2 is null and
	   pv_pos3 is null and
	   pv_pos4 is null and
	   pv_pos5 is null and
	   pv_pos6 is null then
		--!cuando todos las posiciones vengan nulos no se debe insertar ningun registro
		null;
	else
		insert into olap_sys.plan_jugada_details (drawing_type
												, plan_jugada_id
												, id
												, description
												, pos1
												, pos2
												, pos3
												, pos4
												, pos5
												, pos6
												, lt_pattern_id
												, created_by
												, creation_date)
		values (pv_drawing_type
			  , pn_plan_jugada_id
			  , (select nvl(max(id),0) + 1 from olap_sys.plan_jugada_details)
			  , 'LEY_TERCIO_IN'
			  , pv_pos1
			  , pv_pos2
			  , pv_pos3
			  , pv_pos4
			  , pv_pos5
			  , pv_pos6
			  , pn_lt_pattern_id
			  , USER
			  , SYSDATE);										
		gn$ins_cnt := gn$ins_cnt + 1;
	end if;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 
end ins_plan_jugada_details;


--!procedimiento para crear las combinaciones de datos a ser insertados en la tabla plan_jugada_details
procedure plan_jugada_dtl_handler(pv_drawing_type           VARCHAR2
								, pn_drawing_case			NUMBER
								, ptbl$pos1 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos2 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos3 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos4 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos5 				DBMS_SQL.VARCHAR2_TABLE
								, ptbl$pos6 				DBMS_SQL.VARCHAR2_TABLE	
								, x_err_code  IN OUT NOCOPY NUMBER		 								
								) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'plan_jugada_dtl_handler';
	ltbl$pos1 				DBMS_SQL.VARCHAR2_TABLE;
	ltbl$pos2 				DBMS_SQL.VARCHAR2_TABLE;
	ltbl$pos3 				DBMS_SQL.VARCHAR2_TABLE;
	ltbl$pos4 				DBMS_SQL.VARCHAR2_TABLE;
	ltbl$pos5 				DBMS_SQL.VARCHAR2_TABLE;
	ltbl$pos6 				DBMS_SQL.VARCHAR2_TABLE;
	ln$plan_jugada_id       number := 0;
	ln$lt_pattern_id		number := 0;
	lb$data_found			boolean := false;
	
	--!cursor para obtener el ID del patron LT del porcentaje
	cursor c_patrones_porcentajes (pv_drawing_type           VARCHAR2
								 , pn_drawing_case			NUMBER) is
	SELECT ID
	  FROM OLAP_SYS.PLAN_JUGADAS
	 WHERE DRAWING_TYPE = pv_drawing_type
	   AND DESCRIPTION  = 'LT_PATRONES_PERCENTAGE'
	   AND STATUS       = 'A'
	   AND DRAWING_CASE = pn_drawing_case; 	
begin
	--!inicializando arreglos
	--!POS1
	ltbl$pos1 := ptbl$pos1; 
	--!POS2
	ltbl$pos2 := ptbl$pos2; 
	--!POS3
--	ltbl$pos3 := ltbl$pos3; 	
	--!POS4
	ltbl$pos4 := ptbl$pos4; 	
	--!POS5
--	ltbl$pos5 := ptbl$pos5; 	
	--!POS6
	ltbl$pos6 := ptbl$pos6; 

	--!inicializando contador de inserts
	gn$ins_cnt := 0;	
	
	for n in c_patrones_porcentajes (pv_drawing_type => pv_drawing_type
						           , pn_drawing_case => pn_drawing_case) loop
		
		ln$lt_pattern_id := n.id;
		
		--!recuperando las posiciones de los numeros primos
		for t in c_conf_ppn (pv_drawing_type => pv_drawing_type
						   , pn_drawing_case => pn_drawing_case) loop
			
			ln$plan_jugada_id := t.id;
			
			if ltbl$pos1.count > 0 then
				for p1 in ltbl$pos1.first..ltbl$pos1.last loop
					if ltbl$pos2.count > 0 then
						for p2 in ltbl$pos2.first..ltbl$pos2.last loop
							if ltbl$pos3.count > 0 then
								for p3 in ltbl$pos3.first..ltbl$pos3.last loop
									if ltbl$pos4.count > 0 then
										for p4 in ltbl$pos4.first..ltbl$pos4.last loop
											if ltbl$pos5.count > 0 then
												for p5 in ltbl$pos5.first..ltbl$pos5.last loop
													if ltbl$pos6.count > 0 then
														for p6 in ltbl$pos6.first..ltbl$pos6.last loop
															lb$data_found := true;
															ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																				  , pn_plan_jugada_id => ln$plan_jugada_id
																				  , pv_pos1           => ltbl$pos1(p1)
																				  , pv_pos2           => ltbl$pos2(p2)
																				  , pv_pos3           => ltbl$pos3(p3)
																				  , pv_pos4           => ltbl$pos4(p4)
																				  , pv_pos5           => ltbl$pos5(p5)
																				  , pv_pos6           => ltbl$pos6(p6)
																				  , pn_lt_pattern_id  => ln$lt_pattern_id);														
														end loop;
													--!POS6
													else
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => ltbl$pos1(p1)
																			  , pv_pos2           => ltbl$pos2(p2)
																			  , pv_pos3           => ltbl$pos3(p3)
																			  , pv_pos4           => ltbl$pos4(p4)
																			  , pv_pos5           => ltbl$pos5(p5)
																			  , pv_pos6           => NULL
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);													
													end if;												
												end loop;
											--!POS5
											else
												if ltbl$pos6.count > 0 then
													for p6 in ltbl$pos6.first..ltbl$pos6.last loop
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => ltbl$pos1(p1)
																			  , pv_pos2           => ltbl$pos2(p2)
																			  , pv_pos3           => ltbl$pos3(p3)
																			  , pv_pos4           => ltbl$pos4(p4)
																			  , pv_pos5           => NULL
																			  , pv_pos6           => ltbl$pos6(p6)
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);													
													end loop;
												--!POS6
												else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => NULL
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end if;												
											end if;									
										end loop;
									--!POS4
									else
										if ltbl$pos5.count > 0 then
											for p5 in ltbl$pos5.first..ltbl$pos5.last loop
												if ltbl$pos6.count > 0 then
													for p6 in ltbl$pos6.first..ltbl$pos6.last loop
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => ltbl$pos1(p1)
																			  , pv_pos2           => ltbl$pos2(p2)
																			  , pv_pos3           => ltbl$pos3(p3)
																			  , pv_pos4           => NULL
																			  , pv_pos5           => ltbl$pos5(p5)
																			  , pv_pos6           => ltbl$pos6(p6)
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);													
													end loop;
												--!POS6
												else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => NULL
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end if;												
											end loop;
										--!POS5
										else
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => NULL
																		  , pv_pos5           => NULL
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => NULL
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end if;									
									end if;
								end loop;
							--!POS3
							else
								if ltbl$pos4.count > 0 then
									for p4 in ltbl$pos4.first..ltbl$pos4.last loop
										if ltbl$pos5.count > 0 then
											for p5 in ltbl$pos5.first..ltbl$pos5.last loop
												if ltbl$pos6.count > 0 then
													for p6 in ltbl$pos6.first..ltbl$pos6.last loop
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => ltbl$pos1(p1)
																			  , pv_pos2           => ltbl$pos2(p2)
																			  , pv_pos3           => NULL
																			  , pv_pos4           => ltbl$pos4(p4)
																			  , pv_pos5           => ltbl$pos5(p5)
																			  , pv_pos6           => ltbl$pos6(p6)
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);	
													end loop;
												--!POS6
												else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => NULL
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end if;												
											end loop;
										--!POS5
										else
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => NULL
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => NULL
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end if;									
									end loop;
								--!POS4
								else
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => NULL
																		  , pv_pos4           => NULL
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => NULL
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);												
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => ltbl$pos1(p1)
																  , pv_pos2           => ltbl$pos2(p2)
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;												
									end if;									
								end if;						
							end if;
						end loop;
					--!POS2
					else
						if ltbl$pos3.count > 0 then
							for p3 in ltbl$pos3.first..ltbl$pos3.last loop
								if ltbl$pos4.count > 0 then
									for p4 in ltbl$pos4.first..ltbl$pos4.last loop
										if ltbl$pos5.count > 0 then
											for p5 in ltbl$pos5.first..ltbl$pos5.last loop
												if ltbl$pos6.count > 0 then
													for p6 in ltbl$pos6.first..ltbl$pos6.last loop
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => ltbl$pos1(p1)
																			  , pv_pos2           => NULL
																			  , pv_pos3           => ltbl$pos3(p3)
																			  , pv_pos4           => ltbl$pos4(p4)
																			  , pv_pos5           => ltbl$pos5(p5)
																			  , pv_pos6           => ltbl$pos6(p6)
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);													
													end loop;
												--!POS6
												else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => NULL
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end if;												
											end loop;
										--!POS5
										else
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => NULL
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => NULL
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end if;									
									end loop;
								--!POS4
								else
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => NULL
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => NULL
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => ltbl$pos1(p1)
																  , pv_pos2           => NULL
																  , pv_pos3           => ltbl$pos3(p3)
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;												
									end if;									
								end if;
							end loop;
						--!POS3
						else
							if ltbl$pos4.count > 0 then
								for p4 in ltbl$pos4.first..ltbl$pos4.last loop
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => NULL
																		  , pv_pos3           => NULL
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => ltbl$pos1(p1)
																		  , pv_pos2           => NULL
																		  , pv_pos3           => NULL
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;												
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => NULL
																	  , pv_pos3           => NULL
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => ltbl$pos1(p1)
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => ltbl$pos4(p4)
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);																				
										end if;												
									end if;									
								end loop;
							--!POS4
							else
								if ltbl$pos5.count > 0 then
									for p5 in ltbl$pos5.first..ltbl$pos5.last loop
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => ltbl$pos1(p1)
																	  , pv_pos2           => NULL
																	  , pv_pos3           => NULL
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => ltbl$pos1(p1)
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => ltbl$pos5(p5)
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);											
										end if;												
									end loop;
								--!POS5
								else
									if ltbl$pos6.count > 0 then
										for p6 in ltbl$pos6.first..ltbl$pos6.last loop
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => ltbl$pos1(p1)
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => ltbl$pos6(p6)
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end loop;
									--!POS6
									else
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => ltbl$pos1(p1)
															  , pv_pos2           => NULL
															  , pv_pos3           => NULL
															  , pv_pos4           => NULL
															  , pv_pos5           => NULL
															  , pv_pos6           => NULL
															  , pn_lt_pattern_id  => ln$lt_pattern_id);									
									end if;												
								end if;									
							end if;						
						end if;				
					end if;
				end loop;
			--!POS1
			else
				if ltbl$pos2.count > 0 then
					for p2 in ltbl$pos2.first..ltbl$pos2.last loop
						if ltbl$pos3.count > 0 then
							for p3 in ltbl$pos3.first..ltbl$pos3.last loop
								if ltbl$pos4.count > 0 then
									for p4 in ltbl$pos4.first..ltbl$pos4.last loop
										if ltbl$pos5.count > 0 then
											for p5 in ltbl$pos5.first..ltbl$pos5.last loop
												if ltbl$pos6.count > 0 then
													for p6 in ltbl$pos6.first..ltbl$pos6.last loop
														ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																			  , pn_plan_jugada_id => ln$plan_jugada_id
																			  , pv_pos1           => NULL
																			  , pv_pos2           => ltbl$pos2(p2)
																			  , pv_pos3           => ltbl$pos3(p3)
																			  , pv_pos4           => ltbl$pos4(p4)
																			  , pv_pos5           => ltbl$pos5(p5)
																			  , pv_pos6           => ltbl$pos6(p6)
																			  , pn_lt_pattern_id  => ln$lt_pattern_id);													
													end loop;
												--!POS6
												else
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => NULL
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => NULL
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end if;											
											end loop;
										--!POS5
										else
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => NULL
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => NULL
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);													
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);												
											end if;																				
										end if;									
									end loop;
								--!POS4
								else
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => NULL
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => NULL
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);													
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;											
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => ltbl$pos2(p2)
																  , pv_pos3           => ltbl$pos3(p3)
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;										
									end if;								
								end if;						
							end loop;					
						--!POS3
						else
							if ltbl$pos4.count > 0 then
								for p4 in ltbl$pos4.first..ltbl$pos4.last loop
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => NULL
																		  , pv_pos2           => ltbl$pos2(p2)
																		  , pv_pos3           => NULL
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else	
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;											
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);												
											end loop;
										--!POS6
										else	
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => ltbl$pos2(p2)
																  , pv_pos3           => NULL
																  , pv_pos4           => ltbl$pos4(p4)
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);											
										end if;									
									end if;									
								end loop;
							--!POS4
							else
								if ltbl$pos5.count > 0 then
									for p5 in ltbl$pos5.first..ltbl$pos5.last loop
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => ltbl$pos2(p2)
																	  , pv_pos3           => NULL
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => ltbl$pos2(p2)
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => ltbl$pos5(p5)
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;											
									end loop;
								--!POS5
								else
									if ltbl$pos6.count > 0 then
										for p6 in ltbl$pos6.first..ltbl$pos6.last loop
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => ltbl$pos2(p2)
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => ltbl$pos6(p6)
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end loop;
									--!POS6
									else
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => NULL
															  , pv_pos2           => ltbl$pos2(p2)
															  , pv_pos3           => NULL
															  , pv_pos4           => NULL
															  , pv_pos5           => NULL
															  , pv_pos6           => NULL
															  , pn_lt_pattern_id  => ln$lt_pattern_id);									
									end if;										
								end if;								
							end if;						
						end if;				
					end loop;			
				--!POS2
				else
					if ltbl$pos3.count > 0 then
						for p3 in ltbl$pos3.first..ltbl$pos3.last loop
							if ltbl$pos4.count > 0 then
								for p4 in ltbl$pos4.first..ltbl$pos4.last loop
									if ltbl$pos5.count > 0 then
										for p5 in ltbl$pos5.first..ltbl$pos5.last loop
											if ltbl$pos6.count > 0 then
												for p6 in ltbl$pos6.first..ltbl$pos6.last loop
													ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																		  , pn_plan_jugada_id => ln$plan_jugada_id
																		  , pv_pos1           => NULL
																		  , pv_pos2           => NULL
																		  , pv_pos3           => ltbl$pos3(p3)
																		  , pv_pos4           => ltbl$pos4(p4)
																		  , pv_pos5           => ltbl$pos5(p5)
																		  , pv_pos6           => ltbl$pos6(p6)
																		  , pn_lt_pattern_id  => ln$lt_pattern_id);												
												end loop;
											--!POS6
											else
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => NULL
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end if;										
										end loop;
									--!POS5
									else
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => NULL
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => ltbl$pos3(p3)
																  , pv_pos4           => ltbl$pos4(p4)
																  , pv_pos5           => NULL
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;											
									end if;								
								end loop;
							--!POS4
							else
								if ltbl$pos5.count > 0 then
									for p5 in ltbl$pos5.first..ltbl$pos5.last loop
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => NULL
																	  , pv_pos3           => ltbl$pos3(p3)
																	  , pv_pos4           => NULL
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);												
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => ltbl$pos3(p3)
																  , pv_pos4           => NULL
																  , pv_pos5           => ltbl$pos5(p5)
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);											
										end if;										
									end loop;
								--!POS5
								else
									if ltbl$pos6.count > 0 then
										for p6 in ltbl$pos6.first..ltbl$pos6.last loop
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => ltbl$pos3(p3)
																  , pv_pos4           => NULL
																  , pv_pos5           => NULL
																  , pv_pos6           => ltbl$pos6(p6)
																  , pn_lt_pattern_id  => ln$lt_pattern_id);											
										end loop;
									--!POS6
									else
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => NULL
															  , pv_pos2           => NULL
															  , pv_pos3           => ltbl$pos3(p3)
															  , pv_pos4           => NULL
															  , pv_pos5           => NULL
															  , pv_pos6           => NULL
															  , pn_lt_pattern_id  => ln$lt_pattern_id);										
									end if;											
								end if;							
							end if;				
						end loop;			
					--!POS3
					else
						if ltbl$pos4.count > 0 then
							for p4 in ltbl$pos4.first..ltbl$pos4.last loop
								if ltbl$pos5.count > 0 then
									for p5 in ltbl$pos5.first..ltbl$pos5.last loop
										if ltbl$pos6.count > 0 then
											for p6 in ltbl$pos6.first..ltbl$pos6.last loop
												ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																	  , pn_plan_jugada_id => ln$plan_jugada_id
																	  , pv_pos1           => NULL
																	  , pv_pos2           => NULL
																	  , pv_pos3           => NULL
																	  , pv_pos4           => ltbl$pos4(p4)
																	  , pv_pos5           => ltbl$pos5(p5)
																	  , pv_pos6           => ltbl$pos6(p6)
																	  , pn_lt_pattern_id  => ln$lt_pattern_id);											
											end loop;
										--!POS6
										else
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => ltbl$pos4(p4)
																  , pv_pos5           => ltbl$pos5(p5)
																  , pv_pos6           => NULL
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end if;										
									end loop;
								--!POS5
								else
									if ltbl$pos6.count > 0 then
										for p6 in ltbl$pos6.first..ltbl$pos6.last loop
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => ltbl$pos4(p4)
																  , pv_pos5           => NULL
																  , pv_pos6           => ltbl$pos6(p6)
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end loop;
									--!POS6
									else
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => NULL
															  , pv_pos2           => NULL
															  , pv_pos3           => NULL
															  , pv_pos4           => ltbl$pos4(p4)
															  , pv_pos5           => NULL
															  , pv_pos6           => NULL
															  , pn_lt_pattern_id  => ln$lt_pattern_id);									
									end if;										
								end if;									
							end loop;
						--!POS4
						else
							if ltbl$pos5.count > 0 then
								for p5 in ltbl$pos5.first..ltbl$pos5.last loop
									if ltbl$pos6.count > 0 then
										for p6 in ltbl$pos6.first..ltbl$pos6.last loop
											ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
																  , pn_plan_jugada_id => ln$plan_jugada_id
																  , pv_pos1           => NULL
																  , pv_pos2           => NULL
																  , pv_pos3           => NULL
																  , pv_pos4           => NULL
																  , pv_pos5           => ltbl$pos5(p5)
																  , pv_pos6           => ltbl$pos6(p6)
																  , pn_lt_pattern_id  => ln$lt_pattern_id);										
										end loop;
									--!POS6
									else
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => NULL
															  , pv_pos2           => NULL
															  , pv_pos3           => NULL
															  , pv_pos4           => NULL
															  , pv_pos5           => ltbl$pos5(p5)
															  , pv_pos6           => NULL
															  , pn_lt_pattern_id  => ln$lt_pattern_id);									
									end if;										
								end loop;
							--!POS5
							else
								if ltbl$pos6.count > 0 then
									for p6 in ltbl$pos6.first..ltbl$pos6.last loop
										ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
															  , pn_plan_jugada_id => ln$plan_jugada_id
															  , pv_pos1           => NULL
															  , pv_pos2           => NULL
															  , pv_pos3           => NULL
															  , pv_pos4           => NULL
															  , pv_pos5           => NULL
															  , pv_pos6           => ltbl$pos6(p6)
															  , pn_lt_pattern_id  => ln$lt_pattern_id);									
									end loop;
								--!POS6
								else
									ins_plan_jugada_details(pv_drawing_type	  => pv_drawing_type
														  , pn_plan_jugada_id => ln$plan_jugada_id
														  , pv_pos1           => NULL
														  , pv_pos2           => NULL
														  , pv_pos3           => NULL
														  , pv_pos4           => NULL
														  , pv_pos5           => NULL
														  , pv_pos6           => NULL
														  , pn_lt_pattern_id  => ln$lt_pattern_id);								
								end if;										
							end if;							
						end if;
					end if;			
				end if;	
			end if;
		end loop;
	end loop;
	dbms_output.put_line(gn$ins_cnt||' rows inserted');
	commit;	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
	
exception
  when others then
    rollback;
	x_err_code := olap_sys.w_common_pkg.GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise; 	
end plan_jugada_dtl_handler;
								
end w_pick_panorama_pkg;
/
show errors;