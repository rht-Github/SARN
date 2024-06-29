create or replace trigger olap_sys.biu_s_calculo_stats before insert or update on olap_sys.s_calculo_stats
/*************************************************************
trigger name: aiu_s_calculo_stats
purpose: set color_ley_tercio based on ley_tercio 
creation date: 3/4/2016
*************************************************************/
for each row
declare
    GV$CONTEXT              constant varchar2(30) := 'TRIGGER_STG';        
    LV$PROCEDURE_NAME       constant varchar2(30) := 'aiu_s_calculo_stats';
    lv$ca_ranking           olap_sys.s_calculo_stats.ca_ranking%type;     
	ln$pxc					number(1) := 9;
	ln$pre					number(1) := 9;
begin
   if inserting or updating then
      if :new.ley_tercio >= 2 then
         --[1: Red
         :new.color_ley_tercio := 1;
      elsif :new.ley_tercio = 1 then
         --[2: Green
         :new.color_ley_tercio := 2;
      elsif :new.ley_tercio = 0 then
         --[3: Blue
         :new.color_ley_tercio := 3;
      end if;         

      :new.prime_number_flag := olap_sys.w_common_pkg.is_prime_number (pn_digit => :new.digit);
      :new.inpar_number_flag := mod(:new.digit,2);

      if :new.preferencia_flag is not null then
         :new.preferencia_flag := 'X';
      end if;
	  
      if :new.winner_flag is not null then
         :new.winner_flag := upper(:new.winner_flag);
      end if;
      
      if inserting then
         :new.created_by    := USER;
         :new.creation_date := SYSDATE;
      end if;
      
      if updating then
         :new.updated_by := USER;
         :new.updated_date := SYSDATE; 
		 
		 
		 --!nueva logica para sincronizar ambas tablas s_calculo_stats y gl_lt_counts
		 if :new.winner_flag is not null then		 
			if :new.b_type = 'B1' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;

			if :new.b_type = 'B2' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;

			if :new.b_type = 'B3' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;

			if :new.b_type = 'B4' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;

			if :new.b_type = 'B5' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;

			if :new.b_type = 'B6' then
				update olap_sys.gl_lt_counts
				   set winner_flag = 'X'
				 where drawing_id = :new.drawing_id-1
				   and b_type = :new.b_type
				   and lt = decode(:new.color_ley_tercio,1,'R',2,'G',3,'B');
			end if;
		 end if;
		 
		 --!initializing variables
		 select decode(:new.pronos_ciclo,null,0,1)
		   into ln$pxc
		   from dual;
		   
		 select decode(:new.preferencia_flag,null,0,2)
		   into ln$pre
		   from dual;
		 
		 --!computing the value for the new column
		 select case when ln$pxc = 0 and ln$pre = 0 then 0 else case when ln$pxc = 0 and ln$pre > 0 then 3 
				else case when ln$pxc > 0 and ln$pre = 0 then 4 else case when ln$pxc > 0 and ln$pre > 0 then 5 else 9 end end end end
		   into :new.preferencia_num
		   from dual;
      end if;   


	  if :new.b_type in ('B1','B2','B6') then
		 if :new.rango_ley_tercio between 1 and 3 then
			:new.color_rango_ley_tercio := 'H';
		 elsif :new.rango_ley_tercio between 4 and 6 then
			:new.color_rango_ley_tercio := 'M';
		 elsif :new.rango_ley_tercio >= 7 then
			:new.color_rango_ley_tercio := 'L';
		 end if;
	  end if;

	  if :new.b_type in ('B3','B5') then
		 if :new.rango_ley_tercio between 1 and 5 then
			:new.color_rango_ley_tercio := 'H';
		 elsif :new.rango_ley_tercio between 6 and 10 then
			:new.color_rango_ley_tercio := 'M';
		 elsif :new.rango_ley_tercio >= 11 then
			:new.color_rango_ley_tercio := 'L';
		 end if;
	  end if;

	  if :new.b_type = 'B4' then
		 if :new.rango_ley_tercio between 1 and 6 then
			:new.color_rango_ley_tercio := 'H';
		 elsif :new.rango_ley_tercio between 7 and 12 then
			:new.color_rango_ley_tercio := 'M';
		 elsif :new.rango_ley_tercio >= 13 then
			:new.color_rango_ley_tercio := 'L';
		 end if;
	  end if;
	  
      --[in order to update ca_ranking a record for the same gambling_id must be inserted into table s_gl_ciclo_aparicion_stats
      if inserting then
         begin
            select distinct gc.ciclo_aparicion_ranking||gc.ciclo_aparicion_range 
              into lv$ca_ranking        
              from olap_sys.s_gl_ciclo_aparicion_stats gc 
             where gc.drawing_type = :new.drawing_type 
               and gc.b_type = :new.b_type 
               and gc.drawing_id = :new.drawing_id 
               and gc.ciclo_aparicion = :new.ciclo_aparicion 
               and rownum=1;
         dbms_output.put_line('new.b_type: '||:new.b_type||', new.id: '||:new.drawing_id||', new.ca: '||:new.ciclo_aparicion||', ranking: '||lv$ca_ranking);
         exception
           when no_data_found then
              lv$ca_ranking := null;
         end;        
         :new.ca_ranking := lv$ca_ranking;
      end if;            
   end if;
   
exception  
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
    olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                  , p_procedure_name => LV$PROCEDURE_NAME
                                  , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                  , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack()
                                   );
    raise;
end;
/
show errors;
