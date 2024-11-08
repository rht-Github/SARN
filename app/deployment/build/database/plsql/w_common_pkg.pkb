create or replace package body olap_sys.w_common_pkg as
  
  procedure get_segment_code (p_gambling_type     varchar2
                            , p_attribute         number
                            , p_no_segments       olap_sys.w_segments_d.no_segments%type
                            , p_segment_code  out olap_sys.w_segments_d.segment_code%type
                            , p_segment_order out olap_sys.w_segments_d.segment_order%type                            
                             ) is
  
  begin
     select segment_code
          , segment_order
       into p_segment_code
          , p_segment_order
       from olap_sys.w_segments_d
      where attribute3 = p_gambling_type
        and p_attribute between low_value and high_value
        and no_segments = p_no_segments;      
  exception
    when no_data_found then   
       dbms_output.put_line('get_segment_code -> no data found -> p_attribute: '||p_attribute||', p_no_segments: '||p_no_segments); 
       raise; 
  end get_segment_code;                          
  
  procedure get_sl_gamblings (p_gambling_type        varchar2
                            , p_gambling_date        date 
                            , p_gam_tbl       in out g_gam_tbl
                             ) is
  
  begin
     select --trunc(to_date(gambling_date, w_common_pkg.g_date_format)) gambling_date
            trunc(to_date(gambling_date, 'dd-mm-yy')) gambling_date     
          , gambling_id
          , gambling_type
          , comb1
          , comb2
          , comb3
          , comb4
          , comb5
          , comb6
          , additional
          , comb_sum
          , priority
          , sum_par_comb
       bulk collect into p_gam_tbl 
       from olap_sys.sl_gamblings
      where 1=1 
        and to_date(gambling_date, 'dd-mm-yy') >= nvl(p_gambling_date,to_date(gambling_date, 'dd-mm-yy'))  
        and gambling_type = p_gambling_type
        order by to_date(gambling_date, 'dd-mm-yy');

dbms_output.put_line('p_gam_tbl: '||p_gam_tbl.count);        

  exception
    when others then
     dbms_output.put_line('get_sl_gamblings -> gambling_date: '||p_gambling_date||' -> gambling_type -> '||p_gambling_type||' -> others -> '||sqlerrm);
     raise;    
  end get_sl_gamblings;  
  
               
  --<03182010. end>

  --<05092010. begin>
/*  procedure main (p_gambling_type             varchar2) is
  
  begin
     get_rowcnt_sl_gamblings (p_gambling_type       => p_gambling_type
                            , p_rowcnt_sl_gamblings => g_rowcnt
                             );
  
  end main;  
*/
  function get_t_gambling_types_comb_no (p_gambling_type     olap_sys.t_gambling_types.gambling_type%type) return number is
     l$comb_no    olap_sys.t_gambling_types.comb_no%type := 0;
  begin
    select comb_no 
      into l$comb_no
      from olap_sys.t_gambling_types
     where gambling_type = p_gambling_type;
    return l$comb_no;
  exception
    when no_data_found then
       return 0;  
  end get_t_gambling_types_comb_no;

  --[funtion used to return a numeric equivalent value based on a input combination value
  function get_equivalent_value (p_combination varchar2) return number is
  begin
    if substr(nvl(p_combination,'X'),1,1) = 'C' then
       return 1;
    else
       return 0;
    end if;      
  end get_equivalent_value;

  procedure get_t_gambling_types_min_max (p_gambling_type         olap_sys.t_gambling_types.gambling_type%type
                                        , p_min_value      in out olap_sys.t_gambling_types.min_value%type
                                        , p_max_value      in out olap_sys.t_gambling_types.max_value%type
                                         ) is
  begin
    select min_value
         , max_value
      into p_min_value
         , p_max_value
      from olap_sys.t_gambling_types   
     where gambling_type = p_gambling_type;
  exception
    when no_data_found then
       p_min_value := 0;
       p_max_value := 0;        
  end get_t_gambling_types_min_max;

  function convert_f (p_number          number
                    , p_convert_type    varchar2 default 'DIGIT'
                    , p_gambling_type   varchar2) return number is

    ln$Sum_Mean        number := 0;
    ln$Digit_Mean      number := 0;
  begin
    g_return_value := 0;
    if p_convert_type = 'DIGIT' then
       begin
          select (min_value + max_value)/2 Digit_Mean
            into ln$Digit_Mean
            from olap_sys.t_gambling_types
           where gambling_type = p_gambling_type;
          
          if p_number <= ln$Digit_Mean then
             g_return_value := 1;
          elsif p_number >= ln$Digit_Mean+1 then
             g_return_value := 2;
          end if;       
       exception
         when no_data_found then          
            g_return_value := 0;
       end;
    else
       g_return_value := 0;
    end if; 
    return g_return_value;
  end convert_f;

  function convert_low_high_f (p_number          number
                             , p_convert_type    varchar2 default 'DIGIT'
                             , p_gambling_type   varchar2 default 'mrtr') return number is
  
    
    ln$Low_Low     number := 0;
    ln$Low_High    number := 0;
    ln$High_Low    number := 0;
    
               
  begin
    return g_return_value;
  end convert_low_high_f;

  --[ function used to return an detailed equivalent value based on low-low, low-high, high-low and high-high values logic                                       
  function convert_low_high_dtl_f (p_number          number) return number is
  begin
     g_return_value := 0;
        if p_number >= 1  and p_number <= 5  then g_return_value := 1;
     elsif p_number >= 6  and p_number <= 10 then g_return_value := 2;
     elsif p_number >= 11 and p_number <= 15 then g_return_value := 3;
     elsif p_number >= 16 and p_number <= 20 then g_return_value := 4;
     elsif p_number >= 21 and p_number <= 25 then g_return_value := 5;
     elsif p_number >= 26 and p_number <= 30 then g_return_value := 6;
     elsif p_number >= 31 and p_number <= 35 then g_return_value := 7;
     elsif p_number >= 36 and p_number <= 39 then g_return_value := 8;
     else g_return_value := 0;
     end if;
     return g_return_value;
  end convert_low_high_dtl_f;                                       

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

   --[ function used to retrieve the host name used to send emails to outside world                                     
   function get_nls_charset return varchar2 RESULT_CACHE is
      lv$nls_charset   varchar2(30) := 'localhost';
   begin
      select value
        into lv$nls_charset
        from nls_database_parameters
       where parameter = 'NLS_CHARACTERSET';
      return lv$nls_charset;
   exception
     when others then
        return null;   
   end get_nls_charset;

   --[ Write a MIME header
   procedure write_mime_header (p_conn   in out nocopy utl_smtp.connection
                              , pv_name                varchar2
                              , pv_value               varchar2
                             )
                             is
   begin
      utl_smtp.write_data ( p_conn
                          , pv_name || ': ' || pv_value || utl_tcp.crlf
      );
   end write_mime_header;
/*   
   --[ procedure used to send emails out to existing users inserted into table c_users                          
   procedure send_mail (pv_sender                              varchar2
                      , pv_recipient                           varchar2
                      , pv_subject                             varchar2
                      , pv_msg_text                            varchar2
                      , pv_nls_charset                         varchar2 default 'WE8MSWIN1252' 
                      , x_err_code              in out NOCOPY number
                      , x_err_msg               in out NOCOPY varchar2  
                      ) is
                      
     LV$PROCEDURE_NAME       constant varchar2(30) := 'send_mail';
     LV$SMTP_HOST            constant varchar2(30) := 'localhost';
     LN$SMTP_PORT            constant pls_integer  := 1925;
     LV$SMTP_DOMAIN          constant varchar2(30) := 'smtp.gmail.com';
     LV$MAILER_ID            constant varchar2(256) := 'Mailer by Oracle UTL_SMTP'; 
     mail_conn               utl_smtp.connection;
     rc integer; 
   begin
   dbms_output.put_line(LV$PROCEDURE_NAME);
   dbms_output.put_line('pv_sender: '||pv_sender);
   dbms_output.put_line('pv_recipient: '||pv_recipient);
   dbms_output.put_line('pv_subject: '||pv_subject);
   dbms_output.put_line('pv_msg_text: '||substr(pv_msg_text,1,50));
   dbms_output.put_line('pv_nls_charset: '||pv_nls_charset);   
   dbms_output.put_line('LV$SMTP_HOST: '||LV$SMTP_HOST);
   dbms_output.put_line('LN$SMTP_PORT: '||LN$SMTP_PORT);
   dbms_output.put_line('LV$SMTP_DOMAIN: '||LV$SMTP_DOMAIN);
   dbms_output.put_line('***** PIECE OF CODE NOT WORKING YET *****');
 
--      EXECUTE IMMEDIATE 'ALTER SESSION SET smtp_out_server = '||chr(39)||LV$SMTP_HOST||chr(39);
      -- establish connection and autheticate
      mail_conn := utl_smtp.open_connection(LV$SMTP_HOST, LN$SMTP_PORT); -- SMTP on port 25 
      utl_smtp.helo(mail_conn, LV$SMTP_DOMAIN);
      utl_smtp.command(mail_conn, 'auth login');
      utl_smtp.command(mail_conn,utl_encode.text_encode(GV$MAIL_FROM, pv_nls_charset, 1));
      utl_smtp.command(mail_conn, utl_encode.text_encode('Ingenier1a', pv_nls_charset, 1));

      -- set from/recipient
      utl_smtp.command(mail_conn, 'MAIL FROM: <'||pv_sender||'>');
      utl_smtp.command(mail_conn, 'RCPT TO: <'||pv_recipient||'>');

      -- write mime headers
      utl_smtp.open_data (mail_conn);
      write_mime_header (mail_conn, 'From', pv_sender);
      write_mime_header (mail_conn, 'To', pv_recipient);
      write_mime_header (mail_conn, 'Subject', pv_subject);
      write_mime_header (mail_conn, 'Content-Type', 'text/plain');
      write_mime_header (mail_conn, 'X-Mailer', LV$MAILER_ID);
      utl_smtp.write_data (mail_conn, utl_tcp.crlf);
      
      -- write message body
      utl_smtp.write_data (mail_conn, pv_msg_text);
      utl_smtp.close_data (mail_conn);
      
      -- end connection
      utl_smtp.quit (mail_conn);
            
      x_err_code := olap_sys.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
      x_err_msg  := LV$PROCEDURE_NAME||olap_sys.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION; 
   exception
     WHEN UTL_SMTP.INVALID_OPERATION THEN
       utl_smtp.quit (mail_conn);
       x_err_code := sqlcode;
       x_err_msg  := 'Invalid Operation in Mail attempt using UTL_SMTP.'; 
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    
       olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                     , p_package_name   => GV$PACKAGE_NAME
                                     , p_procedure_name => LV$PROCEDURE_NAME
                                     , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                     , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||' USER_EXCEPTION: '||x_err_msg||' ~ '||dbms_utility.format_error_stack()
                                      );
       raise;  
     WHEN UTL_SMTP.TRANSIENT_ERROR THEN
       x_err_code := sqlcode;
       x_err_msg  := 'Temporary e-mail issue - try again.'; 
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    
       olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                     , p_package_name   => GV$PACKAGE_NAME
                                     , p_procedure_name => LV$PROCEDURE_NAME
                                     , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                     , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||' USER_EXCEPTION: '||x_err_msg||' ~ '||dbms_utility.format_error_stack()
                                      );
       raise;  
     WHEN UTL_SMTP.PERMANENT_ERROR THEN
       x_err_code := sqlcode;
       x_err_msg  := 'Permanent Error Encountered.'; 
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    
       olap_sys.w_common_pkg.save_log (p_xcontext       => GV$CONTEXT
                                     , p_package_name   => GV$PACKAGE_NAME
                                     , p_procedure_name => LV$PROCEDURE_NAME
                                     , p_attribute1     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR
                                     , p_attribute7     => olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||' USER_EXCEPTION: '||x_err_msg||' ~ '||dbms_utility.format_error_stack()
                                      );
       raise;  
   end send_mail;                      
*/
   --[ function used to get number of emails not sent while a process is executed
   function get_cnt_mails_not_sent (pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type 
                                  , pn_setup_id                         olap_sys.mails_sent_history_f.setup_id%type default null
                                  , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type default 'S'
                                  , pv_send_flag                        olap_sys.mails_sent_history_f.send_flag%type default 'N'
                                  , pv_assigned_to                      olap_sys.mails_sent_history_f.assigned_to%type default null 
                                  , pn_year                             olap_sys.mails_sent_history_f.year%type default null 
                                  , pn_quarter                          olap_sys.mails_sent_history_f.quarter%type default null
                                  , pn_month                            olap_sys.mails_sent_history_f.month%type default null
                                   ) return number RESULT_CACHE is                      
   begin
      g_rowcnt := 0;
      select count(1) 
        into g_rowcnt
        from olap_sys.mails_sent_history_f 
       where drawing_type = pv_drawing_type
         and setup_id     = nvl(pn_setup_id,setup_id)
         and mail_type    = pv_mail_type
         and send_flag    = pv_send_flag
         and assigned_to  = nvl(pv_assigned_to,assigned_to)
         and year         = nvl(pn_year,year)
         and quarter      = nvl(pn_quarter,quarter)  
         and month        = nvl(pn_month,month);     
      return g_rowcnt;
   exception
     when others then
        return 0;
   end;                   

   --[ function that will return a number value
   function get_lookup_values_1_colnum (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                      , pv_context                     olap_sys.w_lookups_fs.context%type
                                      , pv_code                        olap_sys.w_lookups_fs.code%type
                                      , pv_column_name                 varchar2
                                        ) return number RESULT_CACHE is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_lookup_values_1_colnum';
     x_err_code                       number := 0;
     x_err_msg                        varchar2(1000);
     ln$lookup_value                  olap_sys.w_lookups_fs.attribute1%type;
   begin                                        
     g_dml_stmt := 'select '||pv_column_name||' from olap_sys.w_lookups_fs where gambling_type = : 1 and context = :2 and code = :3';
     execute immediate g_dml_stmt into ln$lookup_value using pv_gambling_type
                                                           , pv_context
                                                           , pv_code;
     return ln$lookup_value;                                                      
   exception
     when no_data_found then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm||' gambling_type: '||pv_gambling_type||' context: '||pv_context||' code: '||pv_code;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

       return -1;
     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

       return -1;
   end get_lookup_values_1_colnum;
   
   --[ function that will return a varchar2 value
   function get_lookup_values_1_colvar (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                      , pv_context                     olap_sys.w_lookups_fs.context%type
                                      , pv_code                        olap_sys.w_lookups_fs.code%type
                                      , pv_column_name                 varchar2
                                       ) return varchar2 RESULT_CACHE is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_lookup_values_1_colvar';
     x_err_code                       number := 0;
     x_err_msg                        varchar2(1000);
     lv$lookup_value                  olap_sys.w_lookups_fs.attribute3%type;
   begin                                        
     g_dml_stmt := 'select '||pv_column_name||' from olap_sys.w_lookups_fs where gambling_type = : 1 and context = :2 and code = :3';
     execute immediate g_dml_stmt into lv$lookup_value using pv_gambling_type
                                                           , pv_context
                                                           , pv_code;
     return lv$lookup_value;                                                      
   exception
     when no_data_found then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm||' gambling_type: '||pv_gambling_type||' context: '||pv_context||' code: '||pv_code;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

       return null;
     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

       return null;
   end get_lookup_values_1_colvar;
   
   --[ procedure that will return two varchar2 values                                   
   procedure get_lookup_values_2_colvar (pv_gambling_type               olap_sys.w_lookups_fs.gambling_type%type
                                       , pv_context                     olap_sys.w_lookups_fs.context%type
                                       , pv_code                        olap_sys.w_lookups_fs.code%type
                                       , xv_attribute3    in out NOCOPY olap_sys.w_lookups_fs.attribute3%type
                                       , xv_attribute4    in out NOCOPY olap_sys.w_lookups_fs.attribute4%type
                                        ) is
     LV$PROCEDURE_NAME       constant varchar2(30) := 'get_lookup_values_2_colvar';
     x_err_code                       number := 0;
     x_err_msg                        varchar2(1000);
   begin                                        
     g_dml_stmt := 'select attribute3, attribute4 from olap_sys.w_lookups_fs where gambling_type = : 1 and context = :2 and code = :3';
     execute immediate g_dml_stmt into xv_attribute3, xv_attribute4 using pv_gambling_type
                                                                        , pv_context
                                                                        , pv_code;                                                    
   exception
     when no_data_found then
       x_err_code    := sqlcode;
       x_err_msg     := sqlerrm||' gambling_type: '||pv_gambling_type||' context: '||pv_context||' code: '||pv_code;
       xv_attribute3 := null;
       xv_attribute4 := null;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg||' ~ '||dbms_utility.format_error_stack());    

       raise;
   end get_lookup_values_2_colvar;

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
                               ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'save_mail_history';
   begin
     olap_sys.w_common_pkg.g_rowcnt := -1;
     select count(1)
       into olap_sys.w_common_pkg.g_rowcnt
       from olap_sys.mails_sent_history_f
      where drawing_type = pv_drawing_type
        and subject      = pv_subject
        and mail_type    = pv_mail_type
        and assigned_to  = pv_assigned_to;
        
     if olap_sys.w_common_pkg.g_rowcnt = 0 then   
	     insert into olap_sys.mails_sent_history_f (drawing_type   
	                                              , sender         
	                                              , recipient      
	                                              , subject        
	                                              , msg            
	                                              , mail_type      
	                                              , send_flag      
	                                              , setup_id       
	                                              , next_drawing_id
	                                              , drawing_date   
	                                              , year        	
	                                              , quarter     	
	                                              , month          
	                                              , assigned_to    
	                                              , created_by	
	                                              , creation_date 	
	                                               )
	     values (pv_drawing_type   
	           , pv_sender         
	           , pv_recipient      
	           , pv_subject        
	           , pv_msg            
	           , pv_mail_type  
	           , decode(pv_email_send_flag,'N','X','Y','N')    
	           , pn_setup_id       
	           , pn_next_drawing_id
	           , pd_drawing_date
	           , to_number(to_char(SYSDATE,'YYYY'))
	           , to_number(to_char(SYSDATE,'Q'))
	           , to_number(to_char(SYSDATE,'MM'))
	           , pv_assigned_to
	           , USER
	           , SYSDATE
	            );   
	     commit;    
     end if;                                    
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION; 
   exception
     when dup_val_on_index then
       x_err_code := sqlcode;
       x_err_msg  := dbms_utility.format_error_stack()||' drawing_type: '||pv_drawing_type||' subject: '||pv_subject||' mail_type: '||pv_mail_type||' assigned_to: '||pv_assigned_to;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||x_err_msg);    

     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

       raise;                               
   end save_mail_history;                                                              
/*   
   --[ wrapper used to call procedure send_email
   procedure send_mail_p (pv_drawing_type                      olap_sys.mails_sent_history_f.drawing_type%type
                         , pn_setup_id                         olap_sys.mails_sent_history_f.setup_id%type
                         , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type
                         , x_err_code            in out NOCOPY number
                         , x_err_msg             in out NOCOPY varchar2                            
                          ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'send_mail_p';
     ln$err_code                        number       := 0;
     lv$err_msg                         varchar2(1000);                            
     le$mail_exception                  exception;
     pragma exception_init (le$mail_exception, -20010);
     cursor c_mail (pv_drawing_type                     olap_sys.w_combinations_picked_f.attribute3%type
                  , pn_setup_id                         olap_sys.w_combinations_picked_f.setup_id%type
                  , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type) is
     select sender
          , recipient
          , subject
          , msg
          , assigned_to
       from olap_sys.mails_sent_history_f
      where drawing_type = pv_drawing_type
        and setup_id     = pn_setup_id
        and mail_type    = pv_mail_type
        and send_flag    = OLAP_SYS.W_COMMON_PKG.GV$MAIL_NOT_SENT; 
   begin
     olap_sys.w_common_pkg.g_errorcnt := 0;
     for m in c_mail (pv_drawing_type => pv_drawing_type
                    , pn_setup_id     => pn_setup_id
                    , pv_mail_type    => pv_mail_type) loop   
        send_mail (pv_sender    => m.sender
                 , pv_recipient => m.recipient
                 , pv_subject   => m.subject
                 , pv_msg_text  => m.msg
                 , x_err_code   => ln$err_code
                 , x_err_msg    => lv$err_msg 
                 );
        if ln$err_code != OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION then
           raise le$mail_exception;
        else
           upd_mails_sent_history_f(pv_drawing_type => pv_drawing_type   
                                  , pn_setup_id     => pn_setup_id
                                  , pv_subject      => m.subject
                                  , pv_mail_type    => pv_mail_type
                                  , pv_assigned_to  => M.assigned_to
                                  , x_err_code      => ln$err_code
                                  , x_err_msg       => lv$err_msg
                                   );   
        end if;                                  
     end loop;                               
   
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION
                               ||' '||olap_sys.w_common_pkg.get_cnt_mails_not_sent (pv_drawing_type => pv_drawing_type, pn_setup_id => pn_setup_id)||' mails were not sent for drawing_type: '||pv_drawing_type
                               ||' setup_id: '||pn_setup_id;  
                                                               

   
   exception
     when le$mail_exception then
       x_err_code := ln$err_code;
       x_err_msg  := lv$err_msg;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||' le$mail_exception: '||dbms_utility.format_error_stack());    

       raise;                                 
     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

       raise;                               
   end send_mail_p;                                                              
 */  
   --[ procedure used to update send flag as Y                                 
   procedure upd_mails_sent_history_f(pv_drawing_type                     olap_sys.mails_sent_history_f.drawing_type%type   
                                    , pn_setup_id                         olap_sys.w_combinations_picked_f.setup_id%type
                                    , pv_subject                          olap_sys.mails_sent_history_f.subject%type
                                    , pv_mail_type                        olap_sys.mails_sent_history_f.mail_type%type
                                    , pv_assigned_to                      olap_sys.mails_sent_history_f.assigned_to%type 
                                    , x_err_code            in out NOCOPY number
                                    , x_err_msg             in out NOCOPY varchar2                            
                                     ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'upd_mails_sent_history_f';
   begin
     update olap_sys.mails_sent_history_f 
        set send_flag    = OLAP_SYS.W_COMMON_PKG.GV$MAIL_SENT 
          , updated_by   = USER
          , updated_date = SYSDATE
      where drawing_type = pv_drawing_type 
        and setup_id     = pn_setup_id
        and subject      = pv_subject 
        and mail_type    = pv_mail_type
        and assigned_to  = pv_assigned_to;
     commit;   
     x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
     x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION; 
   exception
     when others then
       x_err_code := sqlcode;
       x_err_msg  := sqlerrm;
       dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

       raise;                               
   end upd_mails_sent_history_f;                                                              

   --[ function used to retrieve drawing description
   function get_drawing_desc (pv_drawing_type   olap_sys.w_combinations_picked_f.attribute3%type) return varchar result_cache is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'get_drawing_desc';
     lv$drawing_desc      olap_sys.t_gambling_types.description%type;
   begin
     select description
       into lv$drawing_desc
       from olap_sys.t_gambling_types
      where gambling_type = pv_drawing_type; 
     return lv$drawing_desc;
   exception
     when no_data_found then
       return 'description unavailable';  
   end;


  --[ function used to show owner bank account info
  function owner_bank_account_info (pv_drawing_type       olap_sys.w_combinations_picked_f.attribute3%type
                                  , pv_owner_full_name    varchar2
                                   ) return varchar result_cache is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'owner_bank_account_info';
     lv$account_info                    varchar2(4000);                                   
  begin
     lv$account_info := 'Account info'||gv$enter;
     return lv$account_info;
  end owner_bank_account_info;

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
                                      ) is
     LV$PROCEDURE_NAME         constant varchar2(30) := 'upd_status_vendor_drawing';
     ln$seq_id                          number := 0;
  begin
     dbms_output.put_line('------------------------------------');
     dbms_output.put_line(LV$PROCEDURE_NAME);
     dbms_output.put_line('drawing_type: '||pv_drawing_type||' seq_id: '||pn_seq_id||' c1: '||pn_comb1||' c2: '||pn_comb2||' c3: '||pn_comb3||' c4: '||pn_comb4||' c5: '||pn_comb5||' c6: '||pn_comb6||' status: '||pv_status);
     if pn_seq_id is null then
        ln$seq_id := pn_comb1||pn_comb2||pn_comb3||pn_comb4||pn_comb5||pn_comb6;     
     else
        ln$seq_id := pn_seq_id;
     end if;
     dbms_output.put_line('ln$seq_id: '||ln$seq_id);
     
     update olap_sys.w_combination_responses_fs
        set status           = pv_status
          , gambling_counter = decode(pv_status,OLAP_SYS.W_COMMON_PKG.GV$EXCLUDED,gambling_counter+1, gambling_counter)
          , updated_by       = USER
          , updated_date     = SYSDATE
      where attribute3 = pv_drawing_type
        and seq_id     = ln$seq_id;
     
     if sql%found then
        --[ updating temporal table in order to keep matching status between tables related   
        update olap_sys.tmp_loading_drawing_details dd
           set status = pv_status       
         where dd.drawing_type = pv_drawing_type
           and dd.seq_id       = ln$seq_id
           and exists (select 1
                        from olap_sys.w_combination_responses_fs cr
                       where cr.attribute3 = pv_drawing_type
                         and cr.seq_id     = dd.seq_id
                         and cr.status     = pv_status
                          );       
        x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
        x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION;
     else
        x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
        x_err_msg  := olap_sys.W_COMMON_PKG.GV_CONTEXT_WARNING||' ~ '||LV$PROCEDURE_NAME||': drawing_type: '||pv_drawing_type||', seq_id: '||pn_seq_id||' were not updated on table w_combination_responses_fs';
     end if;
     dbms_output.put_line('------------------------------------');
  exception
    when others then
      x_err_code := sqlcode;
      x_err_msg  := sqlerrm;
      dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

      raise;       
  end upd_status_vendor_drawing;

--[ procedure used to get the next drawing id and date based on the latest gambling id inserted into table sl_gamblings                         
procedure get_next_drawing_id_date (p_gambling_type                    varchar2
                                  , xn_next_drawing_id   in out NOCOPY olap_sys.w_comb_setup_header_fs.next_drawing_id%type
                                  , xn_next_drawing_date in out NOCOPY olap_sys.w_comb_setup_header_fs.gambling_date%type 
                                  , x_err_code           in out NOCOPY number
                                  , x_err_msg            in out NOCOPY varchar2
                                   ) is
   LV$PROCEDURE_NAME         constant varchar2(30) := 'get_next_drawing_id_date';
   ln$max_id_sl_gamblings             number := 0;
   ln$max_id_comb_details             number := 0;
   ln$current_day_num                 number := 0;
   lv$next_day                        varchar2(3);
   ln$add_days                        number := 0;
   ld$next_gambling_date              date;
   ld$current_date                    date := SYSDATE;
   le$no_matching_drawing_id          exception;
   pragma exception_init(le$no_matching_drawing_id, -24381);
   cursor c_days (p_gambling_type                    varchar2) is
   select to_number(attribute1) day_num
        , attribute4 day_gambling
     from olap_sys.w_lookups_fs
    where context       = 'USER_GAMBLING_DATE' 
      and gambling_type = p_gambling_type
    order by 1;
begin
--   select to_number(to_char(SYSDATE,'D'))
--     into ln$current_day_num
--     from dual;
   dbms_output.put_line('50. D: '||to_char(ld$current_date,'D'));   
   ln$current_day_num := to_number(to_char(ld$current_date,'D'));

   dbms_output.put_line('100. current_day_num: '||ln$current_day_num);   
   olap_sys.w_common_pkg.g_index := 1;
   for k in c_days (p_gambling_type => p_gambling_type) loop
       if olap_sys.w_common_pkg.g_index = 1 then
--   dbms_output.put_line('k=1, k.day_num: '||k.day_num);
          if ln$current_day_num <= k.day_num then
--   dbms_output.put_line('101');
       	     ln$add_days := abs(k.day_num - ln$current_day_num);
       	     lv$next_day := k.day_gambling;
       	     exit;
       	  end if;
       elsif olap_sys.w_common_pkg.g_index = 2 then	     
--   dbms_output.put_line('k=2, k.day_num: '||k.day_num);
          if ln$current_day_num <= k.day_num then
--   dbms_output.put_line('102');
       	     ln$add_days := abs(k.day_num - ln$current_day_num);
       	     lv$next_day := k.day_gambling;
       	     exit;
          end if;	  
       end if;
       olap_sys.w_common_pkg.g_index := olap_sys.w_common_pkg.g_index + 1;
   end loop;
   dbms_output.put_line('200. next_day: '||lv$next_day||' add_days: '||ln$add_days);   
   
   --ld$next_gambling_date := ld$current_date+ln$add_days-7;
   ld$next_gambling_date := ld$current_date-7;
   dbms_output.put_line('300. ld$next_gambling_date: '||ld$next_gambling_date);
   
   begin
   with sl_gamblings_tbl as(  
   select max(gambling_id)+2 next_drawing_id
        , to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)+7 next_drawing_date
     from olap_sys.sl_gamblings
    where gambling_type = p_gambling_type
      and week_day      = lv$next_day
      and trunc(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format)) = trunc(ld$next_gambling_date)
    group by gambling_id
        , gambling_date
    order by 1 desc)
   select next_drawing_id
        , next_drawing_date
     into xn_next_drawing_id
        , xn_next_drawing_date   
     from sl_gamblings_tbl
    where rownum = 1; 	    

  dbms_output.put_line('400. xn_next_drawing_id: '||xn_next_drawing_id||' xn_next_drawing_date: '||xn_next_drawing_date);      
      x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
      x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION;

   exception
     when no_data_found then
      x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
      x_err_msg  := LV$PROCEDURE_NAME||' 500. no data found in table olap_sys.sl_gamblings';

       dbms_output.put_line(x_err_msg);
       dbms_output.put_line('500. type: '||p_gambling_type||'  day: '||lv$next_day||'  date: '||trunc(ld$next_gambling_date));      
       raise;     
   end;       
exception
  when no_data_found then
    x_err_code := OLAP_SYS.W_COMMON_PKG.GN$FAILED_EXECUTION;
    x_err_msg  := 'Invalid gamgling date.';  
    raise;
  when others then
    x_err_code := sqlcode;
    x_err_msg  := sqlerrm;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

    raise;       
end get_next_drawing_id_date;

--[ function used to return the median value from a set of comb_sum
function get_median_value (p_numerical_value_tbl  olap_sys.tbl_numerical_value) return number is
   LV$PROCEDURE_NAME         constant varchar2(30) := 'get_median_value';
   ln$median_index                    pls_integer  := 0;
   lb$pair_array_cnt                  boolean      := false;
   ln$median_value                    number       := 0;
   ln$loop_index                      pls_integer  := 1;
   cursor c_value (pt_type_name olap_sys.tbl_numerical_value) is
   select value
     from (table(cast(pt_type_name AS olap_sys.tbl_numerical_value)))
    order by value;  
begin
   dbms_output.put_line('-------------------------------------------------');
   dbms_output.put_line('OLAP_SYS.'||GV$PACKAGE_NAME||'.'||LV$PROCEDURE_NAME);
   dbms_output.put_line('p_numerical_value_tbl: '||p_numerical_value_tbl.count);

   --[ find out if array count is a pair value
   if mod(p_numerical_value_tbl.count,2) = 0 then 
      ln$median_index   := p_numerical_value_tbl.count / 2;
      lb$pair_array_cnt := true;
   else
      ln$median_index := ((p_numerical_value_tbl.count-1) / 2) + 1;
   end if;
   dbms_output.put_line('tbl.count: '||p_numerical_value_tbl.count||'  ln$median_index: '||ln$median_index);
   for j in c_value (pt_type_name => p_numerical_value_tbl) loop
      if not lb$pair_array_cnt then
         if ln$loop_index = ln$median_index then
            ln$median_value := j.value;
         end if;   
      else
         if ln$loop_index = ln$median_index then
            dbms_output.put_line('ln$loop_index: '||ln$loop_index||'  j.value: '||j.value);
            ln$median_value := j.value;
         end if;

         if ln$loop_index = ln$median_index + 1 then
            dbms_output.put_line('ln$loop_index+1: '||ln$loop_index||'  j.value: '||j.value);
            ln$median_value := (ln$median_value + j.value)/2;
            exit;            
         end if;         
      end if;
      ln$loop_index := ln$loop_index + 1;   
   end loop; 
   dbms_output.put_line('OUTPUT. ln$median_value: '||ln$median_value);  
   return ln$median_value;   
end get_median_value;

--[ function used to return the max value from an array of numbers
function get_max_value_from_tbl (p_numerical_value_tbl  olap_sys.tbl_numerical_value
                               , pn_rownum              number default 1) return number is
   LV$PROCEDURE_NAME         constant varchar2(30) := 'get_max_value_from_tbl';
   ln$median_index                    pls_integer  := 0;
   lb$pair_array_cnt                  boolean      := false;
   ln$median_value                    number       := 0;
   ln$loop_index                      pls_integer  := 1;
   cursor c_value (pt_type_name olap_sys.tbl_numerical_value
                 , pn_rownum    number) is
   with array_sorted as (
   select value
     from (table(cast(pt_type_name AS olap_sys.tbl_numerical_value)))
    order by value desc)
   select value
     from array_sorted
    where rownum <= pn_rownum;
begin
   if p_numerical_value_tbl.count > 0 then
      for k in c_value (pt_type_name => p_numerical_value_tbl
                      , pn_rownum    => pn_rownum) loop
--       dbms_output.put_line('max_factor: '||k.value);
          return k.value;            
      end loop;
   else
     return 0;
   end if;                    
end get_max_value_from_tbl;

--[ function used to sum all digits of a number
function get_digit_sum (pn_number   number) return number is
   LV$PROCEDURE_NAME         constant varchar2(30) := 'get_digit_sum';
   ln$digit_sum                       number := 0;
   lv$number                          varchar2(30) := to_char(pn_number);
begin
   select replace(decode(length(decode(length(lv$number),2,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))
                                       ,3,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))+to_number(substr(lv$number,3,1))
                                       ,1,pn_number
                    )),2,to_number(substr(to_char(decode(length(lv$number),2,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))
                                       ,3,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))+to_number(substr(lv$number,3,1))
                                       ,1,pn_number
             )),1,1))+to_number(substr(to_char(decode(length(lv$number),2,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))
                                       ,3,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))+to_number(substr(lv$number,3,1))
             )),2,1)),1,decode(length(lv$number),2,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))
                                       ,3,to_number(substr(lv$number,1,1))+to_number(substr(lv$number,2,1))+to_number(substr(lv$number,3,1))
                                       ,1,pn_number
             )
             ),10,1) xdd_sum
     into ln$digit_sum            
     from dual;
   return ln$digit_sum;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

    return 0; 
end get_digit_sum;

--[ function used to return an detailed equivalent sum value based on low-low, low-high, high-low and high-high values logic                                       
function convert_sum_low_high_dtl_f (pn_comb_sum          number) return number is
begin
      if pn_comb_sum <= 80  then return -5;
   elsif pn_comb_sum >= 81  and pn_comb_sum <= 90  then return -4;
   elsif pn_comb_sum >= 91  and pn_comb_sum <= 100 then return -3;
   elsif pn_comb_sum >= 101 and pn_comb_sum <= 110 then return -2;
   elsif pn_comb_sum >= 111 and pn_comb_sum <= 120 then return -1;
   elsif pn_comb_sum >= 121 and pn_comb_sum <= 130 then return 1;
   elsif pn_comb_sum >= 131 and pn_comb_sum <= 140 then return 2;
   elsif pn_comb_sum >= 141 and pn_comb_sum <= 150 then return 3;
   elsif pn_comb_sum >= 151 and pn_comb_sum <= 160 then return 4;
   elsif pn_comb_sum >= 161 then return 5;
   else return 0;
   end if;
end convert_sum_low_high_dtl_f;                                       

--[ procedure used to compute digit counts based on input parameter pn_days_interval in order to sort data located into DB type obj_w_combination_responses_fs  
procedure load_plsql_qry_stmt (pv_drawing_type                 olap_sys.c_query_stmts.drawing_type%type
                             , pv_package_name                 olap_sys.c_query_stmts.package_name%type default NULL
                             , pv_procedure_name               olap_sys.c_query_stmts.procedure_name%type default NULL
                             , pv_type                         olap_sys.c_query_stmts.type%type
                             , x_gt$qry_stmt_tbl in out NOCOPY olap_sys.w_common_pkg.gt$qry_stmt_tbl
                             , x_err_code        in out NOCOPY number
                             , x_err_msg         in out NOCOPY varchar2  
                              ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'load_plsql_qry_stmt';
begin
  dbms_output.put_line('-------------------------------------------------------------------');
--  dbms_output.put_line('PACKAGE NAME: '||GV$PACKAGE_NAME);
--  dbms_output.put_line('PROCEDURE NAME: '||LV$PROCEDURE_NAME);
  dbms_output.put_line('pv_package_name: '||upper(pv_package_name));
  dbms_output.put_line('pv_procedure_name: '||upper(pv_procedure_name));
  dbms_output.put_line('pv_type: '||upper(pv_type));
  
  --[ cleaning plsql table
  x_gt$qry_stmt_tbl.delete;
  
  select q.id
       , s.select_list "CNT"
       , m.dml_stmt "MASTER"
       , d.dml_stmt "DETAIL"
       , s.group_by "GROUP_BY"
       , s.order_by "ORDER_BY"
       , d.category
       , q.execution_order
       , (select ss.select_list from olap_sys.c_select_stmts ss where ss.drawing_type = s.drawing_type and ss.id = s.next_id) "LOAD_SELECT"
       , (select dd.dml_stmt from olap_sys.c_detail_where_stmts dd where dd.drawing_type = s.drawing_type and dd.id = d.next_id) "LOAD_DETAIL"
       , (select ss.group_by from olap_sys.c_select_stmts ss where ss.drawing_type = s.drawing_type and ss.id = s.next_id) "LOAD_GROUP"
       , (select ss.order_by from olap_sys.c_select_stmts ss where ss.drawing_type = s.drawing_type and ss.id = s.next_id) "LOAD_ORDER"
       , (select dd.category from olap_sys.c_detail_where_stmts dd where dd.drawing_type = s.drawing_type and dd.id = d.next_id) "LOAD_CATEGORY"
    bulk collect into x_gt$qry_stmt_tbl   
    from olap_sys.c_select_stmts s
       , olap_sys.c_master_where_stmts m
       , olap_sys.c_detail_where_stmts d
       , olap_sys.c_query_stmts q
   where q.drawing_type   = s.drawing_type
     and q.select_id      = s.id
     and s.status         = 'A'
     and q.drawing_type   = m.drawing_type
     and q.master_id      = m.id
     and m.status         = 'A'
     and q.drawing_type   = d.drawing_type   
     and q.detail_id      = d.id
     and d.status         = 'A'
     and q.status         = 'A'
     and q.drawing_type   = pv_drawing_type
     and q.package_name   = upper(pv_package_name) 
     and q.procedure_name = upper(pv_procedure_name) 
     and q.type           = upper(pv_type)
   order by q.execution_order;

--  dbms_output.put_line('OUTPUT: '||x_gt$qry_stmt_tbl.count||' rows loaded');
   
  x_err_code := OLAP_SYS.W_COMMON_PKG.GN$SUCCESSFUL_EXECUTION;
  x_err_msg  := LV$PROCEDURE_NAME||OLAP_SYS.W_COMMON_PKG.GV$SUCCESSFUL_EXECUTION; 
exception
  when others then
    x_err_code := sqlcode;
    x_err_msg  := sqlerrm;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    

    raise;
end load_plsql_qry_stmt;                               

--[ function used to find out if a digit is par or inpar
function get_par_f (pn_number  number) return number is
begin
  if mod(pn_number,2) = 0 then
    return 0;
  else
    return 1;
  end if;      		
end get_par_f;	

--[ function used to convert a row into column on a select statement
/*function row_to_column_f (p_refcur   olap_sys.w_common_pkg.g_refcur_dgtcnt) return olap_sys.tbl_drawings_digit_counts pipelined is
  out_rec  olap_sys.obj_drawings_digit_counts := olap_sys.obj_drawings_digit_counts(NULL --drawing_type
                                                                           ,NULL --sum_par_comb
                                                                           ,NULL --comb_sum    
                                                                           ,NULL --dgt          
                                                                           ,NULL --odds          
                                                                           ,NULL --row_count    
                                                                           ,NULL --usr_probability   
                                                                           );
  in_rec   p_refcur%ROWTYPE;
BEGIN

  LOOP
    FETCH p_refcur INTO in_rec;
    EXIT WHEN p_refcur%NOTFOUND;

    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 1;
    out_rec.digit_count    := in_rec.c1;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 2;
    out_rec.digit_count    := in_rec.c2;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 3;
    out_rec.digit_count    := in_rec.c3;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 4;
    out_rec.digit_count    := in_rec.c4;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 5;
    out_rec.digit_count    := in_rec.c5;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 6;
    out_rec.digit_count    := in_rec.c6;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 7;
    out_rec.digit_count    := in_rec.c7;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 8;
    out_rec.digit_count    := in_rec.c8;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 9;
    out_rec.digit_count    := in_rec.c9;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 10;
    out_rec.digit_count    := in_rec.c10;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 11;
    out_rec.digit_count    := in_rec.c11;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 12;
    out_rec.digit_count    := in_rec.c12;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 13;
    out_rec.digit_count    := in_rec.c13;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 14;
    out_rec.digit_count    := in_rec.c14;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 15;
    out_rec.digit_count    := in_rec.c15;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 16;
    out_rec.digit_count    := in_rec.c16;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 17;
    out_rec.digit_count    := in_rec.c17;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 18;
    out_rec.digit_count    := in_rec.c18;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 19;
    out_rec.digit_count    := in_rec.c19;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 20;
    out_rec.digit_count    := in_rec.c20;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 21;
    out_rec.digit_count    := in_rec.c21;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 22;
    out_rec.digit_count    := in_rec.c22;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 23;
    out_rec.digit_count    := in_rec.c23;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 24;
    out_rec.digit_count    := in_rec.c24;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 25;
    out_rec.digit_count    := in_rec.c25;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 26;
    out_rec.digit_count    := in_rec.c26;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 27;
    out_rec.digit_count    := in_rec.c27;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 28;
    out_rec.digit_count    := in_rec.c28;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 29;
    out_rec.digit_count    := in_rec.c29;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 30;
    out_rec.digit_count    := in_rec.c30;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 31;
    out_rec.digit_count    := in_rec.c31;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 32;
    out_rec.digit_count    := in_rec.c32;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 33;
    out_rec.digit_count    := in_rec.c33;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 34;
    out_rec.digit_count    := in_rec.c34;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 35;
    out_rec.digit_count    := in_rec.c35;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 36;
    out_rec.digit_count    := in_rec.c36;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 37;
    out_rec.digit_count    := in_rec.c37;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 38;
    out_rec.digit_count    := in_rec.c38;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
    out_rec.drawing_type   := in_rec.drawing_type;
    out_rec.sum_par_comb   := in_rec.sum_par_comb;
    out_rec.comb_sum       := in_rec.comb_sum;
    out_rec.digit          := 39;
    out_rec.digit_count    := in_rec.c39;
    out_rec.row_count      := in_rec.n_rows;
    PIPE ROW(out_rec);    
  END LOOP;
  CLOSE p_refcur;

  RETURN;	
end row_to_column_f; 
*/
--[ function use for returning a record from table c_day_range_factors based on input parameters
function get_day_range_factor (pv_drawing_type    olap_sys.c_day_range_factors.drawing_type%type
                             , pv_type            olap_sys.c_day_range_factors.type%type
                             , pn_day_range       number) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_day_range_factor';
  ln$factor                          olap_sys.c_day_range_factors.factor%type := 0;  
begin
--   dbms_output.put_line(LV$PROCEDURE_NAME); 

   select factor
     into ln$factor
     from olap_sys.c_day_range_factors
    where drawing_type = pv_drawing_type
      and type         = pv_type
      and pn_day_range between day_range_from and day_range_to;
           
--   dbms_output.put_line('ln$factor: '||ln$factor); 
   return ln$factor;        
exception
  when no_data_found then
     return 0;
end get_day_range_factor;



procedure get_central_pos_measures (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                          , pn_days_interval            number
	                          , pv_column_name              varchar2
                                  , xn_avg        in out NOCOPY number 
                                  , xn_median     in out NOCOPY number 
                                  , xn_count      in out NOCOPY number 
                                   ) is
begin
  select median(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb))
       , avg(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb))
       , count(1)
    into xn_median
       , xn_avg
       , xn_count
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval));
exception
  when no_data_found then
    xn_median := 0;
    xn_avg    := 0;		
end get_central_pos_measures;	                                

procedure get_no_central_pos_measures (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                             , pn_days_interval            number
	                             , pv_column_name              varchar2
                                     , xn_mode       in out NOCOPY number 
                                     , xn_rowcount   in out NOCOPY number 
                                      ) is
begin
  with mode_tbl as( 
  select decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb) xmode
       , count(1) xrowcount
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval))
   group by decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb)
   order by count(1) desc
  ) select xmode
         , xrowcount
      into xn_mode
         , xn_rowcount
      from mode_tbl
     where rownum = 1; 	

/*     
Li-1 es el límite inferior de la clase modal.
fi es la frecuencia absoluta de la clase modal.
fi--1 es la frecuencia absoluta inmediatamente inferior a la en clase modal.
fi-+1 es la frecuencia absoluta inmediatamente posterior a la clase modal.
ai es la amplitud de la clase.

En primer lugar tenemos que hallar las alturas.

hi=fi/ai

La clase modal es la que tiene mayor altura.

MO=Li+(hi-hi-1/((hi-hi-1)+(hi+hi+1)))*ai
*/     
exception
  when no_data_found then
    xn_mode     := -1;
    xn_rowcount := 0;
end get_no_central_pos_measures;	                             
                                
procedure get_dispersion_measures (pv_drawing_type              olap_sys.sl_gamblings.gambling_type%type 
	                         , pn_days_interval             number
	                         , pv_column_name               varchar2
                                 , xn_path        in out NOCOPY number 
                                 , xn_max         in out NOCOPY number 
                                 , xn_min         in out NOCOPY number 
                                 , xn_var         in out NOCOPY number 
                                 , xn_stddev      in out NOCOPY number 
                                  ) is
begin
  select max(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb)) - min(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb)) path
       , max(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb))
       , min(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb))
       , trunc(var_samp(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb)))
       , trunc(stddev_samp(decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb)))
    into xn_path  
       , xn_max
       , xn_min
       , xn_var
       , xn_stddev 
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval));

--[ PERCENTILE_DISC(:P_PERCENTILE_DISC) WITHIN GROUP (ORDER BY comb_sum DESC) AS quartile    
exception
  when no_data_found then
    xn_path   := 0;  
    xn_max    := 0;
    xn_min    := 0;
    xn_var    := 0;
    xn_stddev := 0; 	
end get_dispersion_measures;	                                                             

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
                             ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_shape_measures';
  ln$sum_power            number := 0;
  ln$sum_power_2          number := 0;
  ln$sum_power_total      number := 0;
  ln$factorial            number := 0;
  cursor c_drawings (pv_drawing_type                     olap_sys.sl_gamblings.gambling_type%type 
	           , pn_days_interval                    number) is
  select gambling_id
       , decode(pv_column_name,'comb_sum',comb_sum,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6,sum_par_comb) comb_sum
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval))
   order by gambling_id; 

begin 
--dbms_output.put_line('----------------------------');
--dbms_output.put_line(LV$PROCEDURE_NAME); 	
--dbms_output.put_line('pv_drawing_type: '||pv_drawing_type); 	
--dbms_output.put_line('pn_days_interval: '||pn_days_interval); 	
--dbms_output.put_line('pn_avg: '||pn_avg); 	
--dbms_output.put_line('pn_count: '||pn_count); 	
--dbms_output.put_line('pn_stddev: '||pn_stddev); 	

--dbms_output.put_line('computing asymm_coefficient'); 	
  --[ computing asymm_coefficient. 2nd portion of the formula
  --[ AS= n/(n-1)(n-2) * sum (power(xi-avg(X))/stddev,3)	
  for p in c_drawings (pv_drawing_type  => pv_drawing_type
	             , pn_days_interval => pn_days_interval) loop
      ln$sum_power := power((p.comb_sum-pn_avg)/pn_stddev,3);   
--dbms_output.put_line(p.gambling_id||'~'||p.comb_sum||' sum_power: '||ln$sum_power);      
      ln$sum_power_total := ln$sum_power_total + ln$sum_power; 	             
--dbms_output.put_line(ln$sum_power_total);      
  end loop;	             	             
  
  ln$factorial := pn_count/((pn_count-1)*(pn_count-2));
--dbms_output.put_line('ln$factorial: '||ln$factorial);      
  
  xn_asymm_coefficient := ln$factorial*ln$sum_power_total;

  --[ getting description for xn_asymm_coefficient 
  if xn_asymm_coefficient = 0 then
     xv_asymm_desc := 'Simetrica';	
  elsif xn_asymm_coefficient > 0 then	
     xv_asymm_desc := 'Sesgada a la izquierda';
  elsif xn_asymm_coefficient < 0 then
     xv_asymm_desc := 'Sesgada a la derecha';	
  end if;
  	
--dbms_output.put_line('computing curtosis'); 	
  --[ computing curtosis
  --[ curtosis = (n*(n+1)/(n-1)(n-2)(n-3) * sum (power(xi-avg(X))/stddev,4))*(3*power((n-1),2)/(n-1)(n-2))
  ln$sum_power       := 0;
  ln$sum_power_total := 0;
  for p in c_drawings (pv_drawing_type  => pv_drawing_type
	             , pn_days_interval => pn_days_interval) loop
      ln$sum_power := power((p.comb_sum-pn_avg)/pn_stddev,4);   
--dbms_output.put_line(p.gambling_id||'~'||p.comb_sum||' sum_power: '||ln$sum_power);        
      ln$sum_power_total := ln$sum_power_total + ln$sum_power; 	             
--dbms_output.put_line(ln$sum_power_total);    
  end loop;	             	             
  
  ln$factorial := pn_count*(pn_count+1)/((pn_count-1)*(pn_count-2)*(pn_count-3));
--dbms_output.put_line('ln$factorial: '||ln$factorial);    
  ln$sum_power_2 := 3*power((pn_count-1),2)/((pn_count-2)*(pn_count-3));
--dbms_output.put_line('ln$sum_power_2: '||ln$sum_power_2); 
  
  xn_custosis := (ln$factorial*ln$sum_power_total)-ln$sum_power_2;
--dbms_output.put_line('xn_custosis: '||xn_custosis);  

  --[ getting description for xn_asymm_coefficient 
  if xn_custosis = 0 then
     xv_custosis_desc := 'Mesocurtica';	
  elsif xn_custosis > 0 then	
     xv_custosis_desc := 'Leptocurtica';
  elsif xn_custosis < 0 then
     xv_custosis_desc := 'Platicurtica';	
  end if;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||dbms_utility.format_error_stack());    
    raise;    	  	
end get_shape_measures;	                             

procedure get_range_base (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type 
	                , pn_days_interval            number
	                , pn_rownum                   number default 7
	                , pv_column_name              varchar2
	                ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_range_base';
  cursor c_range_master (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type
                       , pn_days_interval            number) is
  with range_bases as(
  select olap_sys.w_common_pkg.get_range_base_4 (comb_sum) range_base
       , to_number(substr(olap_sys.w_common_pkg.get_range_base_4(comb_sum),1,instr(olap_sys.w_common_pkg.get_range_base_4(comb_sum),'-')-1)) xsort
       , count(1) xrowcount
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type 
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval))
    group by olap_sys.w_common_pkg.get_range_base_4 (comb_sum) 
    order by xrowcount desc, xsort
  )select range_base
        , xrowcount
     from range_bases
    where rownum <= pn_rownum; 
    
  cursor c_range_detail (pv_drawing_type             olap_sys.sl_gamblings.gambling_type%type
                       , pn_days_interval            number
                       , pv_range_base               varchar2) is     
  select sum_par_comb
       , count(1) xrowcount
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type 
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval))
     and olap_sys.w_common_pkg.get_range_base_4 (comb_sum) = pv_range_base
   group by sum_par_comb;                        
begin
   --[this procedure must be executed only when column_name is comb_sum due to this is the input value for function get_range_base_4 
   if pv_column_name = 'comb_sum' then	
      dbms_output.put_line('----------------------------');
      dbms_output.put_line('Range Base 4');       
      dbms_output.put_line('RANGE_BASE~XROWCOUNT');                                                                      
      for m in c_range_master (pv_drawing_type  => pv_drawing_type
                             , pn_days_interval => pn_days_interval) loop
          dbms_output.put_line('HDR~'||m.range_base||'~'||m.xrowcount);   
          dbms_output.put_line('#~SUM_PAR_COMB~XROWCOUNT'); 
          for d in c_range_detail (pv_drawing_type  => pv_drawing_type
                              , pn_days_interval => pn_days_interval
                              , pv_range_base    => m.range_base) loop
                        
              dbms_output.put_line('#~'||d.sum_par_comb||'~'||d.xrowcount);   
          end loop;              
                           
      end loop;
   end if;
end get_range_base;	  	                

function get_range_base_3 (pn_comb_sum         number) return varchar2 is
begin
     if pn_comb_sum >= 21 and pn_comb_sum <= 23 then return '21-23'; 
  elsif pn_comb_sum >= 24 and pn_comb_sum <= 26 then return '24-26'; 
  elsif pn_comb_sum >= 27 and pn_comb_sum <= 29 then return '27-29'; 
  elsif pn_comb_sum >= 30 and pn_comb_sum <= 32 then return '30-32'; 
  elsif pn_comb_sum >= 33 and pn_comb_sum <= 35 then return '33-35'; 	
  elsif pn_comb_sum >= 36 and pn_comb_sum <= 38 then return '36-38'; 	
  elsif pn_comb_sum >= 39 and pn_comb_sum <= 41 then return '39-41'; 	
  elsif pn_comb_sum >= 42 and pn_comb_sum <= 44 then return '42-44'; 	
  elsif pn_comb_sum >= 45 and pn_comb_sum <= 47 then return '45-57'; 	
  elsif pn_comb_sum >= 48 and pn_comb_sum <= 50 then return '48-50'; 	
  elsif pn_comb_sum >= 51 and pn_comb_sum <= 53 then return '51-53'; 	
  elsif pn_comb_sum >= 54 and pn_comb_sum <= 56 then return '54-56'; 	
  elsif pn_comb_sum >= 57 and pn_comb_sum <= 59 then return '57-59'; 	
  elsif pn_comb_sum >= 60 and pn_comb_sum <= 62 then return '60-62'; 	
  elsif pn_comb_sum >= 63 and pn_comb_sum <= 65 then return '63-65'; 	
  elsif pn_comb_sum >= 66 and pn_comb_sum <= 68 then return '66-68'; 	
  elsif pn_comb_sum >= 69 and pn_comb_sum <= 71 then return '69-71'; 	
  elsif pn_comb_sum >= 72 and pn_comb_sum <= 74 then return '72-74'; 	
  elsif pn_comb_sum >= 75 and pn_comb_sum <= 77 then return '75-77'; 	
  elsif pn_comb_sum >= 78 and pn_comb_sum <= 80 then return '78-80'; 	
  elsif pn_comb_sum >= 81 and pn_comb_sum <= 83 then return '81-83'; 	
  elsif pn_comb_sum >= 84 and pn_comb_sum <= 86 then return '84-86'; 	
  elsif pn_comb_sum >= 87 and pn_comb_sum <= 89 then return '87-89'; 	
  elsif pn_comb_sum >= 90 and pn_comb_sum <= 92 then return '90-92'; 	
  elsif pn_comb_sum >= 93 and pn_comb_sum <= 95 then return '93-95'; 	
  elsif pn_comb_sum >= 96 and pn_comb_sum <= 98 then return '96-98'; 	
  elsif pn_comb_sum >= 99 and pn_comb_sum <= 101 then return '99-101'; 	
  elsif pn_comb_sum >= 102 and pn_comb_sum <= 104 then return '102-104'; 	
  elsif pn_comb_sum >= 105 and pn_comb_sum <= 107 then return '105-107'; 	
  elsif pn_comb_sum >= 108 and pn_comb_sum <= 110 then return '108-110'; 	
  elsif pn_comb_sum >= 111 and pn_comb_sum <= 113 then return '111-113'; 	
  elsif pn_comb_sum >= 114 and pn_comb_sum <= 116 then return '114-116'; 	
  elsif pn_comb_sum >= 117 and pn_comb_sum <= 119 then return '117-119'; 	
  elsif pn_comb_sum >= 120 and pn_comb_sum <= 122 then return '120-122'; 	
  elsif pn_comb_sum >= 123 and pn_comb_sum <= 125 then return '123-125'; 	
  elsif pn_comb_sum >= 126 and pn_comb_sum <= 128 then return '126-128'; 	
  elsif pn_comb_sum >= 129 and pn_comb_sum <= 131 then return '129-131'; 	
  elsif pn_comb_sum >= 132 and pn_comb_sum <= 134 then return '132-134'; 	
  elsif pn_comb_sum >= 135 and pn_comb_sum <= 137 then return '135-137'; 	
  elsif pn_comb_sum >= 138 and pn_comb_sum <= 140 then return '138-140'; 	
  elsif pn_comb_sum >= 141 and pn_comb_sum <= 143 then return '141-143'; 	
  elsif pn_comb_sum >= 144 and pn_comb_sum <= 146 then return '144-146'; 	
  elsif pn_comb_sum >= 147 and pn_comb_sum <= 149 then return '147-149'; 	
  elsif pn_comb_sum >= 150 and pn_comb_sum <= 152 then return '150-152'; 	
  elsif pn_comb_sum >= 153 and pn_comb_sum <= 155 then return '153-155'; 	
  elsif pn_comb_sum >= 156 and pn_comb_sum <= 158 then return '156-158'; 	
  elsif pn_comb_sum >= 159 and pn_comb_sum <= 161 then return '159-161'; 	
  elsif pn_comb_sum >= 162 and pn_comb_sum <= 164 then return '162-164'; 	
  elsif pn_comb_sum >= 165 and pn_comb_sum <= 167 then return '165-167'; 	
  elsif pn_comb_sum >= 168 and pn_comb_sum <= 170 then return '168-170'; 	
  elsif pn_comb_sum >= 171 and pn_comb_sum <= 173 then return '171-173'; 	
  elsif pn_comb_sum >= 174 and pn_comb_sum <= 176 then return '174-176'; 	
  elsif pn_comb_sum >= 177 and pn_comb_sum <= 179 then return '177-179'; 	
  elsif pn_comb_sum >= 180 and pn_comb_sum <= 182 then return '180-182'; 	
  elsif pn_comb_sum >= 183 and pn_comb_sum <= 185 then return '183-185'; 	
  elsif pn_comb_sum >= 186 and pn_comb_sum <= 188 then return '186-188'; 	
  elsif pn_comb_sum >= 189 and pn_comb_sum <= 191 then return '189-191'; 	
  elsif pn_comb_sum >= 192 and pn_comb_sum <= 194 then return '192-194'; 	
  elsif pn_comb_sum >= 195 and pn_comb_sum <= 197 then return '195-197'; 	
  elsif pn_comb_sum >= 198 and pn_comb_sum <= 200 then return '198-200'; 	
  elsif pn_comb_sum >= 201 and pn_comb_sum <= 203 then return '201-203'; 	
  elsif pn_comb_sum >= 204 and pn_comb_sum <= 206 then return '204-206'; 	
  elsif pn_comb_sum >= 207 and pn_comb_sum <= 209 then return '207-209'; 	
  elsif pn_comb_sum >= 210 and pn_comb_sum <= 212 then return '210-212'; 	
  elsif pn_comb_sum >= 213 and pn_comb_sum <= 215 then return '213-215'; 	
  elsif pn_comb_sum >= 216 and pn_comb_sum <= 218 then return '216-218'; 	
  elsif pn_comb_sum >= 219 and pn_comb_sum <= 221 then return '219-221'; 	
  else return to_char(pn_comb_sum)||'-unknown';
  end if;
end get_range_base_3;

function get_range_base_4 (pn_comb_sum         number) return varchar2 is
begin
     if pn_comb_sum >= 21 and pn_comb_sum <= 24 then return '21-24'; 
  elsif pn_comb_sum >= 25 and pn_comb_sum <= 28 then return '25-28'; 
  elsif pn_comb_sum >= 29 and pn_comb_sum <= 32 then return '29-32'; 
  elsif pn_comb_sum >= 33 and pn_comb_sum <= 36 then return '33-36'; 
  elsif pn_comb_sum >= 37 and pn_comb_sum <= 40 then return '37-40'; 	
  elsif pn_comb_sum >= 41 and pn_comb_sum <= 44 then return '41-44'; 	
  elsif pn_comb_sum >= 45 and pn_comb_sum <= 48 then return '45-48'; 	
  elsif pn_comb_sum >= 49 and pn_comb_sum <= 52 then return '49-52'; 	
  elsif pn_comb_sum >= 53 and pn_comb_sum <= 56 then return '53-56'; 	
  elsif pn_comb_sum >= 57 and pn_comb_sum <= 60 then return '57-60'; 	
  elsif pn_comb_sum >= 61 and pn_comb_sum <= 64 then return '61-64'; 	
  elsif pn_comb_sum >= 65 and pn_comb_sum <= 68 then return '65-68'; 	
  elsif pn_comb_sum >= 69 and pn_comb_sum <= 72 then return '69-72'; 	
  elsif pn_comb_sum >= 73 and pn_comb_sum <= 76 then return '73-76'; 	
  elsif pn_comb_sum >= 77 and pn_comb_sum <= 80 then return '77-80'; 	
  elsif pn_comb_sum >= 81 and pn_comb_sum <= 84 then return '81-84'; 	
  elsif pn_comb_sum >= 85 and pn_comb_sum <= 88 then return '85-88'; 	
  elsif pn_comb_sum >= 89 and pn_comb_sum <= 92 then return '89-92'; 	
  elsif pn_comb_sum >= 93 and pn_comb_sum <= 96 then return '93-96'; 	
  elsif pn_comb_sum >= 97 and pn_comb_sum <= 100 then return '97-100'; 	
  elsif pn_comb_sum >= 101 and pn_comb_sum <= 104 then return '101-104'; 	
  elsif pn_comb_sum >= 105 and pn_comb_sum <= 108 then return '105-108'; 	
  elsif pn_comb_sum >= 109 and pn_comb_sum <= 112 then return '109-112'; 	
  elsif pn_comb_sum >= 113 and pn_comb_sum <= 116 then return '113-116'; 	
  elsif pn_comb_sum >= 117 and pn_comb_sum <= 120 then return '117-120'; 	
  elsif pn_comb_sum >= 121 and pn_comb_sum <= 124 then return '121-124'; 	
  elsif pn_comb_sum >= 125 and pn_comb_sum <= 128 then return '125-128'; 	
  elsif pn_comb_sum >= 129 and pn_comb_sum <= 132 then return '129-132'; 	
  elsif pn_comb_sum >= 133 and pn_comb_sum <= 136 then return '133-136'; 	
  elsif pn_comb_sum >= 137 and pn_comb_sum <= 140 then return '137-140'; 	
  elsif pn_comb_sum >= 141 and pn_comb_sum <= 144 then return '141-144'; 	
  elsif pn_comb_sum >= 145 and pn_comb_sum <= 148 then return '145-148'; 	
  elsif pn_comb_sum >= 149 and pn_comb_sum <= 152 then return '149-152'; 	
  elsif pn_comb_sum >= 153 and pn_comb_sum <= 156 then return '153-156'; 	
  elsif pn_comb_sum >= 157 and pn_comb_sum <= 160 then return '157-160'; 	
  elsif pn_comb_sum >= 161 and pn_comb_sum <= 164 then return '161-164'; 	
  elsif pn_comb_sum >= 165 and pn_comb_sum <= 168 then return '165-168'; 	
  elsif pn_comb_sum >= 169 and pn_comb_sum <= 172 then return '169-172'; 	
  elsif pn_comb_sum >= 173 and pn_comb_sum <= 176 then return '173-176'; 	
  elsif pn_comb_sum >= 177 and pn_comb_sum <= 180 then return '177-180'; 	
  elsif pn_comb_sum >= 181 and pn_comb_sum <= 184 then return '181-184'; 	
  elsif pn_comb_sum >= 185 and pn_comb_sum <= 188 then return '185-188'; 	
  elsif pn_comb_sum >= 189 and pn_comb_sum <= 192 then return '189-192'; 	
  elsif pn_comb_sum >= 193 and pn_comb_sum <= 196 then return '193-196'; 	
  elsif pn_comb_sum >= 197 and pn_comb_sum <= 200 then return '197-200'; 	
  elsif pn_comb_sum >= 201 and pn_comb_sum <= 204 then return '201-204'; 	
  elsif pn_comb_sum >= 205 and pn_comb_sum <= 208 then return '205-208'; 	
  elsif pn_comb_sum >= 209 and pn_comb_sum <= 212 then return '209-212'; 	
  elsif pn_comb_sum >= 213 and pn_comb_sum <= 216 then return '213-216'; 	
  elsif pn_comb_sum >= 217 and pn_comb_sum <= 220 then return '217-220'; 	
  else return to_char(pn_comb_sum)||'-unknown';
  end if;
end get_range_base_4;

function get_range_base_5 (pn_comb_sum         number) return varchar2 is
begin
     if pn_comb_sum >= 21 and pn_comb_sum <= 25 then return '21-25'; 
  elsif pn_comb_sum >= 26 and pn_comb_sum <= 30 then return '26-30'; 
  elsif pn_comb_sum >= 31 and pn_comb_sum <= 35 then return '31-35'; 
  elsif pn_comb_sum >= 31 and pn_comb_sum <= 35 then return '31-35'; 
  elsif pn_comb_sum >= 36 and pn_comb_sum <= 40 then return '36-40'; 	
  elsif pn_comb_sum >= 41 and pn_comb_sum <= 45 then return '41-45'; 	
  elsif pn_comb_sum >= 46 and pn_comb_sum <= 50 then return '46-50'; 	
  elsif pn_comb_sum >= 51 and pn_comb_sum <= 55 then return '51-55'; 	
  elsif pn_comb_sum >= 56 and pn_comb_sum <= 60 then return '56-60'; 	
  elsif pn_comb_sum >= 61 and pn_comb_sum <= 65 then return '61-65'; 	
  elsif pn_comb_sum >= 66 and pn_comb_sum <= 70 then return '66-70'; 	
  elsif pn_comb_sum >= 71 and pn_comb_sum <= 75 then return '71-75'; 	
  elsif pn_comb_sum >= 76 and pn_comb_sum <= 80 then return '76-80'; 	
  elsif pn_comb_sum >= 81 and pn_comb_sum <= 85 then return '81-85'; 	
  elsif pn_comb_sum >= 86 and pn_comb_sum <= 90 then return '86-90'; 	
  elsif pn_comb_sum >= 91 and pn_comb_sum <= 95 then return '91-95'; 	
  elsif pn_comb_sum >= 96 and pn_comb_sum <= 100 then return '96-100'; 	
  elsif pn_comb_sum >= 101 and pn_comb_sum <= 105 then return '101-105'; 	
  elsif pn_comb_sum >= 106 and pn_comb_sum <= 110 then return '106-110'; 	
  elsif pn_comb_sum >= 111 and pn_comb_sum <= 115 then return '111-115'; 	
  elsif pn_comb_sum >= 116 and pn_comb_sum <= 120 then return '116-120'; 	
  elsif pn_comb_sum >= 121 and pn_comb_sum <= 125 then return '121-125'; 	
  elsif pn_comb_sum >= 126 and pn_comb_sum <= 130 then return '126-130'; 	
  elsif pn_comb_sum >= 131 and pn_comb_sum <= 135 then return '131-135'; 	
  elsif pn_comb_sum >= 136 and pn_comb_sum <= 140 then return '136-140'; 	
  elsif pn_comb_sum >= 141 and pn_comb_sum <= 145 then return '141-145'; 	
  elsif pn_comb_sum >= 146 and pn_comb_sum <= 150 then return '146-150'; 	
  elsif pn_comb_sum >= 151 and pn_comb_sum <= 155 then return '151-155'; 	
  elsif pn_comb_sum >= 156 and pn_comb_sum <= 160 then return '156-160'; 	
  elsif pn_comb_sum >= 161 and pn_comb_sum <= 165 then return '161-165'; 	
  elsif pn_comb_sum >= 166 and pn_comb_sum <= 170 then return '166-170'; 	
  elsif pn_comb_sum >= 171 and pn_comb_sum <= 175 then return '171-175'; 	
  elsif pn_comb_sum >= 176 and pn_comb_sum <= 180 then return '176-180'; 	
  elsif pn_comb_sum >= 181 and pn_comb_sum <= 185 then return '181-185'; 	
  elsif pn_comb_sum >= 186 and pn_comb_sum <= 190 then return '186-190'; 	
  elsif pn_comb_sum >= 191 and pn_comb_sum <= 195 then return '191-195'; 	
  elsif pn_comb_sum >= 196 and pn_comb_sum <= 200 then return '196-200'; 	
  elsif pn_comb_sum >= 201 and pn_comb_sum <= 205 then return '201-205'; 	
  elsif pn_comb_sum >= 206 and pn_comb_sum <= 210 then return '206-210'; 	
  elsif pn_comb_sum >= 210 and pn_comb_sum <= 215 then return '210-215'; 	
  elsif pn_comb_sum >= 216 and pn_comb_sum <= 220 then return '216-220'; 	
  else return to_char(pn_comb_sum)||'-unknown';
  end if;
end get_range_base_5;

--[ function used to get digit count on table sl_gamblings based on digit number
function get_vendor_digit_count (pv_drawing_type                    olap_sys.sl_gamblings.gambling_type%type
                               , pn_days_interval                   number
                               , pv_column_name                     varchar2
                               , pn_digit_value                     number
                               ) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_vendor_digit_count';  
begin  
  olap_sys.w_common_pkg.g_rowcnt := 0;	
  select count(1) xrowcount
    into olap_sys.w_common_pkg.g_rowcnt
    from olap_sys.sl_gamblings
   where gambling_type = pv_drawing_type
     and to_date(gambling_date,'DD-MM-YYYY') >= decode(pn_days_interval, 0, to_date(gambling_date,'DD-MM-YYYY'), trunc(SYSDATE - pn_days_interval))
     and decode(pv_column_name,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6) = pn_digit_value;
  return olap_sys.w_common_pkg.g_rowcnt;
end get_vendor_digit_count;

--[ function used to get digit count on table sl_gamblings based on digit number
function get_user_digit_count (pv_drawing_type                    olap_sys.sl_gamblings.gambling_type%type
                             , pv_column_name                     varchar2
                             , pn_digit_value                     number
                              ) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_user_digit_count';  
begin  
  olap_sys.w_common_pkg.g_rowcnt := 0;	
  select count(1) xrowcount
    into olap_sys.w_common_pkg.g_rowcnt
    from olap_sys.w_combination_responses_fs
   where attribute3 = pv_drawing_type
     and decode(pv_column_name,'comb1',comb1,'comb2',comb2,'comb3',comb3,'comb4',comb4,'comb5',comb5,'comb6',comb6) = pn_digit_value;
  return olap_sys.w_common_pkg.g_rowcnt;
end get_user_digit_count;
   
function sum_digit (pn_digit  number) return number is
begin
  if    pn_digit in (1,10,19,28,37) then return 1;
  elsif pn_digit in (2,11,20,29,38) then return 2;
  elsif pn_digit in (3,12,21,30,39) then return 3;
  elsif pn_digit in (4,13,22,31) then return 4;
  elsif pn_digit in (5,14,23,32) then return 5;
  elsif pn_digit in (6,15,24,33) then return 6;
  elsif pn_digit in (7,16,25,34) then return 7;
  elsif pn_digit in (8,17,26,35) then return 8;
  elsif pn_digit in (9,18,27,36) then return 9;
  end if;
end sum_digit;

--[ function used for retrieving sort_by value from w_combination_responses_fs based on seq_id
function get_user_sort_by (pn_seq_id    number) return number is
begin
--   select sort_by
--     into olap_sys.w_common_pkg.g_column_value
--     from olap_sys.w_combination_responses_fs
--    where seq_id = pn_seq_id;
    
--   return olap_sys.w_common_pkg.g_column_value; 
return 0;
exception
  when no_data_found then
     return 0;
  when too_many_rows then
     return -1;	     	   		  
end get_user_sort_by;

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
                                ) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_vendor_comb_sum_cnt';  
begin

   g_dml_stmt := 'select count(1) from olap_sys.sl_gamblings sg where sg.gambling_type = '||chr(39)||pv_drawing_type||chr(39)||' and sg.comb_sum = '||pn_comb_sum;
   if pv_exclude_drawings_flag = 'Y' then
      g_dml_stmt := g_dml_stmt||' and not exists (select 1 from olap_sys.s_drawings_excluded de where de.drawing_type = sg.gambling_type and de.gambling_id = sg.gambling_id)';
   end if;
   g_dml_stmt := g_dml_stmt||' and sg.gambling_id > decode('||chr(39)||pv_read_full_table||chr(39)||','||chr(39)||'N'||chr(39)||',(select max(g.gambling_id)-'||pn_last_n_drawings||' from olap_sys.sl_gamblings g),'||chr(39)||'Y'||chr(39)||',0)'; 

   if pn_sum_par_comb is not null then
      g_dml_stmt := g_dml_stmt||' and sg.sum_par_comb = '||pn_sum_par_comb;
   end if;
   
   if pn_sum_mod_comb is not null then
      g_dml_stmt := g_dml_stmt||' and sg.sum_mod_comb = '||pn_sum_mod_comb;
   end if;

   if pn_sort_by_ini is not null and pn_sort_by_end is not null then
      g_dml_stmt := g_dml_stmt||' and sg.sort_by between '||pn_sort_by_ini||' and '||pn_sort_by_end;
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is not null 
  and pv_comb3_list is not null 
  and pv_comb4_list is not null 
  and pv_comb5_list is not null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (sg.comb1 in ('||pv_comb1_list||') or sg.comb2 in ('||pv_comb2_list||') or sg.comb3 in ('||pv_comb3_list||')';
      g_dml_stmt := g_dml_stmt||' or sg.comb4 in ('||pv_comb4_list||') or sg.comb5 in ('||pv_comb5_list||') or sg.comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is not null 
  and pv_comb4_list is not null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (sg.comb1 in ('||pv_comb1_list||') or sg.comb3 in ('||pv_comb3_list||')';
      g_dml_stmt := g_dml_stmt||' or sg.comb4 in ('||pv_comb4_list||') or sg.comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is null 
  and pv_comb4_list is not null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (sg.comb1 in ('||pv_comb1_list||')';
      g_dml_stmt := g_dml_stmt||' or sg.comb4 in ('||pv_comb4_list||') or sg.comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is null 
  and pv_comb4_list is null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (sg.comb1 in ('||pv_comb1_list||') or sg.comb6 in ('||pv_comb6_list||'))';
   end if;
--   dbms_output.put_line(substr(g_dml_stmt,1,255));
--   dbms_output.put_line(substr(g_dml_stmt,256,255));
   
   execute immediate g_dml_stmt into g_rowcnt;   
   return g_rowcnt;   	
exception
  when others then
     dbms_output.put_line(LV$PROCEDURE_NAME||' Others: '||sqlerrm);
     return -1;	   
end get_vendor_comb_sum_cnt;	                                   
                                   
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
                                ) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_vendor_comb_sum_cnt'; 
begin

   g_dml_stmt := 'select count(1) from olap_sys.w_combination_responses_fs where attribute3 = '||chr(39)||pv_drawing_type||chr(39)||' and comb_sum = '||pn_comb_sum;
   
   if pn_sum_par_comb is not null then
      g_dml_stmt := g_dml_stmt||' and sum_par_comb = '||pn_sum_par_comb;
   end if;
   
   if pn_sum_mod_comb is not null then
      g_dml_stmt := g_dml_stmt||' and mod3_sum = '||pn_sum_mod_comb;
   end if;

   if pn_sort_by_ini is not null and pn_sort_by_end is not null then
      g_dml_stmt := g_dml_stmt||' and sort_by between '||pn_sort_by_ini||' and '||pn_sort_by_end;
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is not null 
  and pv_comb3_list is not null 
  and pv_comb4_list is not null 
  and pv_comb5_list is not null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (comb1 in ('||pv_comb1_list||') or comb2 in ('||pv_comb2_list||') or comb3 in ('||pv_comb3_list||')';
      g_dml_stmt := g_dml_stmt||' or comb4 in ('||pv_comb4_list||') or comb5 in ('||pv_comb5_list||') or comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is not null 
  and pv_comb4_list is not null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (comb1 in ('||pv_comb1_list||') or comb3 in ('||pv_comb3_list||')';
      g_dml_stmt := g_dml_stmt||' or comb4 in ('||pv_comb4_list||') or comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is null 
  and pv_comb4_list is not null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (comb1 in ('||pv_comb1_list||')';
      g_dml_stmt := g_dml_stmt||' or comb4 in ('||pv_comb4_list||') or comb6 in ('||pv_comb6_list||'))';
   end if;

   if pv_comb1_list is not null 
  and pv_comb2_list is null 
  and pv_comb3_list is null 
  and pv_comb4_list is null 
  and pv_comb5_list is null 
  and pv_comb6_list is not null 
  then
      g_dml_stmt := g_dml_stmt||' and (comb1 in ('||pv_comb1_list||') or comb6 in ('||pv_comb6_list||'))';
   end if;
--   dbms_output.put_line(substr(g_dml_stmt,1,255));
--   dbms_output.put_line(substr(g_dml_stmt,256,255));
   
   execute immediate g_dml_stmt into g_rowcnt;   

/*
   --[ procedure used to save a detailed query built in order to get vendor drawaings for users
   save_dml_statements_log(pv_drawing_type     => pv_drawing_type
                         , pn_setup_id         => pn_setup_id
                         , pv_user_id          => pv_user_id
                         , pn_next_drawing_id  => olap_sys.w_common_pkg.get_next_drawing_id (p_gambling_type => pv_drawing_type, p_setup_id => pn_setup_id)
                         , pv_execution_type   => 'COUNT'
                         , pv_digit_flag       => pv_digit_flag
                         , pv_dml_operator     => pv_dml_operator
                         , pclob_dml_stmt      => to_clob(olap_sys.w_common_pkg.gv$primary_qry_stmt)
                         , pv_dml_type         => LV$DML_DETAILED
                         , x_err_code          => x_err_code
                         , x_err_msg           => x_err_msg
                          );   

*/   
   return g_rowcnt;   	
exception
  when others then
     dbms_output.put_line(LV$PROCEDURE_NAME||' Others: '||sqlerrm);
     return -1;	   
end get_user_comb_sum_cnt;	                                   

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
                           , pv_attribute99       varchar2 default null) is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'ins_computed_value';  
begin
  insert into olap_sys.tmp_computed_values(attribute1 
                                         , attribute2 
                                         , attribute3 
                                         , attribute4 
                                         , attribute5 
                                         , attribute6 
                                         , attribute7 
                                         , attribute8 
                                         , attribute9 
                                         , attribute10
                                         , attribute11
                                         , attribute12
                                         , attribute13
                                         , attribute14
                                         , attribute15
                                         , attribute16
                                         , attribute17
                                         , attribute18
                                         , attribute19
                                         , attribute20
                                         , attribute95
                                         , attribute96
                                         , attribute97
                                         , attribute98
                                         , attribute99)
  values (pn_attribute1 
        , pn_attribute2 
        , pn_attribute3 
        , pn_attribute4 
        , pn_attribute5 
        , pn_attribute6 
        , pn_attribute7 
        , pn_attribute8 
        , pn_attribute9 
        , pn_attribute10
        , pn_attribute11
        , pn_attribute12
        , pn_attribute13
        , pn_attribute14
        , pn_attribute15
        , pn_attribute16
        , pn_attribute17
        , pn_attribute18
        , pn_attribute19
        , pn_attribute20
        , pv_attribute95
        , pv_attribute96
        , pd_attribute97
        , pd_attribute98
        , pv_attribute99
        );

exception 
  when others then                                         
     dbms_output.put_line(LV$PROCEDURE_NAME||'. '||sqlerrm);
     raise;	
end ins_computed_value;	 

--[ procedure used for inserting computed values into temporal table based on a plsq table 
procedure ins_computed_value(p_tmp_tbl    olap_sys.w_common_pkg.gt$tmp_rec) is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'ins_computed_value';   
begin
  if p_tmp_tbl.count > 0 then
     for k in p_tmp_tbl.first..p_tmp_tbl.last loop
         ins_computed_value(pn_attribute1  => p_tmp_tbl(k).attribute1 
                          , pn_attribute2  => p_tmp_tbl(k).attribute2 
                          , pn_attribute3  => p_tmp_tbl(k).attribute3 
                          , pn_attribute4  => p_tmp_tbl(k).attribute4 
                          , pn_attribute5  => p_tmp_tbl(k).attribute5 
                          , pn_attribute6  => p_tmp_tbl(k).attribute6 
                          , pn_attribute7  => p_tmp_tbl(k).attribute7 
                          , pn_attribute8  => p_tmp_tbl(k).attribute8 
                          , pn_attribute9  => p_tmp_tbl(k).attribute9 
                          , pn_attribute10 => p_tmp_tbl(k).attribute10
                          , pn_attribute11 => p_tmp_tbl(k).attribute11
                          , pn_attribute12 => p_tmp_tbl(k).attribute12
                          , pn_attribute13 => p_tmp_tbl(k).attribute13
                          , pn_attribute14 => p_tmp_tbl(k).attribute14
                          , pn_attribute15 => p_tmp_tbl(k).attribute15
                          , pn_attribute16 => p_tmp_tbl(k).attribute16
                          , pn_attribute17 => p_tmp_tbl(k).attribute17
                          , pn_attribute18 => p_tmp_tbl(k).attribute18
                          , pn_attribute19 => p_tmp_tbl(k).attribute19
                          , pn_attribute20 => p_tmp_tbl(k).attribute20
                          , pv_attribute95 => p_tmp_tbl(k).attribute95
                          , pv_attribute96 => p_tmp_tbl(k).attribute96
                          , pd_attribute97 => p_tmp_tbl(k).attribute97                          
                          , pd_attribute98 => p_tmp_tbl(k).attribute98
                          , pv_attribute99 => p_tmp_tbl(k).attribute99
                           );
     end loop;	
  end if;		
end ins_computed_value;

--[ funtion used to retrieve current value from sequence olap_sys.tmp_computed_value_seq in order to save data into table olap_sys.tmp_computed_values
function tmp_computed_value_seq return number is
begin
  select olap_sys.tmp_computed_value_seq.nextval into g_rowcnt from dual;
  return g_rowcnt;	
end tmp_computed_value_seq;

function get_sort_by_range (pv_drawing_type    varchar2
                          , pn_comb_sum        number
                          , pn_last_n_drawings number default 214
                          , pn_pct             number default 0.01
                          , pn_quarter         number) return varchar2 is
  lv$output                   varchar2(100) := '{CS}|{LR}|{LOW_CNT}|{AVG}|{HR}|{HIGH_CNT}|{P_LAST}';
  ln$lr_sort_by               number := 0;
  ln$hr_sort_by               number := 0;
  ln$low_cnt                  number := 0;
  ln$high_cnt                 number := 0;
  lb$replace_lr               boolean := true;
  lb$replace_hr               boolean := true;
  lt$sort_by_tbl              gt$sort_by_tbl;
  
begin
   
      with sort_by_stats as (
    select nvl(round(avg(sort_by)),0) avg_sort_by
         , nvl(round(stddev_samp(sort_by)),0) stddev_sort_by
         , abs(round((nvl(avg(sort_by),0) - nvl(stddev_samp(sort_by),0)) - (stddev_samp(sort_by) * pn_pct))) lr_sort_by
         , round((nvl(avg(sort_by),0) + nvl(stddev_samp(sort_by),0)) + (stddev_samp(sort_by) * pn_pct)) hr_sort_by
      from olap_sys.sl_gamblings
     where gambling_type = pv_drawing_type
       and comb_sum      = pn_comb_sum
       and gambling_id > decode(pn_last_n_drawings,0,pn_last_n_drawings,(select max(g.gambling_id)-pn_last_n_drawings from olap_sys.sl_gamblings g)) 
       and decode(pn_quarter,0,0,to_number(to_char(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_quarter_date_format))) = decode(pn_quarter,0,0,pn_quarter))
    select sort_by
         , avg_sort_by
         , stddev_sort_by
         , lr_sort_by
         , hr_sort_by
     bulk collect into lt$sort_by_tbl
     from olap_sys.sl_gamblings
        , sort_by_stats
     where gambling_type = pv_drawing_type
       and comb_sum      = pn_comb_sum
       and gambling_id > decode(pn_last_n_drawings,0,pn_last_n_drawings,(select max(g.gambling_id)-pn_last_n_drawings from olap_sys.sl_gamblings g))
     order by sort_by;       

  if lt$sort_by_tbl.count > 0 then
     for p in lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
         if lt$sort_by_tbl(p).sort_by> lt$sort_by_tbl(p).lr_sort_by and lt$sort_by_tbl(p).sort_by <= lt$sort_by_tbl(p).avg_sort_by then
      	    if lb$replace_lr then
     dbms_output.put_line('if. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
      	       lv$output := replace(lv$output,'{LR}',to_char(lt$sort_by_tbl(p).lr_sort_by));
      	       lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
               lb$replace_lr := false;
            end if;   
            ln$low_cnt := ln$low_cnt + 1;
         end if;
     end loop;
     
     if ln$low_cnt = 0 then
        for p in lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
        dbms_output.put_line('else. '||lt$sort_by_tbl(p).sort_by);
            if lt$sort_by_tbl(p).stddev_sort_by != 0 then
               ln$lr_sort_by := abs(round(lt$sort_by_tbl(p).avg_sort_by - (lt$sort_by_tbl(p).stddev_sort_by*pn_pct)));
            else
               ln$lr_sort_by := abs(round(lt$sort_by_tbl(p).avg_sort_by - (lt$sort_by_tbl(p).avg_sort_by*pn_pct)));
            end if;
            lv$output := replace(lv$output,'{LR}',to_char(ln$lr_sort_by));
            lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
            exit;
        end loop;     	
     end if;	
  end if;	

  dbms_output.put_line('---------------');
  if lt$sort_by_tbl.count > 0 then
     for p in reverse lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
         if lt$sort_by_tbl(p).sort_by< lt$sort_by_tbl(p).hr_sort_by and lt$sort_by_tbl(p).sort_by > lt$sort_by_tbl(p).avg_sort_by then
      	    if lb$replace_hr then
     dbms_output.put_line('if. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
      	       lv$output := replace(lv$output,'{HR}',to_char(lt$sort_by_tbl(p).hr_sort_by));
      	       lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));    	       
               lb$replace_hr := false;
            end if;  
            ln$high_cnt := ln$high_cnt + 1;       
         end if;	 
     end loop;
     
     if ln$high_cnt = 0 then
        for p in reverse lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
        dbms_output.put_line('else. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
            if lt$sort_by_tbl(p).stddev_sort_by != 0 then
               ln$hr_sort_by := round(lt$sort_by_tbl(p).avg_sort_by + (lt$sort_by_tbl(p).stddev_sort_by*pn_pct)); 
            else
               ln$hr_sort_by := round(lt$sort_by_tbl(p).avg_sort_by + (lt$sort_by_tbl(p).avg_sort_by*pn_pct)); 
            end if;
            lv$output := replace(lv$output,'{HR}',to_char(ln$hr_sort_by));
            lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
--            exit;	 
        end loop;     	
     end if;	
  end if;	

  dbms_output.put_line(ln$low_cnt||'~'||ln$high_cnt);
  lv$output := replace(lv$output,'{LOW_CNT}',to_char(ln$low_cnt));
  lv$output := replace(lv$output,'{HIGH_CNT}',to_char(ln$high_cnt));
  lv$output := replace(lv$output,'{CS}',to_char(pn_comb_sum));
  lv$output := replace(lv$output,'{P_LAST}',to_char(pn_last_n_drawings));
  dbms_output.put_line(lv$output);
  lt$sort_by_tbl.delete;
  return lv$output;                   
end get_sort_by_range;	

function get_sort_by_month (pv_drawing_type    varchar2
                          , pn_comb_sum        number
                          , pn_pct             number default 0.01
                          , pn_mon_ini         number
                          , pn_mon_end         number) return varchar2 is
  lv$output                   varchar2(100) := '{CS}|{LR}|{LOW_CNT}|{AVG}|{HR}|{HIGH_CNT}|{MON_INI}|{MON_END}';
  ln$lr_sort_by               number := 0;
  ln$hr_sort_by               number := 0;
  ln$low_cnt                  number := 0;
  ln$high_cnt                 number := 0;
  lb$replace_lr               boolean := true;
  lb$replace_hr               boolean := true;
  lt$sort_by_tbl              gt$sort_by_tbl;
  
begin
   
    with sort_by_stats as (
    select nvl(round(avg(sort_by)),0) avg_sort_by
         , nvl(round(stddev_samp(sort_by)),0) stddev_sort_by
         , abs(round((nvl(avg(sort_by),0) - nvl(stddev_samp(sort_by),0)) - (stddev_samp(sort_by) * pn_pct))) lr_sort_by
         , round((nvl(avg(sort_by),0) + nvl(stddev_samp(sort_by),0)) + (stddev_samp(sort_by) * pn_pct)) hr_sort_by
      from olap_sys.sl_gamblings
     where gambling_type = pv_drawing_type
       and comb_sum      = pn_comb_sum
       and to_number(to_char(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format)) >= pn_mon_ini
       and to_number(to_char(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format)) <= pn_mon_end)
    select sort_by
         , avg_sort_by
         , stddev_sort_by
         , lr_sort_by
         , hr_sort_by
     bulk collect into lt$sort_by_tbl
     from olap_sys.sl_gamblings
        , sort_by_stats
     where gambling_type = pv_drawing_type
       and comb_sum      = pn_comb_sum
       and to_number(to_char(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format)) >= pn_mon_ini
       and to_number(to_char(to_date(gambling_date,olap_sys.w_common_pkg.g_date_format),olap_sys.w_common_pkg.g_month_date_format)) <= pn_mon_end
     order by sort_by;       

  if lt$sort_by_tbl.count > 0 then
     for p in lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
         if lt$sort_by_tbl(p).sort_by> lt$sort_by_tbl(p).lr_sort_by and lt$sort_by_tbl(p).sort_by <= lt$sort_by_tbl(p).avg_sort_by then
      	    if lb$replace_lr then
     dbms_output.put_line('if. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
      	       lv$output := replace(lv$output,'{LR}',to_char(lt$sort_by_tbl(p).lr_sort_by));
      	       lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
               lb$replace_lr := false;
            end if;   
            ln$low_cnt := ln$low_cnt + 1;
         end if;
     end loop;
     
     if ln$low_cnt = 0 then
        for p in lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
        dbms_output.put_line('else. '||lt$sort_by_tbl(p).sort_by);
            if lt$sort_by_tbl(p).stddev_sort_by != 0 then
               ln$lr_sort_by := abs(round(lt$sort_by_tbl(p).avg_sort_by - (lt$sort_by_tbl(p).stddev_sort_by*pn_pct)));
            else
               ln$lr_sort_by := abs(round(lt$sort_by_tbl(p).avg_sort_by - (lt$sort_by_tbl(p).avg_sort_by*pn_pct)));
            end if;
            lv$output := replace(lv$output,'{LR}',to_char(ln$lr_sort_by));
            lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
            exit;
        end loop;     	
     end if;	
  end if;	

  dbms_output.put_line('---------------');
  if lt$sort_by_tbl.count > 0 then
     for p in reverse lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
         if lt$sort_by_tbl(p).sort_by< lt$sort_by_tbl(p).hr_sort_by and lt$sort_by_tbl(p).sort_by > lt$sort_by_tbl(p).avg_sort_by then
      	    if lb$replace_hr then
     dbms_output.put_line('if. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
      	       lv$output := replace(lv$output,'{HR}',to_char(lt$sort_by_tbl(p).hr_sort_by));
      	       lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));    	       
               lb$replace_hr := false;
            end if;  
            ln$high_cnt := ln$high_cnt + 1;       
         end if;	 
     end loop;
     
     if ln$high_cnt = 0 then
        for p in reverse lt$sort_by_tbl.first..lt$sort_by_tbl.last	loop
        dbms_output.put_line('else. '||lt$sort_by_tbl(p).sort_by||'~'||lt$sort_by_tbl(p).avg_sort_by||'~'||lt$sort_by_tbl(p).lr_sort_by||'~'||lt$sort_by_tbl(p).hr_sort_by); 
            if lt$sort_by_tbl(p).stddev_sort_by != 0 then
               ln$hr_sort_by := round(lt$sort_by_tbl(p).avg_sort_by + (lt$sort_by_tbl(p).stddev_sort_by*pn_pct)); 
            else
               ln$hr_sort_by := round(lt$sort_by_tbl(p).avg_sort_by + (lt$sort_by_tbl(p).avg_sort_by*pn_pct)); 
            end if;
            lv$output := replace(lv$output,'{HR}',to_char(ln$hr_sort_by));
            lv$output := replace(lv$output,'{AVG}',to_char(lt$sort_by_tbl(p).avg_sort_by));  
--            exit;	 
        end loop;     	
     end if;	
  end if;	

  dbms_output.put_line(ln$low_cnt||'~'||ln$high_cnt);
  lv$output := replace(lv$output,'{LOW_CNT}',to_char(ln$low_cnt));
  lv$output := replace(lv$output,'{HIGH_CNT}',to_char(ln$high_cnt));
  lv$output := replace(lv$output,'{CS}',to_char(pn_comb_sum));
  lv$output := replace(lv$output,'{MON_INI}',pn_mon_ini);
  lv$output := replace(lv$output,'{MON_END}',pn_mon_end);
  
  dbms_output.put_line(lv$output);
  lt$sort_by_tbl.delete;
  return lv$output;                   
end get_sort_by_month;

function get_next_drawing_id (p_gambling_type      olap_sys.w_comb_setup_header_fs.attribute3%type
                            , p_setup_id           olap_sys.w_comb_setup_header_fs.setup_id%type) return number is
begin
  select next_drawing_id
    into g_data_found
    from olap_sys.w_comb_setup_header_fs
   where attribute3 = p_gambling_type
     and setup_id   = p_setup_id;
  
  return g_data_found;
exception  
  when no_data_found then
    return -1;	      	
end get_next_drawing_id;

--[ function used to return user global index based on seq_id
function get_usr_global_index (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                             , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number is
begin
  select cr.global_index
    into olap_sys.w_common_pkg.g_column_value
    from olap_sys.w_combination_responses_fs cr
   where cr.attribute3 = pv_drawing_type
     and cr.seq_id     = pn_seq_id; 	
  
  return olap_sys.w_common_pkg.g_column_value;
exception
  when no_data_found then
     return 0;	
end get_usr_global_index;	                             

--[ function used to return user global index pct based on seq_id
function get_usr_global_index_pct (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                                 , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number is
begin
--  select cr.global_index_pct
--    into olap_sys.w_common_pkg.g_column_value
--    from olap_sys.w_combination_responses_fs cr
--   where cr.attribute3 = pv_drawing_type
--     and cr.seq_id     = pn_seq_id; 	
  
--  return olap_sys.w_common_pkg.g_column_value;
return 0;
exception
  when no_data_found then
     return 0;	
end get_usr_global_index_pct;

--[ function used to return user elegible flag based on seq_id
function get_usr_elegible_flag (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                              , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return varchar2 is
--  lv$column_value  olap_sys.w_combination_responses_fs.elegible_flag%type;
begin
--  select cr.elegible_flag
--    into lv$column_value
--    from olap_sys.w_combination_responses_fs cr
--   where cr.attribute3 = pv_drawing_type
--     and cr.seq_id     = pn_seq_id; 	
  
--  return lv$column_value;
return null;
exception
  when no_data_found then
     return null;	
end get_usr_elegible_flag;

--[ function used to return user elegible counter based on seq_id
function get_usr_elegible_cnt (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                             , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type) return number is
--  lv$column_value  olap_sys.w_combination_responses_fs.elegible_flag%type;
begin
--  select cr.elegible_cnt
--    into olap_sys.w_common_pkg.g_column_value
--    from olap_sys.w_combination_responses_fs cr
--   where cr.attribute3 = pv_drawing_type
--     and cr.seq_id     = pn_seq_id; 	
  
--  return olap_sys.w_common_pkg.g_column_value;
return 0;
exception
  when no_data_found then
     return 0;	
end get_usr_elegible_cnt;

--[ function used to randomly retrieve an ID based on a sum_par_comb input parameter
function get_random_usr_mod_comb_filter (pv_drawing_type   olap_sys.c_usr_mod_comb_filters.drawing_type%type
                                       , pn_sum_par_comb   olap_sys.c_usr_mod_comb_filters.sum_par_comb%type
                                        ) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'get_random_usr_mod_comb_filter';  
begin
   dbms_output.put_line('------------------------------------------');	
   dbms_output.put_line(LV$PROCEDURE_NAME);
   dbms_output.put_line('pn_sum_par_comb: '||pn_sum_par_comb);
   with c_usr_mod_comb_filters_tbl as (
   select id
     from olap_sys.c_usr_mod_comb_filters
    where drawing_type = pv_drawing_type
      and status       = 'A'
      and sum_par_comb = pn_sum_par_comb
   order by dbms_random.value
   ) select id 
       into g_column_value
       from c_usr_mod_comb_filters_tbl
    where rownum = 1; 
   dbms_output.put_line('debug_id: '||g_column_value); 
   dbms_output.put_line('------------------------------------------'); 
   return g_column_value;
exception
  when no_data_found then
     return 0;	    
end get_random_usr_mod_comb_filter;	

function convert_sum_par_comb (pv_cond_sum_par_comb  varchar2) return number is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'convert_sum_par_comb';  
begin
   dbms_output.put_line('------------------------------------------');	
   dbms_output.put_line(LV$PROCEDURE_NAME);
   dbms_output.put_line('pv_cond_sum_par_comb: '||pv_cond_sum_par_comb);
   --[ reading sum_par_comb from metadata stored into table r_drawing_comb_sum_details
   g_column_value := replace(replace(replace(replace(upper(trim(pv_cond_sum_par_comb)),'IN',null),'(',null),')',null),'SUM_PAR_COMB',null);
   dbms_output.put_line('converted sum_par_comb: '||g_column_value);
   return g_column_value;
exception
  when value_error then
     dbms_output.put_line('value_error: '||sqlerrm);
     return 0;
end convert_sum_par_comb;	


--[ procedure to do commit
procedure do_commit(pn_index   number default 0) is
  LV$PROCEDURE_NAME         constant varchar2(30) := 'do_commit';  
begin
  dbms_output.put_line(LV$PROCEDURE_NAME);
  if pn_index = 0 then
     commit;
  else
    if mod(pn_index,GN$DO_COMMIT) = 0 then
       commit;
    end if;   	
  end if;   		
end do_commit;

--[function used for verifying if the drawing exists on table s_gigamelate_stats
--[will return Y if the drawing is found. Otherwise, will return N
function get_existing_gigamelate (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                                , pn_gambling_id                 number
                                , pn_comb1                       number
                                , pn_comb2                       number
                                , pn_comb3                       number
                                , pn_comb4                       number
                                , pn_comb5                       number
                                , pn_comb6                       number) return number is
  ln$drawing_fount_cnt      number := 0;                              
begin
  dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);	
  olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) from olap_sys.s_gigamelate_stats where drawing_type = :1 and drawing_id = :2 and winner_flag = :3';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and b_type = :4 and digit = :5';

--  olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) from olap_sys.s_gigamelate_stats where drawing_type = '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id = '||pn_gambling_id||' and winner_flag = '||chr(39)||'Y'||chr(39);
--  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and b_type = :4 and digit = :5';

  olap_sys.w_common_pkg.g_rowcnt   := 0;
 
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B1'
       	                                                                                              , pn_comb1;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B1: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb1: '||pn_comb1);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B2'
       	                                                                                              , pn_comb2;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B2: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb2: '||pn_comb2);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B3'
       	                                                                                              , pn_comb3;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B3: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb3: '||pn_comb3);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B4'
       	                                                                                              , pn_comb4;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B4: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb4: '||pn_comb4);
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B5'
       	                                                                                              , pn_comb5;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B5: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb5: '||pn_comb5);
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B6'
       	                                                                                              , pn_comb6;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B6: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb6: '||pn_comb6);

      dbms_output.put_line('ln$drawing_fount_cnt: '||ln$drawing_fount_cnt);
  
  return ln$drawing_fount_cnt;
end get_existing_gigamelate;

--[ function used to retrieve total drawings per day based on all active players by gambling day
FUNCTION get_sum_drawings_per_day (pv_drawing_type  	       olap_sys.sl_gamblings.gambling_type%TYPE
                                 , pv_gambling_day             VARCHAR2
                                  ) RETURN NUMBER IS
BEGIN
  select sum(dd.drawings_per_day)
    INTO g_rowcnt
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
     and u.stop_login_date is NULL;
  dbms_output.put_line('drawings_per_day: '||g_rowcnt);     
  RETURN g_rowcnt;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RETURN 0;		      	
END get_sum_drawings_per_day;

--[ function used to return user m4_comb1..m4_comb6 d on seq_id
function get_usr_m4_comb (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                        , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type
                        , pv_column_name     varchar2) return number is
begin
--  select decode(pv_column_name,'COMB1',m4_comb1,'COMB2',m4_comb2,'COMB3',m4_comb3,'COMB4',m4_comb4,'COMB5',m4_comb5,'COMB6',m4_comb6) 
--    into olap_sys.w_common_pkg.g_column_value
--    from olap_sys.w_combination_responses_fs cr
--   where cr.attribute3 = pv_drawing_type
--     and cr.seq_id     = pn_seq_id; 	
  
--  return olap_sys.w_common_pkg.g_column_value;
return 0;
exception
  when no_data_found then
     return -1;	
end get_usr_m4_comb;
                            
--[ function used to return user level_comb1..level_comb6 based on seq_id
function get_usr_level_comb (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type
                           , pn_seq_id          olap_sys.w_combination_responses_fs.seq_id%type
                           , pv_column_name     varchar2) return varchar2 is
--  lv$level_comb         olap_sys.w_combination_responses_fs.level_comb1%type;                     
begin
--  select decode(pv_column_name,'COMB1',level_comb1,'COMB2',level_comb2,'COMB3',level_comb3,'COMB4',level_comb4,'COMB5',level_comb5,'COMB6',level_comb6) 
--    into lv$level_comb
--    from olap_sys.w_combination_responses_fs cr
--   where cr.attribute3 = pv_drawing_type
--     and cr.seq_id     = pn_seq_id; 	
  
--  return lv$level_comb;
return null;
exception
  when no_data_found then
     return NULL;	
end get_usr_level_comb;

function get_main_range (pn_comb_sum         number) return varchar2 is
begin
     if pn_comb_sum <= 81 then return '21-81'; 
  elsif pn_comb_sum >= 82 and pn_comb_sum <= 94 then return '82-94'; 
  elsif pn_comb_sum >= 95 and pn_comb_sum <= 107 then return '95-107'; 
  elsif pn_comb_sum >= 108 and pn_comb_sum <= 120 then return '108-120'; 
  elsif pn_comb_sum >= 121 and pn_comb_sum <= 133 then return '121-133';
  elsif pn_comb_sum >= 134 and pn_comb_sum <= 146 then return '134-146';
  elsif pn_comb_sum >= 147 then return '147-219'; 
  end if;
  	
end get_main_range;

function get_gigamelate_range (pv_drawing_type    olap_sys.w_combination_responses_fs.attribute3%type) return number is
  lv$month        varchar2(2);
  ln$year         number(4);
  ln$gambling_id  olap_sys.sl_gamblings.gambling_id%type;
begin

   select substr(gambling_date,instr(gambling_date,'-',1,1)+1,2) mm
        , to_number(substr(gambling_date,instr(gambling_date,'-',2,2)+1))-2 yyyy
     into lv$month
        , ln$year   
     from (
   select gambling_date
        , RANK() OVER (PARTITION BY gambling_type ORDER BY gambling_id desc) rank
     from olap_sys.sl_gamblings g
    WHERE gambling_type = pv_drawing_type)
   where rank=1;
   
--   dbms_output.put_line('mon: '||lv$month||' year: '||ln$year);
   
   select gambling_id
     into ln$gambling_id
     from (
   select gambling_id
        , RANK() OVER (PARTITION BY gambling_type ORDER BY gambling_id) rank
     from olap_sys.sl_gamblings g
    WHERE gambling_type = pv_drawing_type
      and substr(gambling_date,instr(gambling_date,'-',1,1)+1,2) = lv$month
      and substr(gambling_date,instr(gambling_date,'-',2,2)+1) = to_char(ln$year)
    )
   where rank=1;
   
  return ln$gambling_id;
exception
  when no_data_found then
    return null;
end get_gigamelate_range;

--[procedure used for sorting in an ascending way all digits tied to a drawing
procedure sort_inbound_comb (x_comb1  in out number
                           , x_comb2  in out number
                           , x_comb3  in out number
                           , x_comb4  in out number
                           , x_comb5  in out number
                           , x_comb6  in out number                           
                            ) as
  cursor c_comb is
  with sort_tbl as(
  select x_comb1 comb from dual
  union
  select x_comb2 comb from dual
  union
  select x_comb3 comb from dual
  union
  select x_comb4 comb from dual
  union
  select x_comb5 comb from dual
  union
  select x_comb6 comb from dual  
  ) select comb from sort_tbl order by comb;
                            
begin
  g_index := 1;
  dbms_output.put_line('bs. '||x_comb1||' '||x_comb2||' '||x_comb3||' '||x_comb4||' '||x_comb5||' '||x_comb6);
  for k in c_comb loop
      if    g_index=1 then x_comb1 := k.comb; 
      elsif g_index=2 then x_comb2 := k.comb;
      elsif g_index=3 then x_comb3 := k.comb; 
      elsif g_index=4 then x_comb4 := k.comb; 
      elsif g_index=5 then x_comb5 := k.comb; 
      elsif g_index=6 then x_comb6 := k.comb;  
      end if;
      g_index := g_index + 1;
  end loop;
  dbms_output.put_line('as. '||x_comb1||' '||x_comb2||' '||x_comb3||' '||x_comb4||' '||x_comb5||' '||x_comb6);
end sort_inbound_comb;                            

--[function used for finding out if a digit is a prime number                            
function is_prime_number (pn_digit           olap_sys.w_combination_responses_fs.comb1%type) return number is
begin
  --will consider 1 as prime number on purpose
  if pn_digit in (1,2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101) then
     return 1;
  else
     return 0;
  end if;      
end is_prime_number;

function is_valid_module_criteria (pv_gambling_type                  olap_sys.s_preview_gamblings.drawing_type%type
                                 , pn_comb1                          olap_sys.s_preview_gamblings.comb1%type
                                 , pn_comb2                          olap_sys.s_preview_gamblings.comb2%type
                                 , pn_comb3                          olap_sys.s_preview_gamblings.comb3%type
                                 , pn_comb4                          olap_sys.s_preview_gamblings.comb4%type
                                 , pn_comb5                          olap_sys.s_preview_gamblings.comb5%type
                                 , pn_comb6                          olap_sys.s_preview_gamblings.comb6%type
                                 , pn_module_value                   number
                                 , pn_primer_number                  number
                                 ) return varchar2 is
begin 
   select count(1)
      into olap_sys.w_common_pkg.g_rowcnt
      from olap_sys.c_drawing_criteria_rules
     where drawing_type     = pv_gambling_type 
       and status           = 'A'
       and criteria_type    = 'NONE_PAIR'
       and data_type        = 'NUM' 
       and prime_number_cnt = pn_primer_number
       and out_attribute1   = mod(pn_comb1,2) 
       and out_attribute2   = mod(pn_comb2,2)
       and out_attribute3   = mod(pn_comb3,2)
       and out_attribute4   = mod(pn_comb4,2)
       and out_attribute5   = mod(pn_comb5,2)
       and out_attribute6   = mod(pn_comb6,2);

   if olap_sys.w_common_pkg.g_rowcnt > 0 then
      return 'Y';
   else
      return 'N';
   end if;       
exception
  when no_data_found then
    return 'N';          
end is_valid_module_criteria; 


function is_valid_prime_number_criteria (pv_gambling_type                  olap_sys.s_preview_gamblings.drawing_type%type
                                       , pn_comb1                          olap_sys.s_preview_gamblings.comb1%type
                                       , pn_comb2                          olap_sys.s_preview_gamblings.comb2%type
                                       , pn_comb3                          olap_sys.s_preview_gamblings.comb3%type
                                       , pn_comb4                          olap_sys.s_preview_gamblings.comb4%type
                                       , pn_comb5                          olap_sys.s_preview_gamblings.comb5%type
                                       , pn_comb6                          olap_sys.s_preview_gamblings.comb6%type
                                       , pn_primer_number                  number
                                       ) return varchar2 is
begin
   select count(1)
      into olap_sys.w_common_pkg.g_rowcnt
      from olap_sys.c_drawing_criteria_rules
     where drawing_type     = pv_gambling_type 
       and status           = 'A'
       and criteria_type    = 'PRIME_NUMBER'
       and data_type        = 'NUM' 
       and prime_number_cnt = pn_primer_number
       and out_attribute1   = olap_sys.w_common_pkg.is_prime_number(pn_comb1) 
       and out_attribute2   = olap_sys.w_common_pkg.is_prime_number(pn_comb2)
       and out_attribute3   = olap_sys.w_common_pkg.is_prime_number(pn_comb3)
       and out_attribute4   = olap_sys.w_common_pkg.is_prime_number(pn_comb4)
       and out_attribute5   = olap_sys.w_common_pkg.is_prime_number(pn_comb5)
       and out_attribute6   = olap_sys.w_common_pkg.is_prime_number(pn_comb6);
   if olap_sys.w_common_pkg.g_rowcnt > 0 then
      return 'Y';
   else
      return 'N';
   end if;
exception
  when no_data_found then
    return 'N';    
end is_valid_prime_number_criteria;

--[function used for verifying if the drawing exists on table s_gigamelate_stats
--[will return Y if the drawing is found. Otherwise, will return N
function get_gigaloterias_count (pv_gambling_type  	         olap_sys.sl_gamblings.gambling_type%type
                               , pn_gambling_id                 number
                               , pn_comb1                       number
                               , pn_comb2                       number
                               , pn_comb3                       number
                               , pn_comb4                       number
                               , pn_comb5                       number
                               , pn_comb6                       number) return number is
  ln$drawing_fount_cnt      number := 0;                              
begin
  dbms_output.put_line('pn_gambling_id: '||pn_gambling_id);	
  olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) from olap_sys.s_calculo_stats where drawing_type = :1 and drawing_id = :2 and winner_flag = :3';
  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and b_type = :4 and digit = :5';

--  olap_sys.w_common_pkg.g_dml_stmt := 'select count(1) from olap_sys.s_gigamelate_stats where drawing_type = '||chr(39)||pv_gambling_type||chr(39)||' and drawing_id = '||pn_gambling_id||' and winner_flag = '||chr(39)||'Y'||chr(39);
--  olap_sys.w_common_pkg.g_dml_stmt := olap_sys.w_common_pkg.g_dml_stmt||' and b_type = :4 and digit = :5';

  olap_sys.w_common_pkg.g_rowcnt   := 0;
 
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B1'
       	                                                                                              , pn_comb1;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B1: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb1: '||pn_comb1);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B2'
       	                                                                                              , pn_comb2;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B2: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb2: '||pn_comb2);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B3'
       	                                                                                              , pn_comb3;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B3: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb3: '||pn_comb3);

         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B4'
       	                                                                                              , pn_comb4;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B4: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb4: '||pn_comb4);
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B5'
       	                                                                                              , pn_comb5;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B5: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb5: '||pn_comb5);
         begin
      	   execute immediate olap_sys.w_common_pkg.g_dml_stmt into olap_sys.w_common_pkg.g_rowcnt using pv_gambling_type
      	                                                                                              , pn_gambling_id
      	                                                                                              , 'Y'
       	                                                                                              , 'B6'
       	                                                                                              , pn_comb6;
           ln$drawing_fount_cnt := ln$drawing_fount_cnt + olap_sys.w_common_pkg.g_rowcnt;
       	 exception
       	   when no_data_found then
       	     olap_sys.w_common_pkg.g_rowcnt := 0;
       	 end;    	                                                                                           
      dbms_output.put_line('B6: '||olap_sys.w_common_pkg.g_rowcnt||', pn_comb6: '||pn_comb6);

      dbms_output.put_line('ln$drawing_fount_cnt: '||ln$drawing_fount_cnt);
  
  return ln$drawing_fount_cnt;
end get_gigaloterias_count;

--[function used for retrieving last drawing_id to be used as criteria in order to get drawing data                                
function get_last_gigaloterias_id (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                 , pn_gambling_id               number default null
                                 , pn_gambling_range            number default 99) return number is
  ln$max_gambling_id    number := 0;
begin
  if pn_gambling_id is null then
     select max(gambling_id)
       into ln$max_gambling_id
       from olap_sys.sl_gamblings
      where gambling_type = pv_gambling_type;
  else
     ln$max_gambling_id := pn_gambling_id;
  end if;
  
  if ln$max_gambling_id > 0 then
     return (ln$max_gambling_id - pn_gambling_range);
  else
     return 9999999;   
  end if;      
end;                                                                 

--[function used for retrieving coonstant values used for computing average patterns
function get_avg_pattern_constant (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                 , pv_column_name               varchar2) return number is

begin
  if pv_gambling_type = 'mrtr' then
     if pv_column_name = 'comb1' then
        return 5.9;
     elsif pv_column_name = 'comb2' then
        return 10.9;
     elsif pv_column_name = 'comb3' then
        return 16.9;
     elsif pv_column_name = 'comb4' then
        return 22.9;
     elsif pv_column_name = 'comb5' then
        return 28.9;
     elsif pv_column_name = 'comb6' then
        return 33.9;
     elsif pv_column_name = 'comb_sum' then
        return 119.9;
     end if;
     
  end if;

end get_avg_pattern_constant;                                

--[function used for getting count for digit that are above corresponding digit average
function get_average_pattern_count (pv_gambling_type  	     olap_sys.sl_gamblings.gambling_type%type default 'mrtr'
                                  , pn_comb1                 number default null
                                  , pn_comb2                 number default null
                                  , pn_comb3                 number default null
                                  , pn_comb4                 number default null
                                  , pn_comb5                 number default null
                                  , pn_comb6                 number default null
                                  , pv_type                  varchar2 default 'AVG') return number is
  CN$AVERAGE_COMB1         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb1');
  CN$AVERAGE_COMB2         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb2');
  CN$AVERAGE_COMB3         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb3');
  CN$AVERAGE_COMB4         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb4');
  CN$AVERAGE_COMB5         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb5');
  CN$AVERAGE_COMB6         constant number(2) := get_avg_pattern_constant(pv_gambling_type => pv_gambling_type, pv_column_name => 'comb6');
  ln$patterb_cnt                    number    := 0; 
begin
  if pv_gambling_type = 'mrtr' then
     if pv_type = 'AVG' then
        if pn_comb1 is not null and pn_comb1 >= CN$AVERAGE_COMB1 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;   
        
        if pn_comb2 is not null and pn_comb2 >= CN$AVERAGE_COMB2 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;   
        
        if pn_comb3 is not null and pn_comb3 >= CN$AVERAGE_COMB3 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;   
        
        if pn_comb4 is not null and pn_comb4 >= CN$AVERAGE_COMB4 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;   
        
        if pn_comb5 is not null and pn_comb5 >= CN$AVERAGE_COMB5 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;   
        
        if pn_comb6 is not null and pn_comb6 >= CN$AVERAGE_COMB6 then
           ln$patterb_cnt := ln$patterb_cnt + 1;
        end if;
     else
        --[low and high range computed based on stddev]
        /*
        if pn_comb1 is not null then 
           if pn_comb1 > 9.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb1 between 1.9 and 9.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb1 < 1.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb2 is not null then
           if pn_comb2 > 16.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb2 between 4.9 and 16.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb2 < 4.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb3 is not null then
           if pn_comb3 > 22.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb3 between 10.9 and 22.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb3 < 10.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb4 is not null then
           if pn_comb4 > 28.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb4 between 16.9 and 28.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb4 < 16.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb5 is not null then
           if pn_comb5 > 34.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb5 between 22.9 and 34.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb5 < 22.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb6 is not null then
           if pn_comb6 > 37.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb6 between 29.9 and 37.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb6 < 29.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;     
     */   
     --[low and high range computed based on stddev/2]
       if pn_comb1 is not null then 
           if pn_comb1 > 7.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb1 between 3.9 and 7.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb1 < 3.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb2 is not null then
           if pn_comb2 > 13.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb2 between 7.9 and 13.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb2 < 7.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb3 is not null then
           if pn_comb3 > 19.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb3 between 13.9 and 19.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb3 < 13.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb4 is not null then
           if pn_comb4 > 25.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb4 between 19.9 and 25.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb4 < 19.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb5 is not null then
           if pn_comb5 > 31.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb5 between 25.9 and 31.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb5 < 25.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;   
        
        if pn_comb6 is not null then
           if pn_comb6 > 35.9 then
              ln$patterb_cnt := 1;
           elsif pn_comb6 between 31.9 and 35.9 then
              ln$patterb_cnt := 2;
           elsif pn_comb6 < 31.9 then
              ln$patterb_cnt := 3;
           end if;      
        end if;     
     end if;     
  end if;
  
  return ln$patterb_cnt;  

end  get_average_pattern_count;                                 

--[function used for returning average on global index based on type
function get_avg_global_index (pv_type   varchar2) return number is

begin
  if pv_type = 'LOW' then
     return GN$AVG_GLOBAL_INDEX_LR;
  elsif pv_type = 'AVG' then
     return GN$AVG_GLOBAL_INDEX;
  elsif pv_type = 'HIGH' then
     return GN$AVG_GLOBAL_INDEX_HR;
  end if;        
end get_avg_global_index;

--[function used for returning average on comb sum based on type
function get_avg_comb_sum (pv_type   varchar2) return number is

begin
  if pv_type = 'LOW' then
     return GN$AVG_COMB_SUM_LR;
  elsif pv_type = 'AVG' then
     return GN$AVG_COMB_SUM;
  elsif pv_type = 'HIGH' then
     return GN$AVG_COMB_SUM_HR;
  end if;        
end get_avg_comb_sum;

--[ function used for returning master count from table olap_sys.c_usr_dtl_sum_par_comb_count   
/*function get_usr_dtl_sum_par_comb_cnt (pv_drawing_type    olap_sys.c_usr_dtl_sum_par_comb_count.drawing_type%type                                     
                                     , pn_sum_par_comb    olap_sys.c_usr_dtl_sum_par_comb_count.sum_par_comb%type default -1
                                     , pn_comb_sum        olap_sys.c_usr_dtl_sum_par_comb_count.comb_sum%type
                                     , pv_column          varchar2 default 'XROWCOUNT'
                                      ) return number is
begin
  select decode(pv_column,'XROWCOUNT',xrowcount,'P_SUM_PAR_COMB',p_sum_par_comb,'P_COMB_SUM',p_comb_sum,'P_A_UNION_B',p_A_union_B,'P_A_INTER_B',p_A_inter_B)
    into g_rowcnt
    from olap_sys.c_usr_dtl_sum_par_comb_count
   where drawing_type = pv_drawing_type
     and sum_par_comb = pn_sum_par_comb
     and comb_sum     = pn_comb_sum;
  
  return g_rowcnt;
exception
  when no_data_found then
     return 0;	
  when others then
     return 0;
end get_usr_dtl_sum_par_comb_cnt;
*/
--[ function used for returning master count from table olap_sys.c_usr_drawings_master_count   
function get_usr_drawings_master_cnt (pv_drawing_type    olap_sys.c_usr_drawings_master_count.drawing_type%type) return number is
begin
  select xrowcount
    into g_rowcnt
    from olap_sys.c_usr_drawings_master_count
   where drawing_type = pv_drawing_type;
  
  return g_rowcnt;
exception
  when no_data_found then
     return 0;	   	
  when others then
     return 0;
end get_usr_drawings_master_cnt;

--[procedure used for updating winner flag on table olap_sys.s_calculo_stats
procedure upd_calculo_stats_winner_flag (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
                                       , pn_gambling_id                 number
                                        ) is 
   pragma autonomous_transaction;
   ln$gambling_id_prev     olap_sys.s_calculo_stats.drawing_id%type:= pn_gambling_id-1;
   cursor c_in (pv_gambling_type  	        olap_sys.sl_gamblings.gambling_type%type
              , pn_gambling_id                 number) is
   select comb1
        , comb2
        , comb3
        , comb4
        , comb5
        , comb6
     from olap_sys.sl_gamblings
    where gambling_type = pv_gambling_type
      and gambling_id   = pn_gambling_id; 
begin
--   if pv_b_type = 'B6' then
	   dbms_output.put_line('gambling_type: '||pv_gambling_type);
	   dbms_output.put_line('sl_gamblings.gambling_id: '||pn_gambling_id);
	   olap_sys.w_common_pkg.g_data_found :=  0;
	   for i in c_in (pv_gambling_type => pv_gambling_type
	                , pn_gambling_id   => pn_gambling_id) loop
	       dbms_output.put_line(i.comb1||' - '||i.comb2||' - '||i.comb3||' - '||i.comb4||' - '||i.comb5||' - '||i.comb6);
	       olap_sys.w_common_pkg.g_data_found :=  1;
	       dbms_output.put_line('s_calculo_stats.ln$gambling_id_prev: '||ln$gambling_id_prev);
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B1'
	          and digit        = i.comb1;
	dbms_output.put_line(sql%rowcount||' rows updated for B1 and '||i.comb1); 
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B2'
	          and digit        = i.comb2;
	dbms_output.put_line(sql%rowcount||' rows updated for B2 and '||i.comb2);
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B3'
	          and digit        = i.comb3;
	dbms_output.put_line(sql%rowcount||' rows updated for B3 and '||i.comb3);
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B4'
	          and digit        = i.comb4;
	dbms_output.put_line(sql%rowcount||' rows updated for B4 and '||i.comb4);
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B5'
	          and digit        = i.comb5;
	dbms_output.put_line(sql%rowcount||' rows updated for B5 and '||i.comb5);
	       update olap_sys.s_calculo_stats
	          set winner_flag  = 'Y'
	            , updated_by   = USER
	            , updated_date = SYSDATE
	        where drawing_type = pv_gambling_type
	          and drawing_id   = ln$gambling_id_prev
	          and b_type       = 'B6'
	          and digit        = i.comb6;
	dbms_output.put_line(sql%rowcount||' rows updated for B6 and '||i.comb6);          
	   end loop;
	   if olap_sys.w_common_pkg.g_data_found = 1 then
	      commit;
	   else
	      dbms_output.put_line('No data found for gambling id: '||ln$gambling_id_prev);          
	   end if;
--   end if;
end upd_calculo_stats_winner_flag;

--[function used for counting winner drawings allocated on table s_gl_abril_combinations
function count_win_drawings (pv_match_history varchar2) return number is
  ln$win_drawing_cnt   number := 0;
  CV$STRING            constant varchar2(1) := ':';
begin
   if pv_match_history is not null then
      for i in 1..length(pv_match_history) loop
         if substr(pv_match_history,i,1) = CV$STRING then
         ln$win_drawing_cnt := ln$win_drawing_cnt + 1;
         end if;
      end loop;
   end if;
   return ln$win_drawing_cnt;
end count_win_drawings;

--[function used for identifying termination numbers in a whole combination
function find_terminations (pn_comn1  number
                        , pn_comn2  number
                        , pn_comn3  number
                        , pn_comn4  number
                        , pn_comn5  number
                        , pn_comn6  number
                         ) return number is
  ln$end_number_cnt  number := 0;
  type strValues IS VARRAY(6) OF VARCHAR2(10);
  numberList strValues;
  cursor c_terminations (pv_comb1  varchar2
                       , pv_comb2  varchar2
                       , pv_comb3  varchar2
                       , pv_comb4  varchar2
                       , pv_comb5  varchar2
                       , pv_comb6  varchar2
                       ) is
  with values_tbl as (
    select substr(pv_comb1,2,1) strValue from dual
    union all
    select substr(pv_comb2,2,1) strValue from dual
    union all
    select substr(pv_comb3,2,1) strValue from dual
    union all
    select substr(pv_comb4,2,1) strValue from dual
    union all
    select substr(pv_comb5,2,1) strValue from dual
    union all
    select substr(pv_comb6,2,1) strValue from dual
  ), groupv_tbl as (
  select strValue
       , count(1) cnt
    from values_tbl 
   group by strValue
   having count(1) > 1
  ), sum_tbl as ( 
  select sum(case when cnt > 1 then 1 else cnt end) sum_cnt 
    from groupv_tbl
 ) select nvl(sum_cnt,0) sum_cnt from sum_tbl
  ;

begin
  ln$end_number_cnt := 0;
  numberList := strValues (lpad(pn_comn1,2,'0'),lpad(pn_comn2,2,'0'),lpad(pn_comn3,2,'0'),lpad(pn_comn4,2,'0'),lpad(pn_comn5,2,'0'),lpad(pn_comn6,2,'0'));
  open c_terminations (pv_comb1 => numberList(1)
                     , pv_comb2 => numberList(2) 
                     , pv_comb3 => numberList(3)
                     , pv_comb4 => numberList(4) 
                     , pv_comb5 => numberList(5) 
                     , pv_comb6 => numberList(6) 
                     );
  fetch c_terminations into ln$end_number_cnt;
  close c_terminations;
  return ln$end_number_cnt;
end find_terminations;

function show_prime_number(pn_digit  number) return number is
begin
  if is_prime_number (pn_digit) = 1 then
     return pn_digit;
  else
     return 0;
  end if;      
end show_prime_number;

function show_multiple_number(pn_digit   number
                            , pn_base    number) return number is
begin
  if mod(pn_digit,pn_base) = 0 then
     return pn_digit;
  else
     return 0;
  end if;      
end show_multiple_number;

function hide_prime_number(pn_digit  number) return number is

begin

  if is_prime_number (pn_digit) = 1 then
     return 1;
  else
     return 0;
  end if;     
end hide_prime_number;

function compute_multiplos(pn_comn1  number
                         , pn_comn2  number
                         , pn_comn3  number
                         , pn_comn4  number
                         , pn_comn5  number
                         , pn_comn6  number) return varchar2 is
   ln$m3    number := 0;                        
   ln$m4    number := 0;
   ln$m5    number := 0;
   ln$m7    number := 0;
   CN$M3    CONSTANT NUMBER := 3;
   CN$M4    CONSTANT NUMBER := 4;
   CN$M5    CONSTANT NUMBER := 5;
   CN$M7    CONSTANT NUMBER := 7;
begin
  --multiplos de 3
  if mod(pn_comn1,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;

  if mod(pn_comn2,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;

  if mod(pn_comn3,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;

  if mod(pn_comn4,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;

  if mod(pn_comn5,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;

  if mod(pn_comn6,CN$M3) = 0 then
     ln$m3 := ln$m3 + 1;
  end if;
      
  
  
end compute_multiplos;                         


function get_dozen_sort (p_d1   varchar2
                       , p_d2   varchar2
                       , p_d3   varchar2
                       , p_d4   varchar2
                       , p_d5   varchar2
                       , p_d6   varchar2
                        ) return number is
    ln$dozen_sort        number := 0;
begin

	 with decenas_tbl as (
		select code
			 , to_number(substr(substr(attribute3,1,instr(attribute3,',',1,1)-1),1,instr(substr(attribute3,1,instr(attribute3,',',1,1)-1),'-',1,1)-1)) d1_ini
			 , to_number(substr(substr(attribute3,1,instr(attribute3,',',1,1)-1),instr(substr(attribute3,1,instr(attribute3,',',1,1)-1),'-',1,1)+1)) d1_end
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),1,instr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),'-',1,1)-1)) d2_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),instr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),'-',1,1)+1)) d2_end
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),1,instr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),'-',1,1)-1)) d3_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),instr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),'-',1,1)+1)) d3_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),1,instr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),'-',1,1)-1)) d4_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),instr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),'-',1,1)+1)) d4_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),1,instr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),'-',1,1)-1)) d5_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),instr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),'-',1,1)+1)) d5_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,5)+1),1,instr(substr(attribute3,instr(attribute3,',',1,5)+1),'-',1,1)-1)) d6_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,5)+1),instr(substr(attribute3,instr(attribute3,',',1,5)+1),'-',1,1)+1)) d6_end    
		  from olap_sys.w_lookups_fs
		 where gambling_type = 'mrtr'
		   and context= 'DECENAS'
		   and status = 'A'
	) select to_number(lkp.code)
	    into ln$dozen_sort
        from decenas_tbl lkp
       where to_number(p_d1) between lkp.d1_ini and lkp.d1_end
         and to_number(p_d2) between lkp.d2_ini and lkp.d2_end 
         and to_number(p_d3) between lkp.d3_ini and lkp.d3_end 
         and to_number(p_d4) between lkp.d4_ini and lkp.d4_end 
         and to_number(p_d5) between lkp.d5_ini and lkp.d5_end 
         and to_number(p_d6) between lkp.d6_ini and lkp.d6_end;

 return ln$dozen_sort;
end get_dozen_sort;

function get_dozen_rank (p_d1   varchar2
                       , p_d2   varchar2
                       , p_d3   varchar2
                       , p_d4   varchar2
                       , p_d5   varchar2
                       , p_d6   varchar2
                        ) return varchar2 is
  lv$dozen_rank             varchar2(5); 	 
begin
	 with decenas_tbl as (
		select attribute4 decena_by
		     , to_number(substr(substr(attribute3,1,instr(attribute3,',',1,1)-1),1,instr(substr(attribute3,1,instr(attribute3,',',1,1)-1),'-',1,1)-1)) d1_ini
			 , to_number(substr(substr(attribute3,1,instr(attribute3,',',1,1)-1),instr(substr(attribute3,1,instr(attribute3,',',1,1)-1),'-',1,1)+1)) d1_end
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),1,instr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),'-',1,1)-1)) d2_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),instr(substr(attribute3,instr(attribute3,',',1,1)+1,instr(attribute3,',',1,2)-instr(attribute3,',',1,1)-1),'-',1,1)+1)) d2_end
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),1,instr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),'-',1,1)-1)) d3_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),instr(substr(attribute3,instr(attribute3,',',1,2)+1,instr(attribute3,',',1,3)-instr(attribute3,',',1,2)-1),'-',1,1)+1)) d3_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),1,instr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),'-',1,1)-1)) d4_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),instr(substr(attribute3,instr(attribute3,',',1,3)+1,instr(attribute3,',',1,4)-instr(attribute3,',',1,3)-1),'-',1,1)+1)) d4_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),1,instr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),'-',1,1)-1)) d5_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),instr(substr(attribute3,instr(attribute3,',',1,4)+1,instr(attribute3,',',1,5)-instr(attribute3,',',1,4)-1),'-',1,1)+1)) d5_end         
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,5)+1),1,instr(substr(attribute3,instr(attribute3,',',1,5)+1),'-',1,1)-1)) d6_ini
			 , to_number(substr(substr(attribute3,instr(attribute3,',',1,5)+1),instr(substr(attribute3,instr(attribute3,',',1,5)+1),'-',1,1)+1)) d6_end    
		  from olap_sys.w_lookups_fs
		 where gambling_type = 'mrtr'
		   and context= 'DECENAS'
		   and status = 'A'
	) select to_number(lkp.decena_by)
	    into lv$dozen_rank
        from decenas_tbl lkp
       where to_number(p_d1) between lkp.d1_ini and lkp.d1_end
         and to_number(p_d2) between lkp.d2_ini and lkp.d2_end 
         and to_number(p_d3) between lkp.d3_ini and lkp.d3_end 
         and to_number(p_d4) between lkp.d4_ini and lkp.d4_end 
         and to_number(p_d5) between lkp.d5_ini and lkp.d5_end 
         and to_number(p_d6) between lkp.d6_ini and lkp.d6_end;

--  dbms_output.put_line(lv$rank);
  return lv$dozen_rank;
end get_dozen_rank;                          

function descomponer_numero(pn_comb  number) return number is

begin
   if pn_comb in (11,22,33) then
      return pn_comb;  
   elsif pn_comb in (1,10,19,28,37) then
      return 1;
   elsif pn_comb in (2,20,29,38) then
      return 2;
   elsif pn_comb in (3,12,21,30,39) then
      return 3;
   elsif pn_comb in (4,13,31) then
      return 4;
   elsif pn_comb in (5,14,23,32) then
      return 5;
   elsif pn_comb in (6,15,24) then
      return 6;
   elsif pn_comb in (7,16,25,34) then
      return 7;
   elsif pn_comb in (8,17,26,35) then
      return 8;
   elsif pn_comb in (9,18,27,36) then
      return 9;
   end if;	  
end descomponer_numero;                                                                                                 	                            					                                               	                                                                     	   	                                                                                     

function is_real_none (pn_digit  number) return number is

begin
  if mod(pn_digit,2)= 1 then
     if is_prime_number (pn_digit => pn_digit) = 1 then
	    return 0;
	 else 
        return 1;
     end if;		
  else
     return 0;
  end if;
end is_real_none;

function is_real_par (pn_digit  number) return number is

begin
  if mod(pn_digit,2)= 0 then
     if is_prime_number (pn_digit => pn_digit) = 1 then
	    return 0;
	 else 
        return 1;
     end if;		
  else
     return 0;
  end if;
end is_real_par;

function compute_ciclos (pv_lista_estadistica   varchar2) return number is
   ln$gambling_id_1   NUMBER := 0;
   ln$gambling_id_2   NUMBER := 0;
   lv$substr_1        VARCHAR2(500);
   lv$input_string    VARCHAR2(500);
   ln$ocurrencia_cnt  NUMBER :=0;
   ln$posicion_x      NUMBER :=2;
   ln$instr_1         NUMBER :=0;
   ln$output          NUMBER :=0;
begin
   dbms_output.put_line('pv_lista_estadistica: '||pv_lista_estadistica); 
   if length(pv_lista_estadistica) = 0 or (length(pv_lista_estadistica) - length(replace(pv_lista_estadistica,'|'))= 1) then
      dbms_output.put_line('000'); 
	  return null;
   else
      lv$input_string := trim(pv_lista_estadistica);
	  dbms_output.put_line('lv$input_string: '||lv$input_string);
	  ln$ocurrencia_cnt := length(lv$input_string) - length(replace(lv$input_string,'|'));
	  dbms_output.put_line('ln$ocurrencia_cnt: '||ln$ocurrencia_cnt);
	  
	  IF ln$ocurrencia_cnt = 2 THEN
	     ln$posicion_x:= 1;
		 lv$substr_1  := lv$input_string;
	  ELSE
		 lv$substr_1 := SUBSTR(lv$input_string,INSTR(lv$input_string,'|', 1, ln$ocurrencia_cnt-2)+1);	  
	  END IF;
	  dbms_output.put_line('ln$posicion_x: '||ln$posicion_x);
dbms_output.put_line('SUBSTR1: '||lv$substr_1);  
      ln$instr_1 := INSTR(lv$substr_1,'|');
	  dbms_output.put_line('INSTR1: '||ln$instr_1);
      ln$gambling_id_1 := TO_NUMBER(SUBSTR(lv$substr_1,1,ln$instr_1-1));  
dbms_output.put_line('ln$gambling_id_1: '||SUBSTR(lv$substr_1,1,ln$instr_1-1));  
dbms_output.put_line('INSTR2: '||INSTR(lv$substr_1,'|',2,2));
      ln$gambling_id_2 := TO_NUMBER(SUBSTR(lv$substr_1,ln$instr_1+1,INSTR(lv$substr_1,'|',2,2)-(ln$instr_1+1))); 
dbms_output.put_line('ln$gambling_id_2: '||ln$gambling_id_2); 
      ln$output := ln$gambling_id_2-ln$gambling_id_1;
dbms_output.put_line('ln$output: '||ln$output); 	  
	  return (ln$output);
   end if; 
exception 
   when others then  
      dbms_output.put_line('others: '||sqlerrm); 
	  return -1;   
end compute_ciclos;

--!funcion que regresa el promedio de los 
function compute_avg_ciclos (pv_lista_ciclos   varchar2) return number is
   lv$input_string    VARCHAR2(500);
   ln$ocurrencia_cnt  NUMBER :=0;
   ln$avg_ciclos      NUMBER :=0;
begin 
   dbms_output.put_line('pv_lista_ciclos: '||pv_lista_ciclos); 
   if length(pv_lista_ciclos) = 0 then
      dbms_output.put_line('000'); 
	  ln$avg_ciclos := null;
   elsif (length(pv_lista_ciclos) - length(replace(pv_lista_ciclos,'|'))= 1) then
      ln$avg_ciclos := to_number(substr(pv_lista_ciclos,1,length(replace(pv_lista_ciclos,'|'))));
   else
      lv$input_string := trim(pv_lista_ciclos);	  
	  ln$ocurrencia_cnt := length(lv$input_string) - length(replace(lv$input_string,'|'));
	  
	  if ln$ocurrencia_cnt >= 2 then 
		   with translate_tbl as (
		select regexp_substr(pv_lista_ciclos,'[^,|]+',1,level) str
						   from dual 
						 connect by level <= length(pv_lista_ciclos)-length(replace(pv_lista_ciclos,'|',''))+1
		) select round(avg(to_number(str)),2) avg_ciclos
		    into ln$avg_ciclos
			from translate_tbl			 
		   where str is not null;
	  elsif ln$ocurrencia_cnt = 1 then 
         ln$avg_ciclos := null;	  
	  end if;	      
   end if;	
   return round(ln$avg_ciclos);   
exception 
   when others then  
      dbms_output.put_line('others: '||sqlerrm); 
	  return -1;   
end compute_avg_ciclos;

--!cuenta el numeros de sorteos incluidos en la lista
function count_drawings_in_list (pv_lista_estadistica   varchar2) return number is
begin
	if pv_lista_estadistica is not null then
		if substr(pv_lista_estadistica,1,1) = '|' then
		   return (length(pv_lista_estadistica) - length(replace(pv_lista_estadistica,'|',null)) -1);
		else
		   return (length(pv_lista_estadistica) - length(replace(pv_lista_estadistica,'|',null)));	
		end if;	
	else
		return 0;
	end if;	
end count_drawings_in_list;


--!regresa el ultimo numero de sorteo de la lista
function get_last_drawing_from_list (pv_lista_estadistica   varchar2) return number is
   lv$last_drawing        varchar2(20);
   ln$drawing_cnt         number := 0;
begin
	if pv_lista_estadistica is not null then
--	dbms_output.put_line('pv_lista_estadistica: '||pv_lista_estadistica);
		if substr(pv_lista_estadistica,1,1) = '|' then
		   ln$drawing_cnt := count_drawings_in_list(pv_lista_estadistica=> pv_lista_estadistica);
--	dbms_output.put_line('1. ln$drawing_cnt: '||ln$drawing_cnt);		
		   lv$last_drawing := replace(substr(pv_lista_estadistica,instr(pv_lista_estadistica,'|',1,ln$drawing_cnt)),'|',null);
--		   dbms_output.put_line('1. lv$last_drawing: '||lv$last_drawing);
		else
		   ln$drawing_cnt := count_drawings_in_list(pv_lista_estadistica=> pv_lista_estadistica)-1;
--	dbms_output.put_line('1. ln$drawing_cnt: '||ln$drawing_cnt);
		   lv$last_drawing := replace(substr(pv_lista_estadistica,instr(pv_lista_estadistica,'|',1,ln$drawing_cnt)),'|',null);
--		   dbms_output.put_line('2. lv$last_drawing: '||lv$last_drawing);
		end if;	
		return lv$last_drawing;
	else
		return 0;
	end if;	
end get_last_drawing_from_list;

function compute_composition_by_tens (pv_ten_list   varchar2) return varchar2 is
    lrec$pm_comp_by_tens gt$pm_comp_by_tens_tbl;
	ln$0        NUMBER := 0;
	ln$1        NUMBER := 0;
	ln$2        NUMBER := 0;
	ln$3        NUMBER := 0;
    lv$use_flag VARCHAR2(1) := 'N';
	cursor c_main (pv_ten_list   varchar2) is
	WITH TEN_LIST_TBL AS (
	select regexp_substr(pv_ten_list,'[^,]+',1,level) ten_list
					   from dual 
					 connect by level <= length(pv_ten_list)-length(replace(pv_ten_list,',',''))+1 
	) SELECT TEN_LIST
		   , COUNT(1) CNT
		FROM TEN_LIST_TBL
	   WHERE TEN_LIST IS NOT NULL	
	   GROUP BY TEN_LIST
	   ORDER BY TEN_LIST;

procedure set_pm_comp_by_tens(pv_pm_ten_list      varchar
                            , pn_pm_accumulated   number 
							 ) is
							 
begin
   g_index := lrec$pm_comp_by_tens.count+1;
--   dbms_output.put_line('before g_index: '||g_index);
   lrec$pm_comp_by_tens(g_index).ten_composition := pv_pm_ten_list;
   lrec$pm_comp_by_tens(g_index).accumulated     := pn_pm_accumulated;
--   dbms_output.put_line('after count: '||lrec$pm_comp_by_tens.count);
end set_pm_comp_by_tens;

begin
   --setting composition_by_tens based on PM data
   set_pm_comp_by_tens(pv_pm_ten_list    => '1-1-1-1'
                     , pn_pm_accumulated => 10.27
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '0-1-1-2'
                     , pn_pm_accumulated => 19.26
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '0-1-2-1'
                     , pn_pm_accumulated => 28.25
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '0-2-1-1'
                     , pn_pm_accumulated => 34.68
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '1-0-1-2'
                     , pn_pm_accumulated => 40.67
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '1-0-2-1'
                     , pn_pm_accumulated => 46.66
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '0-0-2-2'
                     , pn_pm_accumulated => 51.91
					  );
   set_pm_comp_by_tens(pv_pm_ten_list    => '1-1-0-2'
                     , pn_pm_accumulated => 56.40
					  );					  
   dbms_output.put_line('array count: '||lrec$pm_comp_by_tens.count);
   
   --building composition_by_tens based on DB data
   for i in c_main (pv_ten_list => pv_ten_list) loop
       IF i.TEN_LIST = '1-9' THEN
	      ln$0 := i.CNT;
	   END IF;	  
       IF i.TEN_LIST = '10-19' THEN
	      ln$1 := i.CNT;
	   END IF;	  
       IF i.TEN_LIST = '20-29' THEN
	      ln$2 := i.CNT;
	   END IF;	  
       IF i.TEN_LIST = '30-39' THEN
	      ln$3 := i.CNT;
	   END IF;	  
   end loop;
/* 
  dbms_output.put_line('--------------------------------------');
  for j in lrec$pm_comp_by_tens.first..lrec$pm_comp_by_tens.last loop
 	   dbms_output.put_line('ten: '||lrec$pm_comp_by_tens(j).ten_composition);
	   dbms_output.put_line('accum: '||lrec$pm_comp_by_tens(j).accumulated);
  end loop;
  dbms_output.put_line('--------------------------------------');
*/
 
   --validating if the composition ten is into the array holding the 50 percent
   for j in lrec$pm_comp_by_tens.first..lrec$pm_comp_by_tens.last loop
       if lrec$pm_comp_by_tens(j).accumulated <= 57 then 
	   dbms_output.put_line('ten: '||lrec$pm_comp_by_tens(j).ten_composition);
	   dbms_output.put_line('db: '||ln$0||'-'||ln$1||'-'||ln$2||'-'||ln$3);
		   if lrec$pm_comp_by_tens(j).ten_composition = ln$0||'-'||ln$1||'-'||ln$2||'-'||ln$3 then
			  lv$use_flag := 'Y';
			  dbms_output.put_line('use_flag: '||lv$use_flag);
		   else 
			  lv$use_flag := 'N';
			  dbms_output.put_line('use_flag: '||lv$use_flag);
		   end if;
	   end if;
   end loop;
   
   return ln$0||'-'||ln$1||'-'||ln$2||'-'||ln$3||'-'||lv$use_flag;  
end compute_composition_by_tens;

function count_multiple_terminations (pn_comn1  number
                                    , pn_comn2  number
                                    , pn_comn3  number
                                    , pn_comn4  number
                                    , pn_comn5  number
                                    , pn_comn6  number
									, pn_base_number number default 2
									, pv_exe_flag varchar2 default 'TERM_ONLY'
									) return number is
	lv_comb_list       	varchar2(20);
	ln$term_no_match   	number := 0;
	ln$term_match	   	number := 0;
	lb$term_found   	boolean := false;	
	
	lb_true            boolean := true;
	cursor c_main (pv_comb_list  varchar2) is
	with main_tbl as ( 
	select regexp_substr(pv_comb_list,'[^,]+',1,level) str
					   from dual 
					 connect by level <= length(pv_comb_list)-length(replace(pv_comb_list,',',''))+1 
	), count_tbl as ( select substr(lpad(str,2,0),2,1) str
		   , count(1) cnt
		from main_tbl
	   where str is not null
	   group by substr(lpad(str,2,0),2,1) 
	  order by cnt desc
    ) select cnt
           , count(1) strcnt
        from count_tbl
       group by cnt
       order by cnt
	 ;   	       
begin
	if pn_base_number > 0 then
		lv_comb_list := to_char(pn_comn1)||','||to_char(pn_comn2)||','||to_char(pn_comn3)||','||to_char(pn_comn4)||','||to_char(pn_comn5)||','||to_char(pn_comn6);
--		dbms_output.put_line(lv_comb_list);
		for i in c_main (pv_comb_list => lv_comb_list) loop
--		dbms_output.put_line('  i.cnt: '||i.cnt||'  i.str: '||i.strcnt);
			if i.cnt = pn_base_number then 
				ln$term_match := ln$term_match + 1;
				lb$term_found	 := true;
--				dbms_output.put_line('=');
			end if;
			if i.cnt > pn_base_number then 
				ln$term_no_match := ln$term_no_match + 1; 
				lb$term_found	 := true;
--				dbms_output.put_line('>');
			end if;
			if pn_base_number > i.cnt then 
				lb$term_found	 := false;
--				dbms_output.put_line('<');
			end if;
		--	   end if;			
		end loop;
--		dbms_output.put_line('ln$term_match: '||ln$term_match||'  ln$term_no_match: '||ln$term_no_match);
		if not lb$term_found then
			return 0;
		else
			--!todos las terminaciones de los digitos son iguales
			if ln$term_no_match = 0 then
				return 1;
			else
				return 0;
			end if;
		end if;
	else
		return 0;
	end if;	

end count_multiple_terminations;									
						

--!convertir un string separado por comas en renglones de un query
procedure translate_string_to_rows (pv_string                VARCHAR2
                                  , xtbl_row   IN OUT NOCOPY dbms_sql.varchar2_table
								  , x_err_code IN OUT NOCOPY NUMBER
								   ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'translate_string_to_rows';
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
	end if;
  
  --!creando sentencia dinamica
  g_dml_stmt := 'select regexp_substr('||chr(39)||pv_string||chr(39)||','||chr(39)||'[^,]+'||chr(39)||',1,level) str from dual connect by level <= length('||chr(39)||pv_string||chr(39)||')-length(replace('||chr(39)||pv_string||chr(39)||','||chr(39)||','||chr(39)||','||chr(39)||chr(39)||'))+1';

  --!borrando arreglo
  xtbl_row.delete;
  
  execute immediate g_dml_stmt bulk collect into xtbl_row;
  
  x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;
/*
  dbms_output.put_line('imprimiendo arreglo');
  for t in xtbl_row.first..xtbl_row.last loop
  dbms_output.put_line(t||' # '||xtbl_row(t));
  end loop;*/
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end translate_string_to_rows;


--!recuperar los valores distinctos de un string separado por comas
procedure get_distinct_values_from_list (pv_string                             VARCHAR2
									   , pv_data_type                          VARCHAR2 DEFAULT 'STRING'
									   , pv_data_sort						   VARCHAR2 DEFAULT 'DESC'
									   , xv_distinct_value_list  IN OUT NOCOPY VARCHAR2
									    ) is
										
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_distinct_values_from_list';
  cursor c_distinct_desc (pv_string  VARCHAR2) is
  select distinct regexp_substr(pv_string,'[^,]+',1,level) str
    from dual 
 connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1 order by 1 desc;
 
  cursor c_distinct_asc (pv_string  VARCHAR2) is
  select distinct regexp_substr(pv_string,'[^,]+',1,level) str
    from dual 
 connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1 order by 1 asc; 
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
	end if;
	
	xv_distinct_value_list := null;
	if pv_data_sort = 'DESC' then
		for t in c_distinct_desc (pv_string => pv_string) loop
			if pv_data_type = 'STRING' then	
				if pv_string is not null then
					xv_distinct_value_list := xv_distinct_value_list ||chr(39)||t.str||chr(39)||',';
				end if;	
			else
				if pv_string is not null then
					xv_distinct_value_list := xv_distinct_value_list ||t.str||',';
				end if;	
			end if;
		end loop;
	else
		for t in c_distinct_asc (pv_string => pv_string) loop
			if pv_data_type = 'STRING' then	
				if pv_string is not null then
					xv_distinct_value_list := xv_distinct_value_list ||chr(39)||t.str||chr(39)||',';
				end if;	
			else
				if pv_string is not null then
					xv_distinct_value_list := xv_distinct_value_list ||t.str||',';
				end if;	
			end if;
		end loop;
	end if;
	
  --!removiendo ultima coma
  xv_distinct_value_list := substr(xv_distinct_value_list,1,length(xv_distinct_value_list)-1);
--  dbms_output.put_line(xv_distinct_value_list);  
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end get_distinct_values_from_list; 

--!construye el select list final
procedure get_final_select_list (pv_string                             VARCHAR2
							   , xv_select_list          IN OUT NOCOPY VARCHAR2
							   , x_err_code              IN OUT NOCOPY NUMBER
							    ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_final_select_list';
  cursor c_final_string (pv_string  VARCHAR2) is
  select substr(regexp_substr(pv_string,'[^,]+',1,level),instr(regexp_substr(pv_string,'[^,]+',1,level),'-')+1) str
    from dual 
 connect by level <= length(pv_string)-length(replace(pv_string,',',''))+1;
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
	end if;
								
	for t in c_final_string (pv_string => pv_string) loop
		xv_select_list := xv_select_list ||t.str||',';
	end loop;
	
  --!removiendo ultima coma
  xv_select_list := substr(xv_select_list,1,length(xv_select_list)-1);
--  dbms_output.put_line(xv_select_list);  
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;	
end get_final_select_list; 								


procedure load_drawing_weight_metadata (xtbl_metadata IN OUT NOCOPY gt$gam_weight_tbl
									  , x_err_code	  IN OUT NOCOPY number	
										) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'load_drawing_weight_metadata';
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	end if;
	
	select attribute1, attribute3, attribute4
	  bulk collect into xtbl_metadata
	  from olap_sys.w_lookups_fs
	 where context = 'JUGADAS_FINALES'
	 order by attribute1;
	 
	 x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;
end load_drawing_weight_metadata;	


--!transforma el string de seis elementos en un string de tres elementos
procedure transform_input_string (pv_string 					  varchar2
							    , ptbl_metadata				  gt$gam_weight_tbl
							    , xv_new_string 	IN OUT NOCOPY varchar2
							    , x_err_code	  	IN OUT NOCOPY number
								 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'transform_input_string';
  ltbl$row_source                  dbms_sql.varchar2_table;
  ltbl$row_pattern                 dbms_sql.varchar2_table;
  ln$digito					   	   number := 0;
  ln$contador1					   number := 0;
  ln$contador3					   number := 0;
  ln$contador7					   number := 0;
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_string: '||pv_string);
	  dbms_output.put_line('ptbl_metadata.count: '||ptbl_metadata.count);
	end if;

	--!convertir un string separado por comas en renglones de un query
	translate_string_to_rows (pv_string  => pv_string
						    , xtbl_row   => ltbl$row_source
						    , x_err_code => x_err_code
						     );

	--!convertir un string separado por comas en renglones de un query
	translate_string_to_rows (pv_string  => ptbl_metadata(ptbl_metadata.first).termination
						    , xtbl_row   => ltbl$row_pattern
						    , x_err_code => x_err_code
						     );
	
	for t in ltbl$row_source.first..ltbl$row_source.last loop
--		dbms_output.put_line(ltbl$row_source(t));
		select decode(substr(ltbl$row_source(t),1,1),0,substr(ltbl$row_source(t),2,1),ltbl$row_source(t))
		  into ln$digito
		  from dual;
		  
		--[function used for finding out if a digit is a prime number                            
		if is_prime_number (pn_digit => ln$digito) = 1 then
			--!validando terminaciones en 1
			if substr(ltbl$row_source(t),2,1) = ltbl$row_pattern(1) then
				ln$contador1 := ln$contador1 + 1;
			end if;
			
			--!validando terminaciones en 3
			if substr(ltbl$row_source(t),2,1) = ltbl$row_pattern(2) then
				ln$contador3 := ln$contador3 + 1;
			end if;

			--!validando terminaciones en 7
			if substr(ltbl$row_source(t),2,1) = ltbl$row_pattern(3) then
				ln$contador7 := ln$contador7 + 1;
			end if;	
		end if;	
	end loop;		

	xv_new_string := ln$contador1||'-'||ln$contador3||'-'||ln$contador7;
--	dbms_output.put_line(xv_new_string);	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;
end transform_input_string;


--!recupera el peso de la jugada en base al nuevo string
procedure get_drawing_weight (pv_new_string 				  varchar2
						    , ptbl_metadata				  	  gt$gam_weight_tbl
							, xn_drawing_weight IN OUT NOCOPY number
						    , x_err_code	  	IN OUT NOCOPY number
							 ) is
  LV$PROCEDURE_NAME       constant varchar2(30) := 'get_drawing_weight';
begin
    if GB$SHOW_PROC_NAME then
	  dbms_output.put_line('--------------------------------');
	  dbms_output.put_line(LV$PROCEDURE_NAME);
	  dbms_output.put_line('pv_new_string: '||pv_new_string);
	  dbms_output.put_line('ptbl_metadata.count: '||ptbl_metadata.count);	  
	end if;

	xn_drawing_weight := ptbl_metadata(ptbl_metadata.last).weight;
	for t in ptbl_metadata.first..ptbl_metadata.last loop
		if pv_new_string = ptbl_metadata(t).pattern then
			xn_drawing_weight := ptbl_metadata(t).weight;
			exit;		
		end if;
	end loop;
	
	x_err_code := olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION;	
exception
  when others then
    x_err_code := sqlcode;
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	raise;
end get_drawing_weight;

  
--!recupera el peso de una jugada en base a metadatos
function get_drawing_weight (pv_string varchar2) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_drawing_weight';
	ltbl_weight_metadata		gt$gam_weight_tbl;
	ln$drawing_weight			number := -1;
	ln$err_code					number := 0;
	lv$new_string				varchar2(50);
begin
 
	--!cargando el metadata del peso de las jugadas
	load_drawing_weight_metadata (xtbl_metadata => ltbl_weight_metadata
								, x_err_code	=> ln$err_code
								 );

	if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
		--!transforma el string de seis elementos en un string de tres elementos
		transform_input_string (pv_string 		=> pv_string
							  , ptbl_metadata	=> ltbl_weight_metadata
							  , xv_new_string 	=> lv$new_string
							  , x_err_code	  	=> ln$err_code
							   );	

		if ln$err_code = olap_sys.w_common_pkg.GN$SUCCESSFUL_EXECUTION then
			--!recupera el peso de la jugada en base al nuevo string
			get_drawing_weight (pv_new_string 	  => lv$new_string
							  , ptbl_metadata	  => ltbl_weight_metadata
							  , xn_drawing_weight => ln$drawing_weight
							  , x_err_code	  	  => ln$err_code
							   );
		end if;										
	end if;					 
 
	return ln$drawing_weight;
exception
  when others then

    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	return -1;
end get_drawing_weight;


--!recupera el ultimo elemento de una lista separada por |
function get_last_list_item (pv_string varchar2) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_last_list_item';
    ln$last_item					 number := 0;
begin
	if pv_string is null then
		ln$last_item := 0;
	else 
		select to_number(case when length(pv_string)-length(replace(pv_string,'|',null))-1 = 0 then substr(pv_string,1,length(pv_string)-1) else replace(substr(pv_string,instr(pv_string,'|',1,length(pv_string)-length(replace(pv_string,'|',null))-1)+1),'|',null) end)
		  into ln$last_item
		  from dual;
	end if;	
	return ln$last_item;
exception
  when others then

    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	return -1;
end get_last_list_item; 


--!recupera un elemnto especifico de una lista separada por comas
function get_n_list_item (pv_string 	varchar2
						, pn_position	number) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_n_list_item';
    ln$err_code					 	 number := 0;
	ltbl$row_source                  dbms_sql.varchar2_table;
begin
	--!convertir un string separado por comas en renglones de un query
	translate_string_to_rows (pv_string  => pv_string
						    , xtbl_row   => ltbl$row_source
						    , x_err_code => ln$err_code
						     );

	for t in ltbl$row_source.first..ltbl$row_source.last loop
--		dbms_output.put_line(ltbl$row_source(t));
		if t = pn_position then
			return to_number(ltbl$row_source(t));
			exit;
		end if;	
	end loop;	  
exception
  when others then

    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
	return -1;
end get_n_list_item; 
						
						
--!recupera la longitud de la columna de una tabla
function get_column_length (pv_owner 		varchar2 default 'OLAP_SYS'
						  , pv_table_name	varchar2
						  , pv_column_name	varchar2) return number is
begin
	select data_length
	  into olap_sys.w_common_pkg.g_index
	  from all_tab_columns
	 where owner = 'OLAP_SYS'
	   and table_name = 'JUGADAS_LISTAS'
	   and column_name = 'VALIDACION';
	return olap_sys.w_common_pkg.g_index;
end get_column_length;						  

--!procedimiento para extraer el valor de cada posicion de config_ppn_description con este formato PR-%-%-PR-%-%
procedure read_config_primos_string (pv_config_ppn_description VARCHAR2
								   , xv_pos1	IN OUT NOCOPY VARCHAR2
								   , xv_pos2	IN OUT NOCOPY VARCHAR2
								   , xv_pos3	IN OUT NOCOPY VARCHAR2
								   , xv_pos4	IN OUT NOCOPY VARCHAR2
								   , xv_pos5	IN OUT NOCOPY VARCHAR2
								   , xv_pos6	IN OUT NOCOPY VARCHAR2) is
LV$PROCEDURE_NAME       constant varchar2(30) := 'read_config_primos_string';
begin
--	dbms_output.put_line(pv_config_ppn_description);
	xv_pos1 := substr(pv_config_ppn_description,1,instr(pv_config_ppn_description,'-',1,1)-1);	
	xv_pos2 := substr(pv_config_ppn_description,instr(pv_config_ppn_description,'-',1,1)+1,(instr(pv_config_ppn_description,'-',1,2)-instr(pv_config_ppn_description,'-',1,1))-1);
	xv_pos3 := substr(pv_config_ppn_description,instr(pv_config_ppn_description,'-',1,2)+1,(instr(pv_config_ppn_description,'-',1,3)-instr(pv_config_ppn_description,'-',1,2))-1);
	xv_pos4 := substr(pv_config_ppn_description,instr(pv_config_ppn_description,'-',1,3)+1,(instr(pv_config_ppn_description,'-',1,4)-instr(pv_config_ppn_description,'-',1,3))-1);
	xv_pos5 := substr(pv_config_ppn_description,instr(pv_config_ppn_description,'-',1,4)+1,(instr(pv_config_ppn_description,'-',1,5)-instr(pv_config_ppn_description,'-',1,4))-1);
	xv_pos6 := substr(pv_config_ppn_description,instr(pv_config_ppn_description,'-',1,5)+1);

exception
  when others then
    xv_pos1 := null;	
	xv_pos2 := null;
	xv_pos3 := null;
	xv_pos4 := null;
	xv_pos5 := null;
	xv_pos6 := null;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());    
    raise;
end read_config_primos_string;
								   
								   
--!contador de terminaciones en base a un numero base
function termination_counter (pv_termination_list	VARCHAR2
							, pn_base_number		NUMBER DEFAULT 2) return number is

begin
	begin
		with term_tbl as ( 
		select regexp_substr(pv_termination_list,'[^,]+',1,level) term
		  from dual 
	   connect by level <= length(pv_termination_list)-length(replace(pv_termination_list,',',''))+1
		)select count(1) cnt
		   into g_rowcnt
		   from term_tbl
		  where term = pn_base_number
		  group by term;
	exception 
		when no_data_found then
			g_rowcnt := 0;
	end;
	return g_rowcnt;	
end termination_counter;

--!funcion para identificar si se usuara una nueva secuencia para un nuevo registro
function is_new_gl_mapa_seq (pv_gl_type		varchar2
						   , pn_xrownum		number
						   , pn_seq_no		number
						   , pn_drawing_id	number) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'is_new_gl_mapa_seq';	
	ln$drawing_id			number :=0;
	pragma autonomous_transaction; 						   
begin
	g_data_found := 0;
--	dbms_output.put_line(LV$PROCEDURE_NAME);
--	dbms_output.put_line('pv_gl_type: '||pv_gl_type);
--	dbms_output.put_line('pn_xrownum: '||pn_xrownum);
--	dbms_output.put_line('pn_seq_no: '||pn_seq_no);
	if pn_xrownum = 2 and pn_seq_no = 2 then
--		dbms_output.put_line('pn_xrownum = 2');
		ln$drawing_id := pn_drawing_id + 1;
	else
--		dbms_output.put_line('pn_xrownum = 1');
		ln$drawing_id := pn_drawing_id;
	end if;	
--	dbms_output.put_line('pn_drawing_id: '||ln$drawing_id);	
	select count(1)
	  into g_data_found
	  from olap_sys.s_gl_mapas
	 where seq_no     = decode(pn_seq_no,2,1,pn_seq_no)
	   and drawing_id = ln$drawing_id;
--dbms_output.put_line('g_data_found: '||g_data_found);	
	return g_data_found;
end is_new_gl_mapa_seq;


--!funcion para recuperar el master_id de la tabla olap_sys.s_gl_mapas
function get_gl_mapa_master_id (pn_xrownum		number
						      , pn_seq_no		number
						      , pn_drawing_id	number
							  , pv_gl_type		varchar2 DEFAULT 'LT'
							  ) return number is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_gl_mapa_master_id';	
	ln$drawing_id			number :=0;
	ln$max_master_id		number :=0;
begin
	g_data_found := 0;
--	dbms_output.put_line(LV$PROCEDURE_NAME);
--	dbms_output.put_line('pv_gl_type: '||pv_gl_type);	
--	dbms_output.put_line('pn_seq_no: '||pn_seq_no);
	if pn_xrownum = 2 and pn_seq_no = 2 then
--		dbms_output.put_line('pn_xrownum = 2');
		ln$drawing_id := pn_drawing_id + 1;
	else
--		dbms_output.put_line('pn_xrownum = 1');
		ln$drawing_id := pn_drawing_id;
	end if;	
--	dbms_output.put_line('pn_drawing_id: '||ln$drawing_id);	
	select distinct master_id
	  into g_data_found
	  from olap_sys.s_gl_mapas
	 where seq_no     = decode(pn_seq_no,2,1,pn_seq_no)
	   and gl_type= pv_gl_type
	   and drawing_id = ln$drawing_id;	
--	dbms_output.put_line('g_data_found: '||g_data_found);	
	return g_data_found;
exception
	when no_data_found then
		select max(master_id)+1
		  into ln$max_master_id
		  from olap_sys.s_gl_mapas;
		return ln$max_master_id;
end get_gl_mapa_master_id;
		
--!funcion para averiguar si una jugada tiene terminaciones repetidas		
function is_jugada_term (pn_seq_id      number
                       , pn_digito      number) return number is
    ln$term_cnt             number := 0;
    ln$qry_term_cnt         number := 0;
	ln$terminacion_digito	number := 0;
    cursor c_jugadas (pn_seq_id      number) is   
    SELECT substr(lpad(comb1,2,'0'),2,1) comb1
         , substr(lpad(comb2,2,'0'),2,1) comb2
         , substr(lpad(comb3,2,'0'),2,1) comb3
         , substr(lpad(comb4,2,'0'),2,1) comb4
         , substr(lpad(comb5,2,'0'),2,1) comb5
         , substr(lpad(comb6,2,'0'),2,1) comb6
         , term_cnt
      from olap_sys.w_combination_responses_fs
     where seq_id = pn_seq_id;
begin    
    ln$terminacion_digito := substr(lpad(pn_digito,2,'0'),2,1); 
	
	with jugadat_terminaciones_tbl as (
        select substr(lpad(comb1,2,'0'),2,1) comb
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id
    union all
        select substr(lpad(comb2,2,'0'),2,1) 
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id 
    union all
        select substr(lpad(comb3,2,'0'),2,1) 
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id
    union all
        select substr(lpad(comb4,2,'0'),2,1) 
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id 
    union all
        select substr(lpad(comb5,2,'0'),2,1) 
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id
    union all
        select substr(lpad(comb6,2,'0'),2,1) 
             , term_cnt
          from olap_sys.w_combination_responses_fs
        where  SEQ_ID = pn_seq_id 
    ) select term_cnt
           , count(1) qry_term_cnt
        into ln$term_cnt
           , ln$qry_term_cnt 
      from jugadat_terminaciones_tbl
     where comb = ln$terminacion_digito 
     group by term_cnt;
    
    if ln$term_cnt = ln$qry_term_cnt then
        return 1;
    else
        return 0;
    end if;
exception
    when no_data_found then
        return 0;
end is_jugada_term;

--!insertar registros de PM Panorama en tabla temporal para usarlo en la validacion de las jugadas
procedure ins_pm_panorama(pv_str       varchar2) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_pm_panrama';	
	CV$GAMBLING_TYPE		constant varchar2(5)  := 'mrtr';
	CV$CONTEXT				constant varchar2(30) := 'PM_PANORAMA';
	CV$CODE				    constant varchar2(30) := 'PARES_INPARES';
begin

	if pv_str is not null then
		insert into olap_sys.w_lookups_fs(seq_id
		                                , gambling_type
									    , context
									    , code
									    , attribute3
										, status
										, creation_date
										, created_by)
		values (olap_sys.w_lookups_fs_seq.nextval
		      , CV$GAMBLING_TYPE
			  , CV$CONTEXT
              , CV$CODE
			  , pv_str
			  , 'A'
			  , sysdate
			  , user);
		commit;	  
	end if;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
end ins_pm_panorama;


--!filtrar las parejas de numeros primos dependiendo del operador
function pm_filtrar_pareja_primos(pn_comb1  number
							    , pn_comb2  number
							    , pn_comb3  number
							    , pn_comb4  number
							    , pn_comb5  number
							    , pn_comb6  number
							    , pv_operador varchar2 default '>'
								, pv_dis_filtrar_pareja_primos varchar2 default 'N') return varchar2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'pm_filtrar_pareja_primos';	
	ln$primo_ini		number(2) := 0;
	ln$primo_end		number(2) := 0;
begin
	
	if pv_dis_filtrar_pareja_primos = 'Y' then
		return pv_dis_filtrar_pareja_primos;
	else	
		g_data_found := 0;
		
		--!encontrar los numeros primos en la jugada
		with numeros_primos_tbl as (
		select lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb1),1,pn_comb1,null),2,'0')
			|| lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb2),1,pn_comb2,null),2,'0')
			|| lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb3),1,pn_comb3,null),2,'0')
			|| lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb4),1,pn_comb4,null),2,'0')
			|| lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb5),1,pn_comb5,null),2,'0')
			|| lpad(decode(olap_sys.w_common_pkg.is_prime_number(pn_comb6),1,pn_comb6,null),2,'0') primos
		  from dual
		) select to_number(substr(primos,1,2)) primo_ini
			   , (substr(primos,3,2)) primo_end
			into ln$primo_ini
			   , ln$primo_end
			from numeros_primos_tbl;

		if pv_operador = '>' then
	/*		with parejas_primos_ini_tbl as (
			select drawing_id, (select max(gambling_id) from olap_sys.sl_gamblings) max_id, (select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id resultado_cnt     
			  from olap_sys.pm_parejas_primos
			 where 1=1
			   and diferencia > 0
			   and primo_ini = ln$primo_ini
			), percentil_tbl as ( select percentile_cont (0.1) within group (order by resultado_cnt) perc_ini
				   , percentile_cont (0.9) within group (order by resultado_cnt) perc_end
				from parejas_primos_ini_tbl
			), parejas_primos_tbl as (
			select primo_ini, primo_fin, drawing_id, (select max(gambling_id) from olap_sys.sl_gamblings) max_id, (select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id resultado_cnt 
			  from olap_sys.pm_parejas_primos
			 where 1=1
			   and diferencia > 0
			   and primo_ini = ln$primo_ini 
			) select count(1)
				into g_data_found
				from parejas_primos_tbl
			   where resultado_cnt between (select perc_ini from percentil_tbl) and (select perc_end from percentil_tbl)
				 and primo_ini = ln$primo_ini
				 and primo_fin = ln$primo_end; */
			
			select count(1) cnt
			  into g_data_found
			  from olap_sys.w_lookups_fs
			 where context = 'PM_NUMEROS_PRIMOS'
			   and code = 'MAYOR'
			   and attribute1 = ln$primo_ini
			   and attribute2 = ln$primo_end;	   
		else
	/*		with parejas_primos_ini_tbl as (
			select drawing_id, (select max(gambling_id) from olap_sys.sl_gamblings) max_id, (select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id resultado_cnt     
			  from olap_sys.pm_parejas_primos
			 where 1=1
			   and diferencia < 0
			   and primo_ini = ln$primo_ini
			), percentil_tbl as ( select percentile_cont (0.1) within group (order by resultado_cnt) perc_ini
				   , percentile_cont (0.9) within group (order by resultado_cnt) perc_end
				from parejas_primos_ini_tbl
			), parejas_primos_tbl as (
			select primo_ini, primo_fin, drawing_id, (select max(gambling_id) from olap_sys.sl_gamblings) max_id, (select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id resultado_cnt 
			  from olap_sys.pm_parejas_primos
			 where 1=1
			   and diferencia < 0
			   and primo_ini = ln$primo_ini 
			) select count(1)
				into g_data_found
				from parejas_primos_tbl
			   where resultado_cnt between (select perc_ini from percentil_tbl) and (select perc_end from percentil_tbl)
				 and primo_ini = ln$primo_ini
				 and primo_fin = ln$primo_end; */		
		
			select count(1) cnt
			  into g_data_found
			  from olap_sys.w_lookups_fs
			 where context = 'PM_NUMEROS_PRIMOS'
			   and code = 'MENOR'
			   and attribute1 = ln$primo_ini
			   and attribute2 = ln$primo_end;
		end if;
		


		if g_data_found > 0 then
			return 'Y';
		else
			return 'N';
		end if;
	end if;
exception
  when others then
    return 'N';
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end pm_filtrar_pareja_primos;

--!cargar todos los resultados
procedure carga_resultados (pn_anio_ini         in number
                          , xtbl$resultados		in out nocopy g_resultado_tbl                      
						  , x_err_code  		in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'carga_resultados';
begin
	select gambling_id, comb1, comb2, comb3, comb4, comb5, comb6
      bulk collect into xtbl$resultados
	  from olap_sys.sl_gamblings
	 where to_number(to_char(to_date(gambling_date,'DD-MM-YYYY'),'YYYY')) >= pn_anio_ini 
     order by gambling_id;
	
dbms_output.put_line(xtbl$resultados.count||'  registros historicos cargados');	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end carga_resultados;

--!inicializa contador
procedure inicializa_contador(xtbl$contador		in out nocopy g_contador_tbl
						    , x_err_code  		in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'inicializa_contador';
begin
	xtbl$contador.delete;

	for k in 1..39 loop
		xtbl$contador(k).digito := k;
		xtbl$contador(k).cnt := 0;
		xtbl$contador(k).ultimo_id := 0;
	end loop;	
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end inicializa_contador;

--!buscar digito siguiente
procedure busca_digito_siguiente (pn_index                        number
								, pn_posicion     				  number
							    , ptbl$resultados		          g_resultado_tbl
							    , xtbl$contador		in out nocopy g_contador_tbl					
							    , x_err_code  		in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'busca_digito_siguiente';
begin
--	dbms_output.put_line(LV$PROCEDURE_NAME);
--	dbms_output.put_line('index: '||pn_index);
--	dbms_output.put_line('posicion: '||pn_posicion);

	for t in pn_index..pn_index loop
		if pn_posicion = 1 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos1 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;
		
		if pn_posicion = 2 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos2 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;

		if pn_posicion = 3 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos3 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;
		
		if pn_posicion = 4 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos4 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;		

		if pn_posicion = 5 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos5 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;		

		if pn_posicion = 6 then
			for c in xtbl$contador.first..xtbl$contador.last loop
				if xtbl$contador(c).digito = ptbl$resultados(t).pos6 then
					xtbl$contador(c).cnt := xtbl$contador(c).cnt + 1;
					xtbl$contador(c).ultimo_id := ptbl$resultados(t).id;
				end if;
			end loop;
		end if;		
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end busca_digito_siguiente;

--!buscar digito indicado
procedure busca_digito (pn_digito						  number
					  , pn_posicion     				  number
					  , ptbl$resultados		              g_resultado_tbl
					  , xtbl$contador		in out nocopy g_contador_tbl					
					  , x_err_code  		in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'busca_digito';
	ln$index				number := 0;
	ln$match_cnt			number := 0;
begin
--	dbms_output.put_line(LV$PROCEDURE_NAME);
--	dbms_output.put_line('digito: '||pn_digito);
--	dbms_output.put_line('posicion: '||pn_posicion);
	
	g_rowcnt := 0;
	
	for k in ptbl$resultados.first..ptbl$resultados.last loop
		ln$index := 0;
--		dbms_output.put_line('index: '||k||' resultado: '||ptbl$resultados(k).pos1||' id: '||ptbl$resultados(k).id);
		if k < ptbl$resultados.last then
			if pn_posicion = 1 then
				if ptbl$resultados(k).pos1 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
--dbms_output.put_line('ln$index: '||ln$index||' g_rowcnt: '||g_rowcnt);					
				end if;
			end if;

			if pn_posicion = 2 then
				if ptbl$resultados(k).pos2 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
				end if;
			end if;

			if pn_posicion = 3 then
				if ptbl$resultados(k).pos3 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
				end if;
			end if;

			if pn_posicion = 4 then
				if ptbl$resultados(k).pos4 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
				end if;
			end if;			

			if pn_posicion = 5 then
				if ptbl$resultados(k).pos5 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
				end if;
			end if;

			if pn_posicion = 6 then
				if ptbl$resultados(k).pos6 = pn_digito then
					ln$index := k;
					g_rowcnt := g_rowcnt + 1;
				end if;
			end if;			
			
			--!buscar digito siguiente
			if ln$index > 0 then
				busca_digito_siguiente (pn_index        => ln$index+1
									  , pn_posicion     => pn_posicion
									  , ptbl$resultados	=> ptbl$resultados
									  , xtbl$contador	=> xtbl$contador					
									  , x_err_code  	=> x_err_code);
			end if;					  
		end if;	
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end busca_digito;

--!recuperar el ID ultimo sorteo
function get_max_drawing_id (pv_drawing_type             VARCHAR2 default 'mrtr') return number is
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

--!imprimir resultados
procedure imprime_resultados(pn_digito		    number
						   , pn_posicion        number
						   , pn_anio_ini        number
						   , ptbl$contador		g_contador_tbl) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'imprime_resultados';
	ltbl$resultados			g_resultado_tbl;	
	ltbl$contador			g_contador_tbl;
	ln$err_code				number := -1;
	ln$ultimo_id			number := 0;
	ln$diferencia_sorteos   number := 0;
	
	cursor c_digito_siguiente is
	select lpad(substr(valor,1,instr(valor,'-',1,1)-1),2,'0') digito
		 , lpad(substr(valor,instr(valor,'-',1,1)+1, instr(valor,'-',1,2) - instr(valor,'-',1,1)-1),3,'0') contador
		 , substr(valor,instr(valor,'-',1,2)+1, instr(valor,'-',1,3) - instr(valor,'-',1,2)-1) ultimo_id
		 , lpad(substr(valor,instr(valor,'-',1,3)+1),3,'0') diferencia
	  from olap_sys.tmp_testing
	 order by to_number(substr(valor,instr(valor,'-',1,1)+1, instr(valor,'-',1,2) - instr(valor,'-',1,1)-1)) desc
		 , to_number(substr(valor,instr(valor,'-',1,3)+1)) desc;	
begin
	--!limpiando la tabla
	delete olap_sys.tmp_testing;
	
	--!obtener el ultimo ID del resultado
	ln$ultimo_id := get_max_drawing_id;

	dbms_output.put_line('ultimo sorteo: '||ln$ultimo_id);
	dbms_output.put_line('digito: '||pn_digito||'('||g_rowcnt||'), posicion: '||pn_posicion||', anio: '||pn_anio_ini);
	
	--!guardando los datos en una tabla para poder ordenarlos despues
	for r in ptbl$contador.first..ptbl$contador.last loop
		if ptbl$contador(r).cnt > 0 then
			--dbms_output.put_line(ptbl$contador(r).digito||' - '||ptbl$contador(r).cnt||' - '||ptbl$contador(r).ultimo_id);
			--!calcular la diferencia entre el ultimo sorteo y el ultimo resultado
			ln$diferencia_sorteos := ln$ultimo_id-ptbl$contador(r).ultimo_id;
			insert into olap_sys.tmp_testing(valor)
			values(ptbl$contador(r).digito||'-'||ptbl$contador(r).cnt||'-'||ptbl$contador(r).ultimo_id||'-'||ln$diferencia_sorteos);
		end if;
	end loop;
	commit;
	
	--!imprimiendo resultados
	for r in c_digito_siguiente loop
		dbms_output.put_line(r.digito||' - '||r.contador||' - '||r.ultimo_id||' - '||r.diferencia);
	end loop;
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end imprime_resultados;

--!cuentador de las ocurrencias de los digitos siguientes a digito indicado
procedure cuenta_sig_digito(pn_digito		number
						  , pn_posicion     number
						  , pn_anio_ini     number default 2010) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'cuenta_sig_digito';
	ltbl$resultados			g_resultado_tbl;	
	ltbl$contador			g_contador_tbl;
	ln$err_code				number := -1;
begin
	--!cargar todos los resultados
	carga_resultados (pn_anio_ini     => pn_anio_ini
	                , xtbl$resultados => ltbl$resultados
					, x_err_code  	  => ln$err_code);

	--!inicializa contador
	inicializa_contador(xtbl$contador => ltbl$contador
					  , x_err_code    => ln$err_code);
					  
	--!buscar digito indicado
	busca_digito (pn_digito			=> pn_digito
			    , pn_posicion     	=> pn_posicion
			    , ptbl$resultados	=> ltbl$resultados
			    , xtbl$contador		=> ltbl$contador
			    , x_err_code  		=> ln$err_code);		

	--!imprimir resultados
	imprime_resultados(pn_digito	 => pn_digito
			         , pn_posicion   => pn_posicion
					 , pn_anio_ini   => pn_anio_ini
				     , ptbl$contador => ltbl$contador);
				
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end cuenta_sig_digito;	

--!insertar los numeros extremos en la tabla de plan_jugada_details
procedure ins_numeros_extremos(pn_drawing_case 	 number
							 , pv_pos1			 varchar2
							 , pv_pos6			 varchar2
							  ) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_numeros_extremos_handler';
begin
	insert into olap_sys.plan_jugada_details (drawing_type
											, plan_jugada_id
											, id
											, description
											, pos1
											, pos6
											, created_by
											, creation_date)
	values ('mrtr'
		  , (select id from olap_sys.plan_jugadas where description = 'DECENAS' and drawing_case = pn_drawing_case)
		  , (select nvl(max(id),0) + 1 from olap_sys.plan_jugada_details)
		  , 'EXCLUIR_EXTREMOS'
		  , pv_pos1
		  , pv_pos6
		  , USER
		  , SYSDATE);
    g_inscnt := g_inscnt + 1;		  
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end;

--!insertar los numeros extremos para usarlos despues como filtro para generar las jugadas
procedure ins_numeros_extremos_handler is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'ins_numeros_extremos_handler';
	CF$PERCENTIL_JCNT		constant float := 0.3;
	CF$PERCENTIL_INTER		constant float := 0.4;

	cursor c_main (pn_drawing_case   number) is
	with J_EXTREMOS_TBL as (  
	select case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7  				  
		   end drawing_case
		 , pn_cnt
		 , comb1
		 , comb6
		 , (comb6 - comb1) - 1 intermedios
		 , pn1
		 , pn6
		 , COUNT(1) jcnt
	  from olap_sys.w_combination_responses_fs   
	 where PN_CNT = 2
	   and status = 'Y'
	   and (D1, D2, D3, D4, D5, D6) IN  (
										  ('1-9','1-9','10-19','10-19','20-29','30-39') --1
										 ,('1-9','1-9','10-19','20-29','20-29','30-39') --2
										 ,('1-9','10-19','10-19','20-29','20-29','30-39') --3
										 ,('1-9','10-19','10-19','20-29','30-39','30-39') --4
										 ,('1-9','10-19','20-29','20-29','30-39','30-39') --5
										 ,('1-9','1-9','10-19','20-29','30-39','30-39') --6
										 ,('1-9','10-19','20-29','20-29','20-29','30-39') --7
										)
	 group by case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7				  
			  end
			, pn_cnt
			, pn1
			, pn6        
			, comb1
			, comb6
	), R_EXTREMOS_TBL as (  
	select case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7				  
		   end drawing_case
		 , pn_cnt
		 , comb1
		 , comb6
		 , olap_sys.w_common_pkg.is_prime_number(comb1) pn1
		 , olap_sys.w_common_pkg.is_prime_number(comb6) pn6
		 , count(1) rcnt
		 , max(gambling_id) max_rid
	   from olap_sys.pm_mr_resultados_v2
	  where PN_CNT = 2
		and (D1, D2, D3, D4, D5, D6) IN  (
										  ('1-9','1-9','10-19','10-19','20-29','30-39') --1
										 ,('1-9','1-9','10-19','20-29','20-29','30-39') --2
										 ,('1-9','10-19','10-19','20-29','20-29','30-39') --3
										 ,('1-9','10-19','10-19','20-29','30-39','30-39') --4
										 ,('1-9','10-19','20-29','20-29','30-39','30-39') --5
										 ,('1-9','1-9','10-19','20-29','30-39','30-39') --6
										 ,('1-9','10-19','20-29','20-29','20-29','30-39') --7
										)
	 group by case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7
			  end
			, pn_cnt
			, olap_sys.w_common_pkg.is_prime_number(comb1)
			, olap_sys.w_common_pkg.is_prime_number(comb6)        
			, comb1
			, comb6    
	), R_MAX_GAMBLING_TBL as (  
	select case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6 
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7
		   end drawing_case
		 , max(gambling_id) max_rid
	   from olap_sys.pm_mr_resultados_v2
	  where PN_CNT = 2
		and (D1, D2, D3, D4, D5, D6) IN  (
										  ('1-9','1-9','10-19','10-19','20-29','30-39') --1
										 ,('1-9','1-9','10-19','20-29','20-29','30-39') --2
										 ,('1-9','10-19','10-19','20-29','20-29','30-39') --3
										 ,('1-9','10-19','10-19','20-29','30-39','30-39') --4
										 ,('1-9','10-19','20-29','20-29','30-39','30-39') --5
										 ,('1-9','1-9','10-19','20-29','30-39','30-39') --6
										 ,('1-9','10-19','20-29','20-29','20-29','30-39') --7
										)
	 group by case   when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '10-19' and D5 = '20-29' and D6 = '30-39' then 1 
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 2
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 3              
				  when D1 = '1-9' and D2 = '10-19' and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 4
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 5
				  when D1 = '1-9' and D2 = '1-9'   and D3 = '10-19' and D4 = '20-29' and D5 = '30-39' and D6 = '30-39' then 6
				  when D1 = '1-9' and D2 = '10-19' and D3 = '20-29' and D4 = '20-29' and D5 = '20-29' and D6 = '30-39' then 7				  
		   end                                   
	), FINAL_TBL as (
	 select j.drawing_case
		  , j.pn_cnt
		  , j.comb1
		  , j.comb6
		  , j.intermedios inter
		  , j.pn1
		  , j.pn6
		  , j.jcnt
		  , nvl((select r.rcnt from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0) rcnt
		  , nvl((select r.max_rid from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0) rid
		  , (select r.max_rid from R_MAX_GAMBLING_TBL R where r.drawing_case = j.drawing_case) max_drid
		  , decode(nvl((select r.max_rid from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0),0,null,(select r.max_rid from R_MAX_GAMBLING_TBL R where r.drawing_case = j.drawing_case) - nvl((select r.max_rid from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0)) dif_rid
		  , (select max(gambling_id) from olap_sys.pm_mr_resultados_v2) max_rid
		  , decode(nvl((select r.max_rid from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0),0,null,(select max(gambling_id) from olap_sys.pm_mr_resultados_v2) - nvl((select r.max_rid from R_EXTREMOS_TBL R where r.drawing_case = j.drawing_case and r.comb1 = j.comb1 and r.comb6 = j.comb6 and r.pn1 = j.pn1 and r.pn6 = j.pn6),0)) total_dif
		from j_extremos_tbl j
	  where drawing_case = pn_drawing_case
	) select drawing_case
           , comb1
		   , comb6
			from FINAL_TBL
		   where jcnt >= (select percentile_cont (CF$PERCENTIL_JCNT) within group (order by jcnt) from FINAL_TBL)
			 --and inter >= (select percentile_cont (CF$PERCENTIL_INTER) within group (order by inter) from FINAL_TBL)
			 and rcnt > 0   
	  order by drawing_case
	      , comb1
		  , comb6;	
begin
    --eliminar los numeros extremos de la tabla
	delete olap_sys.plan_jugada_details where description = 'EXCLUIR_EXTREMOS';
	
	g_inscnt := 0;
	for d in 1..7 loop
		for k in c_main (pn_drawing_case => d) loop
			--!insertar los numeros extremos en la tabla de plan_jugada_details
			ins_numeros_extremos(pn_drawing_case => k.drawing_case
							   , pv_pos1		 => to_char(k.comb1)
							   , pv_pos6		 => to_char(k.comb6));		
			
		end loop;
	end loop;
	commit;
	dbms_output.put_line(g_inscnt||' numeros extremos insertados');
exception
  when others then
    dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());	
end ins_numeros_extremos_handler;

--!filtrando jugadas en base a los numeros extremos
function pm_filtrar_extremos(pn_drawing_case number
                           , pn_comb1        number	
                           , pn_comb6        number
						   , pv_dis_filtrar_extremos varchar2) return varchar2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'pm_filtrar_extremos';
begin
	if pv_dis_filtrar_extremos = 'Y' then
		return pv_dis_filtrar_extremos;
	else		
		select count(1)
		  into g_data_found
		  from olap_sys.plan_jugadas pj
			 , olap_sys.plan_jugada_details jd
		 where pj.id = jd.plan_jugada_id
		   and pj.description = 'DECENAS'	  
		   and jd.description = 'EXCLUIR_EXTREMOS'	  
		   and to_number(jd.pos1) = pn_comb1
		   and to_number(jd.pos6) = pn_comb6
		   and pj.drawing_case = pn_drawing_case;
		
		if g_data_found > 0 then
			return 'Y';
		else
			return 'N';
		end if;
	end if;

exception
  when others then
    return 'N';
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
end pm_filtrar_extremos;

--!validar el ciclo de aparicion superior se encuentre dentro del rango del plan de jugadas
function is_plan_jugada_ca_valido (pn_drawing_case  	number
							     , pn_sum_ca			number
							      ) return boolean is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'is_plan_jugada_ca_valido';
begin
	select 1	  
	  into g_data_found
	  from olap_sys.plan_jugadas 
	 where description = 'DECENAS' 
	   and status = 'A' 
	   and pn_sum_ca between r_ca_ini and r_ca_end
	   and drawing_case = pn_drawing_case;
--	dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
--	dbms_output.put_line('pn_sum_ca: '||pn_sum_ca);   
	return true;
exception
  when no_data_found then
	return false;
  when others then
	return false;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end is_plan_jugada_ca_valido;

--!validar que el ciclo de aparicion de cada posicion se encuentre dentro del rango de la tabla 
function is_ciclo_aparicion_valido(pn_drawing_case  	number
							     , pn_ca1				number
								 , pn_ca2				number 
								 , pn_ca3				number
								 , pn_ca4				number
								 , pn_ca5				number
								 , pn_ca6				number
								 , pn_ca_no_match_cnt  	number) return boolean is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'is_ciclo_aparicion_valido';
	ln$match_cnt			number := 0;
	ln$ca_cnt				number := 0;
begin
/*	dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
	dbms_output.put_line('pn_ca1: '||pn_ca1);
	dbms_output.put_line('pn_ca2: '||pn_ca2);
	dbms_output.put_line('pn_ca3: '||pn_ca3);
	dbms_output.put_line('pn_ca4: '||pn_ca4);
	dbms_output.put_line('pn_ca5: '||pn_ca5);
	dbms_output.put_line('pn_ca6: '||pn_ca6);*/
	
	select count(1) cnt
	  into ln$ca_cnt
	  from olap_sys.plan_jugada_ciclos_aparicion ca
		 , olap_sys.plan_jugadas pj
	 where ca.plan_jugada_id  = pj.id
	   and pj.description= 'DECENAS' 
	   and pj.status = 'A'
	   and pj.drawing_case = pn_drawing_case;
--	dbms_output.put_line('ln$ca_cnt: '||ln$ca_cnt);   
	if ln$ca_cnt > 0 then
		--!inicializando la variable
		g_rowcnt := 0;		
		--!B1
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B1'
			   and pn_ca1 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;			   
			ln$match_cnt := ln$match_cnt + 1;
			dbms_output.put_line('pn_ca1: '||pn_ca1); 			
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;	

		--!inicializando la variable
		g_rowcnt := 0;		
		--!B2
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B2'
			   and pn_ca2 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;
			ln$match_cnt := ln$match_cnt + 1;
--			dbms_output.put_line('pn_ca2: '||pn_ca2); 			
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;

		--!inicializando la variable
		g_rowcnt := 0;
		--!B3
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B3'
			   and pn_ca3 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;
			ln$match_cnt := ln$match_cnt + 1;  
--			dbms_output.put_line('pn_ca3: '||pn_ca3);
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;

		--!inicializando la variable
		g_rowcnt := 0;
		--!B4
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B4'
			   and pn_ca4 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;
			ln$match_cnt := ln$match_cnt + 1;   
--			dbms_output.put_line('pn_ca4: '||pn_ca4);			
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;		

		--!inicializando la variable
		g_rowcnt := 0;
		--!B5
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B5'
			   and pn_ca5 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;
			ln$match_cnt := ln$match_cnt + 1;   
--			dbms_output.put_line('pn_ca5: '||pn_ca5);			
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;

		--!inicializando la variable
		g_rowcnt := 0;		
		--!B6
		begin
			select 1
			  into g_rowcnt
			  from olap_sys.plan_jugada_ciclos_aparicion ca
				 , olap_sys.plan_jugadas pj
			 where ca.plan_jugada_id  = pj.id
			   and pj.description= 'DECENAS' 
			   and pj.status = 'A'
			   and ca.b_type = 'B6'
			   and pn_ca6 between ca_ini and ca_end
   			   and pj.drawing_case = pn_drawing_case;
			ln$match_cnt := ln$match_cnt + 1;   
--			dbms_output.put_line('pn_ca6: '||pn_ca6);			
		exception
			when no_data_found then
				g_rowcnt := 0;
		end;
--dbms_output.put_line('ln$match_cnt: '||ln$match_cnt);
		--!todos los ca hacen match con los rangos o al manos un ca puede no hacer match
		if ln$match_cnt >= (ln$ca_cnt - pn_ca_no_match_cnt) then
--			dbms_output.put_line('<<true>>');
			return true;
--		--!al manos un ca puede no hacer match	
--		elsif (ln$match_cnt -1) = ln$ca_cnt then
--			return true;
		--!mas de un ca no hacen match	
		else
--			dbms_output.put_line('<<false>>');
			return false;
		end if;	
	else
		return true;
	end if;
exception
  when no_data_found then
	return false;
  when others then
	return false;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end is_ciclo_aparicion_valido;


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
							 , pn_ca_no_match_cnt   number default 1) return varchar2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'gl_filtrar_ca_handler';
begin	
	--!validar el ciclo de aparicion superior se encuentre dentro del rango del plan de jugadas
	if is_plan_jugada_ca_valido (pn_drawing_case => pn_drawing_case
					           , pn_sum_ca	     => pn_sum_ca) then
		--!validar que el ciclo de aparicion de cada posicion se encuentre dentro del rango de la tabla 
		if pv_look_ca_pos = 'Y' and is_ciclo_aparicion_valido(pn_drawing_case    => pn_drawing_case
													    , pn_ca1			 => pn_ca1
													    , pn_ca2			 => pn_ca2
													    , pn_ca3			 => pn_ca3
													    , pn_ca4			 => pn_ca4
													    , pn_ca5			 => pn_ca5
													    , pn_ca6			 => pn_ca6
													    , pn_ca_no_match_cnt => pn_ca_no_match_cnt) then
			return 'Y';
		else
			return 'N';
		end if;	
    else
		return 'N';
	end if;	
end gl_filtrar_ca_handler;


--!recuperar un patron numerico en base al parametro de entrada y a la posicion de los digitos
function get_string_pattern(pn_comb1  number
						  , pn_comb2  number
						  , pn_comb3  number
						  , pn_comb4  number
						  , pn_comb5  number
						  , pn_comb6  number
						  --!PR=Primo, I=Inpar, P=Par 
						  , pv_type varchar2 default 'PR') return VARCHAR2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_string_pattern';
begin
	if pv_type in ('PR','P','I') then
		with calculos_tbl as (
			select case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb1) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb1) = 0 and mod(pn_comb1,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb1) = 0 and mod(pn_comb1,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB1
				 , case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb2) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb2) = 0 and mod(pn_comb2,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb2) = 0 and mod(pn_comb2,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB2       
				 , case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb3) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb3) = 0 and mod(pn_comb3,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb3) = 0 and mod(pn_comb3,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB3 
				 , case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb4) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb4) = 0 and mod(pn_comb4,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb4) = 0 and mod(pn_comb4,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB4
				 , case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb5) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb5) = 0 and mod(pn_comb5,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb5) = 0 and mod(pn_comb5,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB5 
				 , case when pv_type = 'PR' then case when olap_sys.w_common_pkg.is_prime_number(pn_comb6) = 1 then 'Y' else 'N' end else 
				   case when pv_type = 'P'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb6) = 0 and mod(pn_comb6,2) = 0 then 'Y' else 'N' end else  
				   case when pv_type = 'I'  then case when olap_sys.w_common_pkg.is_prime_number(pn_comb6) = 0 and mod(pn_comb6,2) = 1 then 'Y' else 'N' end else 'N' 
				   end end end PCOMB6           
			  FROM dual
		) select PCOMB1 || PCOMB2 || PCOMB3 || PCOMB4 || PCOMB5 || PCOMB6 npattern
			into g_pattern
			from calculos_tbl;
	else
		g_pattern := NULL;
	end if;
	return g_pattern;
exception	
  when others then
	return NULL;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;		
end get_string_pattern;


--!obtener la suma de incidencias de numeros primos en base al case 
procedure get_primos_suma(pn_drawing_case  				  number
						, xn_percentile     in out nocopy number
						, xtbl_primos_sum	in out nocopy g_primos_sum_tbl
						, x_err_code    	in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'get_primos_suma';
	cursor c_primos_sum (pn_drawing_case		  number) is
	select jd.pos1
	     , jd.pos2
		 , jd.pos3
		 , jd.pos4
		 , jd.pos5
		 , jd.pos6
		 , jd.seq_no percentile
	  from olap_sys.plan_jugada_details jd
		 , olap_sys.plan_jugadas pj 
	 where pj.drawing_type = jd.drawing_type 
	   and pj.id = jd.plan_jugada_id 
	   and jd.status = 'A'
	   and pj.status = 'A'
	   and jd.description = 'PRIMOS_SUMA'
	   and pj.drawing_case = pn_drawing_case;
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
	x_err_code := -1;
	
	for k in c_primos_sum (pn_drawing_case => pn_drawing_case) loop 	
		xtbl_primos_sum(1).primo_sum := k.pos1;
		--xtbl_primos_sum(1).bandera := 'N';
		xtbl_primos_sum(2).primo_sum := k.pos2;
		--xtbl_primos_sum(2).bandera := 'N';
		xtbl_primos_sum(3).primo_sum := k.pos3;
		--xtbl_primos_sum(3).bandera := 'N';
		xtbl_primos_sum(4).primo_sum := k.pos4;
		--xtbl_primos_sum(4).bandera := 'N';
		xtbl_primos_sum(5).primo_sum := k.pos5;
		--xtbl_primos_sum(5).bandera := 'N';
		xtbl_primos_sum(6).primo_sum := k.pos6;
		--xtbl_primos_sum(6).bandera := 'N';
		xn_percentile := k.percentile;
		dbms_output.put_line('encontro suma');
	end loop;	
	
	dbms_output.put_line('xtbl_primos_sum.count: '||xtbl_primos_sum.count);
	
	if xtbl_primos_sum.count = 0 then		
		x_err_code := GN$FAILED_EXECUTION;		
	else
		x_err_code := GN$SUCCESSFUL_EXECUTION;
	end if;	
	dbms_output.put_line('x_err_code: '||x_err_code);
exception	
  when others then
	x_err_code := GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;		
end get_primos_suma;


--!marcar las posiciones que sean mayores al percentile
procedure marcar_primos_posiciones (pn_percentile				  number
								  , xtbl_primos_sum	in out nocopy g_primos_sum_tbl
								  , x_err_code    	in out nocopy number) is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'marcar_primos_posiciones';
begin
	dbms_output.put_line(LV$PROCEDURE_NAME);
	dbms_output.put_line('pn_percentile: '||pn_percentile);
	for t in xtbl_primos_sum.first..xtbl_primos_sum.last loop
		if xtbl_primos_sum(t).primo_sum > pn_percentile then
			xtbl_primos_sum(t).bandera := 'Y';
		end if;	

		dbms_output.put_line('primo('||t||').sum : '||xtbl_primos_sum(t).primo_sum||' bandera: ' ||xtbl_primos_sum(t).bandera);
	end loop;	
	
	for i in xtbl_primos_sum.first..xtbl_primos_sum.last loop
		dbms_output.put_line('primos_sum: '||xtbl_primos_sum(i).primo_sum);
		dbms_output.put_line('bandera: '||xtbl_primos_sum(i).bandera);
	end loop;	
	x_err_code := GN$SUCCESSFUL_EXECUTION;	
exception	
  when others then
	x_err_code := GN$FAILED_EXECUTION;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end marcar_primos_posiciones;

--!filtar las jugadas que no concuerden con las posiciones de primos calculadas
function filtrar_primos_jugadas(pn_primo1  			number
							  , pn_primo2  			number
							  , pn_primo3  			number
							  , pn_primo4  			number
							  , pn_primo5  			number
							  , pn_primo6  			number
							  , ptbl_primos_sum	    g_primos_sum_tbl
							   ) return boolean is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'filtrar_primos_jugadas';
	CN$NUMERO_PRIMO         constant number(1) := 1;
	CV$POSICION_VALIDA      constant varchar2(1) := 'Y';
	ln$match_cnt					 number := 0;	
begin
	dbms_output.put_line('pn_primo1: '||pn_primo1);
	dbms_output.put_line('pn_primo2: '||pn_primo2);
	dbms_output.put_line('pn_primo3: '||pn_primo3);
	dbms_output.put_line('pn_primo4: '||pn_primo4);
	dbms_output.put_line('pn_primo5: '||pn_primo5);
	dbms_output.put_line('pn_primo6: '||pn_primo6);
	
	--!esta posicion se da por confirmada como match
	--if ptbl_primos_sum(1).bandera = CV$POSICION_VALIDA and pn_primo1 = CN$NUMERO_PRIMO then
	--	ln$match_cnt := ln$match_cnt + 1;
	--end if;	

	if ptbl_primos_sum(2).bandera = CV$POSICION_VALIDA and pn_primo2 = CN$NUMERO_PRIMO then
		ln$match_cnt := ln$match_cnt + 1;
	end if;	

	if ptbl_primos_sum(3).bandera = CV$POSICION_VALIDA and pn_primo3 = CN$NUMERO_PRIMO then
		ln$match_cnt := ln$match_cnt + 1;
	end if;	

	if ptbl_primos_sum(4).bandera = CV$POSICION_VALIDA and pn_primo4 = CN$NUMERO_PRIMO then
		ln$match_cnt := ln$match_cnt + 1;
	end if;	

	if ptbl_primos_sum(5).bandera = CV$POSICION_VALIDA and pn_primo5 = CN$NUMERO_PRIMO then
		ln$match_cnt := ln$match_cnt + 1;
	end if;	

	if ptbl_primos_sum(6).bandera = CV$POSICION_VALIDA and pn_primo6 = CN$NUMERO_PRIMO then
		ln$match_cnt := ln$match_cnt + 1;
	end if;		
	
	if ln$match_cnt = 0 then
		return false;
	else	
		return true;
	end if;	
exception	
  when others then
	return false;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;
end filtrar_primos_jugadas;							  

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
									  , pn_primos_cnt		number default 2
									  ) return varchar2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'pm_filtrar_primos_por_posicion';
	ln$percentile			number := 0;
	ln$err_code				number := -1;
	ltbl_primos_sum			g_primos_sum_tbl;
begin
	
	if pv_filtrar_primos_por_posicion = 'N' then
		dbms_output.put_line(LV$PROCEDURE_NAME);
		dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
		dbms_output.put_line('pn_comb1: '||pn_comb1);
		dbms_output.put_line('pn_comb2: '||pn_comb2);
		dbms_output.put_line('pn_comb3: '||pn_comb3);
		dbms_output.put_line('pn_comb4: '||pn_comb4);
		dbms_output.put_line('pn_comb5: '||pn_comb5);
		dbms_output.put_line('pn_comb6: '||pn_comb6);
		dbms_output.put_line('pv_filtrar_primos_por_posicion: '||pv_filtrar_primos_por_posicion);
	end if;
	
	if pv_filtrar_primos_por_posicion = 'Y' then
		dbms_output.put_line('return pv_filtrar_primos_por_posicion');
		return pv_filtrar_primos_por_posicion;
	else	
		dbms_output.put_line('else');
		--!obtener la suma de incidencias de numeros primos en base al case 
		get_primos_suma(pn_drawing_case => pn_drawing_case
					  , xn_percentile   => ln$percentile
					  , xtbl_primos_sum	=> ltbl_primos_sum
					  , x_err_code      => ln$err_code);
		
		if ln$err_code = GN$SUCCESSFUL_EXECUTION then		
				--!marcar las posiciones que sean mayores al percentile
				marcar_primos_posiciones (pn_percentile	  => ln$percentile
										, xtbl_primos_sum => ltbl_primos_sum
										, x_err_code      => ln$err_code);
			
				if ln$err_code = GN$SUCCESSFUL_EXECUTION then
					--!filtar las jugadas que no concuerden con las posiciones de primos calculadas
					if filtrar_primos_jugadas(pn_primo1  	  => is_prime_number (pn_digit => pn_comb1)
											, pn_primo2  	  => is_prime_number (pn_digit => pn_comb2)
											, pn_primo3  	  => is_prime_number (pn_digit => pn_comb3)
											, pn_primo4  	  => is_prime_number (pn_digit => pn_comb4)
											, pn_primo5  	  => is_prime_number (pn_digit => pn_comb5)
											, pn_primo6  	  => is_prime_number (pn_digit => pn_comb6)
											, ptbl_primos_sum => ltbl_primos_sum 
											 ) then
						dbms_output.put_line('return Y');
						return 'Y';	
					else
						dbms_output.put_line('1. return N');
						return 'N';	
					end if;	
				else
					dbms_output.put_line('2. return N');
					return 'N';	
				end if;						
		else
			dbms_output.put_line('3. return N');
			return 'N';		
		end if;	
	end if;
exception	
  when others then
	dbms_output.put_line('4. return N');
	return 'N';
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;		
end pm_filtrar_primos_por_posicion;

								   
--!valida si el numero de ocurrencias de numeros favorables	de 1 a 4 esta en la configuracion de la tabla					  
function gl_is_nf_config_valid(pn_drawing_case		number
						     , pn_sum_nf			number
							 , pn_pre_sum_nf        number
						     , pv_pre1  			varchar2
						     , pv_pre2  			varchar2
						     , pv_pre3  			varchar2
						     , pv_pre4  			varchar2
						     , pv_pre5  			varchar2
						     , pv_pre6  			varchar2
							 , pv_dis_nf_config_valid varchar2) return varchar2 is
	LV$PROCEDURE_NAME       constant varchar2(30) := 'gl_is_nf_config_valid';
begin					 
	if pv_dis_nf_config_valid = 'Y' then
		return pv_dis_nf_config_valid;
	else	
		--!inicializando variable
		g_rowcnt := 0;
		
		if pn_pre_sum_nf >= 5 then
			return 'Y';
		else	
			select count(1) cnt	 
			  into g_rowcnt	 
			  from olap_sys.plan_jugada_details jd
				 , olap_sys.plan_jugadas pj 
			 where pj.drawing_type = jd.drawing_type 
			   and pj.id = jd.plan_jugada_id 
			   and jd.status = 'A'
			   and pj.status = 'A'
			   and jd.description = 'PATRON_NUMEROS_FAVORABLES'
			   and jd.pos1 = nvl(pv_pre1,'.')
			   and jd.pos2 = nvl(pv_pre2,'.')
			   and jd.pos3 = nvl(pv_pre3,'.')
			   and jd.pos4 = nvl(pv_pre4,'.')
			   and jd.pos5 = nvl(pv_pre5,'.')
			   and jd.pos6 = nvl(pv_pre6,'.')
			   and pj.drawing_case = pn_drawing_case
			   and jd.seq_no between 1 and pn_sum_nf;									 
		
			if g_rowcnt = 0 then
				return 'N';
			else	
				return 'Y';
			end if;	
		end if;
	end if;
exception
	when others then
		return 'N';
end gl_is_nf_config_valid;

--!recupera el grupo establecido en la tabla plan_jugada_details.
--!funcion usada por la vista pm_mr_resultados_v2					  
function gl_get_nf_group(pv_pre1  			varchar2
					   , pv_pre2  			varchar2
					   , pv_pre3  			varchar2
					   , pv_pre4  			varchar2
					   , pv_pre5  			varchar2
					   , pv_pre6  			varchar2) return number is

begin
	--!inicializando variable
	g_data_found := 0;
	
	select distinct jd.sort_execution	 
	  into g_data_found	 
	  from olap_sys.plan_jugada_details jd
		 , olap_sys.plan_jugadas pj 
	 where pj.drawing_type = jd.drawing_type 
	   and pj.id = jd.plan_jugada_id 
	   and jd.status = 'A'
	   and pj.status = 'A'
	   and jd.description = 'PATRON_NUMEROS_FAVORABLES'
	   and jd.pos1 = nvl(pv_pre1,'.')
	   and jd.pos2 = nvl(pv_pre2,'.')
	   and jd.pos3 = nvl(pv_pre3,'.')
	   and jd.pos4 = nvl(pv_pre4,'.')
	   and jd.pos5 = nvl(pv_pre5,'.')
	   and jd.pos6 = nvl(pv_pre6,'.');
	
	return g_data_found;
exception
	when no_data_found then
		return null;
	when others then
		return null;
end;

--!insertar en la tabla w_lookups_fs los pares de numeros primos
procedure ins_numeros_primos (pv_code			varchar2
                            , pv_description    varchar2
							, pn_attribute1		number
							, pn_attribute2		number
							, pv_attribute3		varchar2 default null
							, pv_attribute4		varchar2 default null) is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'ins_numeros_primos';
begin
	insert into olap_sys.w_lookups_fs (seq_id
									 , gambling_type
									 , context
									 , code
									 , description
									 , attribute1
									 , attribute2
									 , attribute3
									 , attribute4
									 , status
									 , creation_date
									 , created_by)
	values (olap_sys.w_lookups_fs_seq.nextval
		  , 'mrtr'
		  , 'PM_NUMEROS_PRIMOS'
		  , pv_code
		  , pv_description
		  , pn_attribute1
		  , pn_attribute2
		  , pv_attribute3
		  , pv_attribute4
		  , 'A'
		  , SYSDATE
		  , USER);								 
exception	
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end ins_numeros_primos;


--!insertar en la tabla w_lookups_fs los pares de numeros primos a jugar
procedure ins_numeros_primos_handler is 
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'ins_numeros_primos_handler';
	CF$PORCENTAJE_RESULTADO		constant float := 0.9;
	CF$PORCENTAJE_ESTADISTICA	constant float := 0.3;
	CN$NUMERO_PRIMO             constant number (1) := 1;
	
	cursor c_parejas_primos (pn_primo_ini				number
	                       , pf_porcentaje_resultado	float
						   , pf_porcentaje_estadistica  float
	) is
	with details_tbl as (
	select primo_ini
	     , primo_fin
		 , prob_pareja
		 , estadistica
		 , diferencia
		 , drawing_id
		 , (select max(gambling_id) from olap_sys.sl_gamblings) max_id
		 , (select max(gambling_id) from olap_sys.sl_gamblings) - drawing_id resultado_cnt
		 , drawing_list 
	  from olap_sys.pm_parejas_primos
	 where primo_ini = pn_primo_ini 
	), percentil_tbl as (
	select round(percentile_cont(pf_porcentaje_resultado) within group (order by resultado_cnt) over (partition by primo_ini)) per_res_con 
		 , round(percentile_cont(pf_porcentaje_estadistica) within group (order by estadistica) over (partition by primo_ini)) per_est_con 
	from details_tbl
	) 
	select primo_ini
	     , primo_fin
		 , prob_pareja
		 , estadistica
		 , diferencia
		 , drawing_id
		 , max_id
		 , resultado_cnt
		 , (select distinct per_res_con from percentil_tbl) per_res_con
		 , (select distinct per_est_con from percentil_tbl) per_est_con
	  from details_tbl
	 order by estadistica
	     , resultado_cnt;
begin
	--!limpiando el contexto
	delete olap_sys.w_lookups_fs where context = 'PM_NUMEROS_PRIMOS';
	
	for np in 1..37 loop
		if is_prime_number (pn_digit => np) = CN$NUMERO_PRIMO then
			for k in c_parejas_primos (pn_primo_ini	=> np
			                         , pf_porcentaje_resultado	 => CF$PORCENTAJE_RESULTADO
						             , pf_porcentaje_estadistica => CF$PORCENTAJE_ESTADISTICA) loop
				
				if k.resultado_cnt < k.per_res_con and k.estadistica >= k.per_est_con then
					--!insertar en la tabla w_lookups_fs los pares de numeros primos
					ins_numeros_primos (pv_code		  => 'MAYOR'
									  , pn_attribute1 => k.primo_ini
									  , pn_attribute2 => k.primo_fin
									  , pv_attribute3 => k.drawing_id
									  , pv_attribute4 => k.max_id
									  , pv_description => 'PERC_RES: '||k.per_res_con||', PERC_EST: '||k.per_est_con
									  ); 
				else
					--!insertar en la tabla w_lookups_fs los pares de numeros primos
					ins_numeros_primos (pv_code		  => 'MENOR'
									  , pn_attribute1 => k.primo_ini
									  , pn_attribute2 => k.primo_fin
									  , pv_attribute3 => k.drawing_id
									  , pv_attribute4 => k.max_id
									  , pv_description => 'PERC_RES: '||k.per_res_con||', PERC_EST: '||k.per_est_con
									  ); 
				end if;	
			end loop;
		end if;
	end loop;
	commit;
exception	
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end ins_numeros_primos_handler;


procedure ins_plan_jugada_ca_stats(pn_plan_jugada_id 	number
							     , pn_ca1 				number
							     , pn_ca2_perc_ini		number
							     , pn_ca2_perc_end		number
							     , pn_ca3_perc_ini		number
							     , pn_ca3_perc_end		number
							     , pn_ca4_perc_ini		number
							     , pn_ca4_perc_end		number
							     , pn_ca5_perc_ini		number
							     , pn_ca5_perc_end		number
							     , pn_ca6 				number
							     , pn_rcnt				number)	is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'ins_ca_stats_handler';								 
begin
null;

	insert into olap_sys.plan_jugada_ca_stats (plan_jugada_id
											 , id
											 , ca1
											 , ca2_perc_ini
											 , ca2_perc_end
											 , ca3_perc_ini
											 , ca3_perc_end
											 , ca4_perc_ini
											 , ca4_perc_end
											 , ca5_perc_ini
											 , ca5_perc_end
											 , ca6
											 , rcnt
											 , status
											 , created_by
											 , creation_date)
	values (pn_plan_jugada_id
		 , (select nvl(max(id),0) + 1 from olap_sys.plan_jugada_ca_stats)
		 , pn_ca1
		 , pn_ca2_perc_ini
		 , pn_ca2_perc_end
		 , pn_ca3_perc_ini
		 , pn_ca3_perc_end
		 , pn_ca4_perc_ini
		 , pn_ca4_perc_end
		 , pn_ca5_perc_ini
		 , pn_ca5_perc_end
		 , pn_ca6
		 , pn_rcnt
		 , 'A'
		 , USER
		 , SYSDATE);
		 
exception	
  when others then
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end ins_plan_jugada_ca_stats;

--!insertar en la tabla plan_jugada_ca_stats las etadisticas de los ciclos de aparicion
procedure ins_ca_stats_handler is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'ins_ca_stats_handler';
	CF$PERCENTILE_INI			constant float := 0.1;
	CF$PERCENTILE_END			constant float := 0.9;
	CN$SORTEO_BASE				constant number := 594;	
	CN$DEFAULT_INI				constant number := 1;
	CN$DEFAULT_END				constant number := 99;	

	--!obtener promedios de CA2, CA3, CA4 y CA5 en base a CA1 y CA6	
	cursor c_ca_stats (pn_sorteo_base		number
					 , pf_percentile_ini	float
					 , pf_percentile_end	float) is	
with ca_tbl as (
select year, gambling_id, nvl(c1_ca,0) ca1, nvl(c2_ca,0) ca2, nvl(c3_ca,0) ca3, nvl(c4_ca,0) ca4, nvl(c5_ca,0) ca5, nvl(c6_ca,0) ca6, sum_ca
  from olap_sys.pm_mr_resultados_v2
 where gambling_id > 594
   and nvl(c1_ca,0) > 0
   and nvl(c6_ca,0) > 0
 order by gambling_id 
), filter_tbl as (
 select ca1
      , ca6
      , count(1) cnt
   from ca_tbl
  where ca1 in (5,7,6,4,9,8,11,13)   
  group by ca1
      , ca6
   order by cnt desc   
) --select avg(cnt) from filter_tbl; --2.74
, ca6_tbl as (
   select *
    from filter_tbl
   where cnt > (select avg(cnt) from filter_tbl)
) --select min(cnt) from ca6_tbl;
, ca1_ca6_tbl as( 
 select ca1
      , ca6
    from ca6_tbl
   where cnt > (select min(cnt) from ca6_tbl) 
), percentile2_tbl as (
select ca1
       , round(percentile_cont(pf_percentile_ini) within group (order by ca2)) ca_ini
       , round(percentile_cont(pf_percentile_end) within group (order by ca2)) ca_end
       , ca6
       , count(1) cnt
  from ca_tbl     
 where (ca1,ca6) in (select ca1,ca6 from ca1_ca6_tbl)   
   and ca2 > 0
  group by ca1
      , ca6  
), percentile3_tbl as (
select ca1
       , round(percentile_cont(pf_percentile_ini) within group (order by ca3)) ca_ini
       , round(percentile_cont(pf_percentile_end) within group (order by ca3)) ca_end
       , ca6
       , count(1) cnt
  from ca_tbl     
 where (ca1,ca6) in (select ca1,ca6 from ca1_ca6_tbl)   
   and ca3 > 0
  group by ca1
      , ca6  
), percentile4_tbl as (
select ca1
       , round(percentile_cont(pf_percentile_ini) within group (order by ca4)) ca_ini
       , round(percentile_cont(pf_percentile_end) within group (order by ca4)) ca_end
       , ca6
       , count(1) cnt
  from ca_tbl     
 where (ca1,ca6) in (select ca1,ca6 from ca1_ca6_tbl)   
   and ca4 > 0
  group by ca1
      , ca6  
), percentile5_tbl as (
select ca1
       , round(percentile_cont(pf_percentile_ini) within group (order by ca5)) ca_ini
       , round(percentile_cont(pf_percentile_end) within group (order by ca5)) ca_end
       , ca6
       , count(1) cnt
  from ca_tbl     
 where (ca1,ca6) in (select ca1,ca6 from ca1_ca6_tbl)   
   and ca5 > 0
  group by ca1
      , ca6  
) --select * from ca_tbl where (ca1,ca6) in (select ca1,ca6 from ca1_ca6_tbl) and ca1 = 4 and ca6 = 6;
  select ft.ca1
       , nvl((select ca_ini from percentile2_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_INI) ca2_ini
       , nvl((select ca_end from percentile2_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_END) ca2_end
       , nvl((select ca_ini from percentile3_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_INI) ca3_ini
       , nvl((select ca_end from percentile3_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_END) ca3_end
       , nvl((select ca_ini from percentile4_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_INI) ca4_ini
       , nvl((select ca_end from percentile4_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_END) ca4_end 
       , nvl((select ca_ini from percentile5_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_INI) ca5_ini
       , nvl((select ca_end from percentile5_tbl pt where pt.ca1 = ft.ca1 and pt.ca6 = ft.ca6),CN$DEFAULT_END) ca5_end        
       , ft.ca6
       , ft.cnt
    from filter_tbl ft
   where (ft.ca1,ft.ca6) in (select ca1,ca6 from ca1_ca6_tbl) 
  order by ft.ca1
      , ft.cnt desc; 	
begin
	delete olap_sys.plan_jugada_ca_stats;
	
	--!iteraciones para las decenas
	for i in 1..7 loop
		--!recuperamos el id de la decena
		for d in olap_sys.w_new_pick_panorama_pkg.c_decenas (pn_drawing_case => i) loop	
			--!obtener promedios de CA2, CA3, CA4 y CA5 en base a CA1 y CA6	
			for ca in c_ca_stats (pn_sorteo_base    => CN$SORTEO_BASE
								, pf_percentile_ini => CF$PERCENTILE_INI
								, pf_percentile_end => CF$PERCENTILE_END) loop
				
				ins_plan_jugada_ca_stats(pn_plan_jugada_id => d.id
									   , pn_ca1 		   => ca.ca1
									   , pn_ca2_perc_ini   => ca.ca2_ini
									   , pn_ca2_perc_end   => ca.ca2_end
									   , pn_ca3_perc_ini   => ca.ca3_ini
									   , pn_ca3_perc_end   => ca.ca3_end
									   , pn_ca4_perc_ini   => ca.ca4_ini
									   , pn_ca4_perc_end   => ca.ca4_end
									   , pn_ca5_perc_ini   => ca.ca5_ini
									   , pn_ca5_perc_end   => ca.ca5_end
									   , pn_ca6 		   => ca.ca6
									   , pn_rcnt		   => ca.cnt);
									   
			end loop;					
		end loop;
	end loop;
	commit;
exception	
  when others then
	rollback;
	dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
	raise;	
end ins_ca_stats_handler;

--!validacion del conteo de inpares y pares por decena
function valida_conteo_inpar_par(pn_drawing_case	number
							   , pn_none_cnt		number
							   , pn_par_cnt			number
							   , pv_val_inpar_par   varchar2 default 'N') return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'valida_conteo_inpar_par';
begin
	--!inicializando variable
	g_data_found := 0;							   
	
	if upper(pv_val_inpar_par) = 'Y' then
		return 'Y';
	else	
		select count(1) cnt
		  into g_data_found
		  from olap_sys.plan_jugada_details jd
			 , olap_sys.plan_jugadas pj 
		 where pj.drawing_type = jd.drawing_type 
		   and pj.id = jd.plan_jugada_id 
		   and jd.status = 'A'
		   and pj.status = 'A'
		   and jd.description = 'CONTEO_INPAR_PAR'
		   and pj.drawing_case = pn_drawing_case
		   and jd.pos1 = pn_none_cnt
		   and jd.pos2 = pn_par_cnt;							   
	
		if g_data_found = 0 then
			return 'N';
		else
			return 'Y';
		end if;	
	end if;
end valida_conteo_inpar_par;

--!validacion de terminaciones por decena
function valida_conteo_terminaciones(pn_drawing_case		number
								   , pn_term1_cnt			number
								   , pn_term2_cnt			number
								   , pv_terminacion_doble   varchar2
								   , pv_val_terminaciones   varchar2 default 'N') return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'valida_conteo_terminaciones';
begin
	--!inicializando variable
	g_data_found := 0;							   
	
	if upper(pv_val_terminaciones) = 'Y' then
		return 'Y';
	else	
		select count(1) cnt
		  into g_data_found
		  from olap_sys.plan_jugada_details jd
			 , olap_sys.plan_jugadas pj 
		 where pj.drawing_type = jd.drawing_type 
		   and pj.id = jd.plan_jugada_id 
		   and jd.status = 'A'
		   and pj.status = 'A'
		   and jd.description = 'CONTEO_TERMINACIONES'
		   and jd.flag1 = pv_terminacion_doble
		   and pj.drawing_case = pn_drawing_case
		   and jd.pos1 = pn_term1_cnt
		   and jd.pos2 = pn_term2_cnt;							   
	
		if g_data_found = 0 then
			return 'N';
		else
			return 'Y';
		end if;	
	end if;
end valida_conteo_terminaciones;

--!devuelve el conteo de los resultados en base a b1, b4, b6 y decena
function get_conteo_b1_b4_b6(pn_drawing_case	number
						   , pn_comb1  			number
						   , pn_comb4  			number
						   , pn_comb6  			number) return number is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_conteo_b1_b4_b6';
begin
/*dbms_output.put_line('pn_drawing_case: '||pn_drawing_case);
dbms_output.put_line('pn_comb1: '||pn_comb1);
dbms_output.put_line('pn_comb4: '||pn_comb4);
dbms_output.put_line('pn_comb6: '||pn_comb6);*/
    g_data_found := 0;
	select jd.resultados_cnt
	  into g_data_found
	  from olap_sys.plan_jugada_details jd
		 , olap_sys.plan_jugadas pj 
	 where pj.drawing_type = jd.drawing_type 
	   and pj.id = jd.plan_jugada_id 
	   and jd.status = 'A'
	   and pj.status = 'A'
	   and jd.description = 'CONTEO_B1_B4_B6'
	   and pj.drawing_case = pn_drawing_case
	   and jd.pos1 = to_char(pn_comb1)
	   and jd.pos4 = to_char(pn_comb4)
	   and jd.pos6 = to_char(pn_comb6);
	
	return g_data_found;	
exception
	when no_data_found then
		return -1;
	when others then
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
		return -2;
end get_conteo_b1_b4_b6;

--!regresa el conteo de las terminaciones en funcion de los resultados
function get_conteo_favorables(pn_drawing_case		number
						     , pv_pre1  			varchar2
						     , pv_pre2  			varchar2
						     , pv_pre3  			varchar2
						     , pv_pre4  			varchar2
						     , pv_pre5  			varchar2
						     , pv_pre6  			varchar2) return number is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_conteo_favorables';
begin							 
	g_data_found := 0;
	select jd.resultados_cnt
	  into g_data_found
	  from olap_sys.plan_jugada_details jd
		 , olap_sys.plan_jugadas pj 
	 where pj.drawing_type = jd.drawing_type 
	   and pj.id = jd.plan_jugada_id 
	   and jd.status = 'A'
	   and pj.status = 'A'
	   and jd.description = 'PATRON_NUMEROS_FAVORABLES'
       and jd.pos1 = pv_pre1
       and jd.pos2 = pv_pre2
       and jd.pos3 = pv_pre3
       and jd.pos4 = pv_pre4
       and jd.pos5 = pv_pre5
       and jd.pos6 = pv_pre6
	   and pj.drawing_case = pn_drawing_case;
	return g_data_found;	
exception
	when no_data_found then
		return -1;
	when others then
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
		return -2;
end get_conteo_favorables;


--!validar que el ca_sum y comb_sum esten dentro del rango calculado
function valida_ca_sum_comb_sum(pn_comb1  		number
							   , pn_comb2  		number
							   , pn_comb3  		number
							   , pn_comb4  		number
							   , pn_comb5  		number
							   , pn_comb6  		number
							   , pv_val_type	varchar2 default 'ALL') return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'valida_ca_sum_comb_sum';
	cursor c_plan_jugada is
	select to_number(pos1) ca_sum_ini
		 , to_number(pos2) ca_sum_end
		 , to_number(pos3) comb_sum_ini
		 , to_number(pos4) comb_sum_end
	  from olap_sys.plan_jugadas
	 where description = 'B1_B4_B6_PERCENTILE_CNT'
	   and drawing_case = 0;	
	   
	cursor c_ca_sum_comb_sum is
	with resultados_tbl as (
	select max(gambling_id) gambling_id from olap_sys.sl_gamblings
	), calculo_stats_tbl as (
	select b_type
	     , digit
		 , ciclo_aparicion ca 
	  from olap_sys.s_calculo_stats cs where cs.drawing_id = (select gambling_id from resultados_tbl)
	), ca1_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B1'  
	   and digit = pn_comb1
	), ca2_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B2'
	   and digit = pn_comb2
	), ca3_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B3'
	   and digit = pn_comb3
	), ca4_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B4'
	   and digit = pn_comb4
	), ca5_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B5'
	   and digit = pn_comb5
	), ca6_tbl as (
	select digit
		 , ca
	  from calculo_stats_tbl
	 where b_type = 'B6'
	   and digit = pn_comb6
	), items_sum_tbl as (
	select nvl((select ca from ca1_tbl),0)
		 + nvl((select ca from ca2_tbl),0)
		 + nvl((select ca from ca3_tbl),0)
		 + nvl((select ca from ca4_tbl),0)
		 + nvl((select ca from ca5_tbl),0)
		 + nvl((select ca from ca6_tbl),0) ca_sum
		 , nvl((select digit from ca1_tbl),0)
		 + nvl((select digit from ca2_tbl),0)
		 + nvl((select digit from ca3_tbl),0)
		 + nvl((select digit from ca4_tbl),0)
		 + nvl((select digit from ca5_tbl),0)
		 + nvl((select digit from ca6_tbl),0) comb_sum
	  from dual   
	)select ca_sum
		  , comb_sum
		from items_sum_tbl;		
begin							 
	for p in c_plan_jugada loop
		dbms_output.put_line('pj ca_sum_ini: '||p.ca_sum_ini||', ca_sum_end: '||p.ca_sum_end||
							 ', pj comb_sum_ini: '||p.comb_sum_ini||', comb_sum_end: '||p.comb_sum_end);
		for c in c_ca_sum_comb_sum loop		
			dbms_output.put_line('gl c.ca_sum: '||c.ca_sum||', c.comb_sum: '||c.comb_sum);
			if pv_val_type = 'ALL' then
				if (c.ca_sum between p.ca_sum_ini and p.ca_sum_end) and
		           (c.comb_sum between p.comb_sum_ini and p.comb_sum_end) then   
					dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|Y');
					return 'Y';
				else
					dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|N');
					return 'N';
				end if;   
			elsif upper(pv_val_type) = 'CA' then
				if c.ca_sum between p.ca_sum_ini and p.ca_sum_end then   
				   dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|Y');
				   return 'Y';
				else
					dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|N');				
					return 'N';
				end if; 
			elsif upper(pv_val_type) = 'CO' then
				if c.comb_sum between p.comb_sum_ini and p.comb_sum_end then   
				   dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|Y');
				   return 'Y';
				else
					dbms_output.put_line(pn_comb1||'|'||pn_comb2||'|'||pn_comb3||'|'||pn_comb4||'|'||pn_comb5||'|'||pn_comb6||'|N');				
					return 'N';
				end if; 			
			end if;
		end loop;
	end loop;
	
exception
	when others then
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
end valida_ca_sum_comb_sum;


--!validar que el ca_sum y comb_sum en base a las resultados ganadores de 5 y 6 aciertos
function valida_ca_sum_comb_sum(pn_comb_sum  		number
							  , pn_ca_sum 		number
							  , pv_val_type	varchar2 default 'ALL') return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'valida_ca_sum_comb_sum';
	CN$VALOR_VALIDO				constant number(1) := 1;
	ln$ca_sum_flag				number(1) := 0;
	ln$comb_sum_flag			number(1) := 0;
begin
	--!percentiles de ciclos de aparicion para b1, b6 y las suma 
	with r_ca_tbl as (
	select nvl(c1_ca,0) ca1,  nvl(c6_ca,0) ca6, sum_ca ca_sum
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id > 594
	   and gl_cnt in (5,6)
	   and comb1 < 10  
	), r_ca_percentile_tbl as (
	select percentile_cont(0.1) within group (order by ca1) r_perc_ca1
		 , percentile_cont(0.1) within group (order by ca6) r_perc_ca6
		 , percentile_cont(0.1) within group (order by ca_sum) r_perc_ca_sum
	  from r_ca_tbl
	) select 1
		into ln$ca_sum_flag
		from dual
	   where pn_ca_sum > (select r_perc_ca_sum from r_ca_percentile_tbl); 	

	--!percentile de la suma de todos los digitos
	with r_comb_sum_tbl as (
	select comb_sum
	  from olap_sys.pm_mr_resultados_v2
	 where gambling_id > 594
	   and gl_cnt in (5,6)
	   and comb1 < 10  
	), r_comb_sum_percentile_tbl as (
	select percentile_cont(0.1) within group (order by comb_sum) r_perc_comb_sum
	  from r_comb_sum_tbl
	) select 1
		into ln$comb_sum_flag
		from dual
	   where pn_comb_sum > (select r_perc_comb_sum from r_comb_sum_percentile_tbl);	

	if upper(pv_val_type) = 'ALL' then
		if ln$ca_sum_flag = CN$VALOR_VALIDO and ln$comb_sum_flag = CN$VALOR_VALIDO then
			return 'Y';
		else
			return 'N';
		end if;	
	elsif upper(pv_val_type) = 'CA' then
		if ln$ca_sum_flag = CN$VALOR_VALIDO then
			return 'Y';
		else
			return 'N';
		end if;			
	elsif upper(pv_val_type) = 'CO' then
		if ln$comb_sum_flag = CN$VALOR_VALIDO then
			return 'Y';
		else
			return 'N';
		end if;			
	end if;
exception
	when others then
		return 'N';
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
end valida_ca_sum_comb_sum;


--!regresa si el numero es numero primo, inpar o par
function get_position_type(pn_comb      number) return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_position_type';
	lv$position_type	varchar2(2);
begin
	select case when olap_sys.w_common_pkg.is_prime_number(pn_comb) = 1 then 'PR' else 
           case when mod(pn_comb,2) = 0 then 'PA' else 
	       case when mod(pn_comb,2) > 0 then 'IN' end end end
	  into lv$position_type
      from dual;	  
	return lv$position_type;
exception
	when others then
		return 'ER';
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());
end get_position_type;

--!regresa el ID del drawing_case
function get_plan_jugada_id(pn_drawing_case		number)	return number is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_plan_jugada_id';
	ln$plan_jugada_id		number := 0;
begin
	select id
	  into ln$plan_jugada_id
	  from olap_sys.plan_jugadas
	 where drawing_type = 'mrtr'
	   and description  = 'DECENAS'
	   and status       = 'A'
	   and drawing_case = pn_drawing_case;
	return ln$plan_jugada_id;
exception
	when no_data_found then
		return -1;
	when others then
		return -1;
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
end get_plan_jugada_id;

--!regresa la bandera del mapa de numeros primos guardado en la tabla pm_mapa_numeros_primos 					  
function get_mapa_numeros_primos(pn_drawing_id	number
							   , pn_comb1  		number
							   , pn_comb2  		number
							   , pn_comb3  		number
							   , pn_comb4  		number
							   , pn_comb5  		number
							   , pn_comb6  		number)	return varchar2 is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_mapa_numeros_primos';							   
	ln$comb1		number := 0;
	ln$comb2		number := 0;
	ln$comb3		number := 0;
	ln$comb4		number := 0;
	ln$comb5		number := 0;
	ln$comb6		number := 0;
	lv$status		varchar2(1);
begin
	select status
	  into lv$status
	  from olap_sys.pm_mapa_numeros_primos
	 where last_id = pn_drawing_id 
	   and (comb1, comb2, comb3, comb4, comb5, comb6) in ( 
	select decode(is_prime_number(pn_comb1), 1, pn_comb1, 0) tcomb1
	     , decode(is_prime_number(pn_comb2), 1, pn_comb2, 0) tcomb2
		 , decode(is_prime_number(pn_comb3), 1, pn_comb3, 0) tcomb3
		 , decode(is_prime_number(pn_comb4), 1, pn_comb4, 0) tcomb4
		 , decode(is_prime_number(pn_comb5), 1, pn_comb5, 0) tcomb5
		 , decode(is_prime_number(pn_comb6), 1, pn_comb6, 0) tcomb6
	  from dual); 	 
	return lv$status;
exception
	when no_data_found then
		return null;
	when others then
		return 'X';
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
end get_mapa_numeros_primos;

--!cuenta las igualdades de valores entre dos cadenas de valores separadas por comas
function contar_igualdades (pv_string1		varchar2
						  , pv_string2		varchar2) return number is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'contar_igualdades';	
	ln$match_cnt						 number := 0;
begin
	with intersect_tbl as (
		select regexp_substr(pv_string1,'[^,]+',1,level) match
							   from dual 
							 connect by level <= length(pv_string1)-length(replace(pv_string1,',',''))+1
		intersect
		select regexp_substr(pv_string2,'[^,]+',1,level) match
							   from dual 
							 connect by level <= length(pv_string2)-length(replace(pv_string2,',',''))+1
	) select count(match) match_cnt
		into ln$match_cnt
		from intersect_tbl;	

	return ln$match_cnt;
exception
	when others then
		return -1;
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
end contar_igualdades;	

--!comparar loa valores de los 2 parametros de entrada y regresa un valor equivalente para cada caso
--!dependiendo de id del sorteo
function get_favorito(pn_drawing_id		number
				    , pn_pxc				number
				    , pv_preferido		varchar2) return number is
	LV$PROCEDURE_NAME       	constant varchar2(30) := 'get_favorito';	
	lv$pxc								 number(1) := -1;
	lv$preferido						 number(1) := -1;
begin
	--!convirtiendo pxc
	if pn_drawing_id < 594 then
		lv$pxc := -1;
	else
		if pn_pxc is null then
			lv$pxc := 0;
		else
			lv$pxc := 1;					
		end if;
	end if;
	
	--!convirtiendo preferido
	if pn_drawing_id < 898 then
		lv$preferido := -1;		
	else
		if pv_preferido is null then
			lv$preferido := 0;			
		else
			lv$preferido := 1;
		end if;
	end if;	

	--!retorna numero equivalente
	if lv$pxc = 0 and lv$preferido = 0 then
		return 0;
	elsif lv$pxc = 0 and lv$preferido = 1 then
		return 1;
	elsif lv$pxc = 1 and lv$preferido = 0 then
		return 2;
	elsif lv$pxc = 1 and lv$preferido = 1 then
		return 3;
	else
		return -1;
	end if;
exception
	when others then
		return -1;
		dbms_output.put_line(olap_sys.W_COMMON_PKG.GV_CONTEXT_ERROR||' ~ '||LV$PROCEDURE_NAME||': '||sqlerrm||' ~ '||dbms_utility.format_error_stack());		
end get_favorito;	
					
end w_common_pkg;
/
show errors;