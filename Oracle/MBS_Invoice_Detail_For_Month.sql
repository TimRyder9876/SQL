select /*+ parallel(m,8) */
 to_char(last_day(c.ctcr_sys_cre_dt),'YYYY-MM') as mbs_load_date
 ,substr(m.mbs_invoice_id,0,3) as invoice_type
 ,m.mbs_invoice_id
 ,m.account_id
 ,m.mbs_kgen_record_id
 ,m.last_version_ind
 ,m.record_status
 ,m.mbs_record_id
 ,m.related_entity_id
 ,m.record_type
 ,m.udac_code
 ,m.line_amt
 ,m.product_code
 ,m.product_issue_num
 ,fa.account_sub_type
 ,m.adj_reason_code
 ,m.sv_item_id
 ,m.sv_order_id
 ,m.cr_dr_type
 ,m.product_type
 ,m.start_date
 ,m.end_date
 ,m.mbs_invoice_date
 from 
  finance.mbs_invoice_control c,
  finance.mbs_invoice_summary i,
  finance.mbs_invoice_detail m,
  fn_account fa
 where  
  c.file_seq_num = i.file_seq_num and
  c.file_status = 'LC' AND
  to_char(last_day(c.ctcr_sys_cre_dt),'yyyymmdd') = to_char(last_day(to_date('2024-11-01','yyyy-mm-dd')),'yyyymmdd') and
  c.version_num = (select max(c1.version_num) from finance.mbs_invoice_control c1 
                  where c1.file_seq_num = c.file_seq_num and c1.file_status = 'LC') and
  m.file_seq_num = i.file_seq_num and
  m.mbs_invoice_id = i.mbs_invoice_id and
  m.account_id = i.account_id and
  m.version_num = i.version_num and
  m.last_version_ind = 'Y' and
  m.ACCOUNT_ID = fa.ACCOUNT_ID	
