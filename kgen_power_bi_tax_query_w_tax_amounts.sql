select
 crm.ctcr_batch_prog_dt,
 crm.posting_date,
 crm.acnt_type,
 crm.cust_city,
 crm.cust_state,
 crm.cust_zip,
 crm.print_state,
 m.udac_code,
 crm.product_code,
 crm.charging_type,
 crm.reason_code,
 crm.tax_charged,
 sum(cr_dr_amt) as charge_amount,
 -sum(crm.je_amt) as je_amount,
 sum(crm.tax_cr_dr_amt) as charge_tax_amount,
 -sum(crm.tax_je_amt) as je_tax_amount
from
 (
      select 
       x.ctcr_batch_prog_dt, x.posting_date, x.acnt_type, x.charging_type,
       x.reason_code, x.account_id,
       x.mbs_kgen_record_id, x.product_code, x.product_issue_num, 
       nvl(c.community_name,'UNK') as cust_city, nvl(c.state_abbr,'UNK') as cust_state,
       nvl(a.zip_five,'UNK') as cust_zip, p.state_abbreviation as print_state,
       x.tax_charged, x.cr_dr_amt, x.je_amt, x.tax_cr_dr_amt, x.tax_je_amt
      from
       (
        select
         x.ctcr_batch_prog_dt, x.posting_date, x.acnt_type, x.charging_type,
         x.reason_code, x.account_id, x.tax_sync,
         x.mbs_kgen_record_id, x.product_code, x.product_issue_num, 
         x.cr_dr_type, x.cr_dr_amt, x.je_amt,
         case when y.account_id is null then 'N' else 'Y' end as tax_charged,
         sum(y.cr_dr_amt) as tax_cr_dr_amt, sum(y.je_amt) as tax_je_amt
        from
         (
          select 
           j.ctcr_batch_prog_dt, j.posting_date, j.acnt_type, j.charging_type,
           case when j.charging_type = 'ACC' and j.reason_code in('SCRV','SCSA') then 'SCADJ'
                else j.reason_code end as reason_code, cr.account_id, 
           case when j.charging_type = 'ACC' and j.reason_code in('SCRV','SCSA') then cr.account_id||'SCTX'
                else to_char(nvl(substr(cr.mbs_kgen_record_id,2,14), cr.cr_dr_element_id)) end as tax_sync,
           cr.mbs_kgen_record_id, cr.product_code, cr.product_issue_num, 
           cr.cr_dr_type, sum(cr.cr_dr_amt) as cr_dr_amt, sum(j.je_amt) as je_amt
          from
           (select /*+ parallel(j,8) */ 
             j.ctcr_batch_prog_dt, j.posting_date, j.account_id, j.trans_id,
             j.acnt_type, j.charging_type, j.reason_code,
             decode(j.cr_dr_ind,'DR',1,-1) * j.activity_amt as je_amt
            from 
             finance.journal j
            where 
             j.ctcr_batch_prog_dt >= (select case when extract(month from sysdate-1) > 7 then 
                                                  to_date('07/01/' || to_number(to_char(sysdate,'YYYY')-1),'mm/dd/yyyy')
                                                  else to_date('07/01/' || to_number(to_char(sysdate,'YYYY')-2),'mm/dd/yyyy')
                                             end from dual) and
             j.activity_type in ('CHGR','CHGW') and
             j.logical_acnt = 'UNBILLED' 
             ) j
           inner join finance.transaction_log t on
            t.trans_id = j.trans_id
           inner join finance.cr_dr_element cr on
            cr.account_id = t.account_id and
            cr.cr_dr_element_id = t.ar_entity_id 
         group by j.ctcr_batch_prog_dt, j.posting_date, j.acnt_type, j.charging_type,
           case when j.charging_type = 'ACC' and j.reason_code in('SCRV','SCSA') then 'SCADJ'
                else j.reason_code end, cr.account_id, 
           case when j.charging_type = 'ACC' and j.reason_code in('SCRV','SCSA') then cr.account_id||'SCTX'
                else to_char(nvl(substr(cr.mbs_kgen_record_id,2,14), cr.cr_dr_element_id)) end,
           cr.mbs_kgen_record_id, cr.product_code, cr.product_issue_num, 
           cr.cr_dr_type
          ) x
         left join (
                    select 
                     j.ctcr_batch_prog_dt, j.posting_date, t.account_id, 
                     j.charging_type,cr.mbs_kgen_record_id, cr.product_code, cr.product_issue_num,                    
                     case when j.reason_code = 'SCTX' then t.account_id || 'SCTX' 
                          else to_char(nvl(substr(cr.mbs_kgen_record_id,2,14), cr.taxable_element_id)) end as tax_sync,
                     sum(cr.cr_dr_amt) as cr_dr_amt, sum(j.je_amt) as je_amt
                    from
                         (select /*+ parallel(j,8) */ 
                           j.ctcr_batch_prog_dt, j.posting_date, j.account_id, j.trans_id,
                           j.acnt_type, j.charging_type, j.reason_code,
                           decode(j.cr_dr_ind,'DR',1,-1) * j.activity_amt as je_amt
                          from 
                           finance.journal j
                          where
                           j.ctcr_batch_prog_dt >= (select case when extract(month from sysdate-1) > 7 then 
                                                                     to_date('07/01/' || to_number(to_char(sysdate,'YYYY')-1),'mm/dd/yyyy')
                                                                else to_date('07/01/' || to_number(to_char(sysdate,'YYYY')-2),'mm/dd/yyyy')
                                                    end from dual) and
                           j.logical_acnt = 'TAX-LIAB'
                       ) j
                     inner join finance.transaction_log t on
                      t.trans_id = j.trans_id   
                     inner join finance.cr_dr_element cr on
                      cr.account_id = t.account_id and
                      cr.cr_dr_element_id = t.ar_entity_id 
                    group by
                     j.ctcr_batch_prog_dt, j.posting_date, t.account_id, 
                     j.charging_type,cr.mbs_kgen_record_id, cr.product_code, cr.product_issue_num,
                     case when j.reason_code = 'SCTX' then t.account_id || 'SCTX' 
                          else to_char(nvl(substr(cr.mbs_kgen_record_id,2,14), cr.taxable_element_id)) end
                    ) y
           on x.account_id = y.account_id and
              x.tax_sync = y.tax_sync and
              x.ctcr_batch_prog_dt = y.ctcr_batch_prog_dt
         group by x.ctcr_batch_prog_dt, x.posting_date, x.acnt_type, x.charging_type,
                  x.reason_code, x.account_id, x.tax_sync,
                  x.mbs_kgen_record_id, x.product_code, x.product_issue_num, 
                  x.cr_dr_type, x.cr_dr_amt, x.je_amt,
                  case when y.account_id is null then 'N' else 'Y' end   
       ) x
       inner join finance.billing_account b on
        b.billing_account_id = x.account_id
       left join core.address a on
        a.address_id = b.billing_addr_id
       left join refread.community c on
        c.community_id = a.community_id
       left join refread.product_issue p on
        x.product_code = p.product_code and
        x.product_issue_num = p.product_issue_num) crm
 left join finance.mbs_invoice_detail m ON
  crm.account_id = m.account_id and 
  crm.mbs_kgen_record_id = m.mbs_kgen_record_id 
  and nvl(m.last_version_ind,'Y') = 'Y'    
group by
 crm.ctcr_batch_prog_dt,
 crm.posting_date,
 crm.acnt_type,
 crm.cust_city,
 crm.cust_state,
 crm.cust_zip,
 crm.print_state,
 m.udac_code,
 crm.product_code,
 crm.charging_type,
 crm.reason_code,
 crm.tax_charged  
