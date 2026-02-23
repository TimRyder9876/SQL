with
all_pobs as
(
    select /*+ parallel(pq,16) */
     pq.id,
     pq.pob_productcode, 
     pq.pob_dirissuenumber,
     pq.pob_svitemid,
     pq.pob_svorderid,
     pq.pob_transactiondate,
     pq.pob_transactionamount,
     pq.pob_revenuestart,
     pq.pob_revenueend,
     pq.txn_status_code,
     pq.pob_producttype,
     x.billing_cycle
    from 
     finrepo.pob_txns_export_ssp pq 
     inner join 
         (select
           k.product_code,
           k.product_issue_num,
           k.version,
           add_months(last_day(nvl(k.gl_Revenue_Date,k.issue_date))+1,-1) as kgen_rev_start_date, -- make it the first of the month
           k.billing_cycle
          from 
           (select
             k.product_code,
             k.product_issue_num,
             max(k.version) as version
            from 
             finrepo.kgen_product_info k
            group by 
             k.product_code,
             k.product_issue_num
           ) x
          inner join finrepo.kgen_product_info k
           on k.product_code = x.product_code and
              k.product_issue_num = x.product_issue_num and
              k.version = x.version
          ) x   
      on  pq.pob_productcode = x.product_code and 
          pq.pob_dirissuenumber = x.product_issue_num
    where
     pq.pob_alloc_map = 'O' and
     pq.is_coupon is null and
     pq.pob_source in ('SFDC') and
     pq.pob_producttype = 'Y' and
     pq.created_date <= add_months(last_day(to_date('2022-10-01','yyyy-mm-dd')),1) and
     pq.status = 'C' and
     nvl(pq.txn_status_code,'NO') = 'NO' and
     pq.pob_transactionamount > 0.01 and
     substr(pob_revenuestart,0,6) between 202207 and 202209 --and
     --months_between(to_date(pq.pob_revenueend,'yyyymmdd')+1, to_date(pq.pob_revenuestart,'yyyymmdd')) > 12
     --x.billing_cycle > 12
)


select
 ap.id,
 ap.pob_transactiondate,
 ap.pob_svorderid,
 ap.pob_svitemid,
 ap.pob_revenuestart,
 ap.pob_revenueend,
 ap.pob_producttype,
 ap.pob_transactionamount,
 ap.txn_status_code, 
 months_between(to_date(ap.pob_revenueend,'yyyymmdd')+1, to_date(ap.pob_revenuestart,'yyyymmdd')) as pob_life,
 ap.billing_cycle as kgen_life,
 s1.transaction_amount,
 ap.billing_cycle * s1.transaction_amount as sfdc_contract_amt,
 months_between(to_date(ap.pob_revenueend,'yyyymmdd')+1, to_date(ap.pob_revenuestart,'yyyymmdd')) * s1.transaction_amount as amt_with_l4_life_and_sfdc_amt
from 
    (
     select -- retrieve the latest version of an item
      ap.pob_svorderid,
      ap.pob_svitemid,
      max(s.id) as id
     from
      all_pobs ap  
      inner join finrepo.sales_txns_sfdc s
       on ap.pob_svorderid = s.order_id and
          ap.pob_svitemid = s.sv_item_id_eiid and
          s.status = 'P' and 
          s.is_coupon is null and
          s.transaction_type != 'C' and
          s.transaction_amount != 0 and
          s.status_name__c != 'Completed - Cancelled' and
          s.created_date <= add_months(last_day(to_date('2022-10-01','yyyy-mm-dd')),1)
     group by 
      ap.pob_svorderid,
      ap.pob_svitemid
     ) x1
   inner join finrepo.sales_txns_sfdc s1
    on s1.id = x1.id
   inner join all_pobs ap
    on ap.pob_svorderid = s1.order_id and
       ap.pob_svitemid = s1.sv_item_id_eiid
where
 months_between(to_date(ap.pob_revenueend,'yyyymmdd')+1, to_date(ap.pob_revenuestart,'yyyymmdd'))* s1.transaction_amount <> ap.pob_transactionamount
 --ap.billing_cycle * s1.transaction_amount <> ap.pob_transactionamount
