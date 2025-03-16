MERGE INTO quantification.pob_txns_export_ssp_quant q
USING
(
select
 p.id,
 p.order_code,
 p.pob_svorderid,
 p.pob_svitemid,
 p.pob_productcode, 
 p.pob_dirissuenumber,
 p.pob_revenuestart,
 p.pob_revenueend,
 to_char(add_months(to_date(p.pob_revenuestart,'yyyymmdd'),x.billing_cycle)-1,'yyyymmdd') as pob_revenueend_1,
 p.pob_transactionamount,
 case when p.pob_transactionamount = 0.01 then 0.01 
      else (p.pob_transactionamount/
            months_between(to_date(p.pob_revenueend,'yyyymmdd')+1,to_date(p.pob_revenuestart,'yyyymmdd')))*
           x.billing_cycle 
      end as pob_transactionamount_1,
 months_between(to_date(p.pob_revenueend,'yyyymmdd')+1,to_date(p.pob_revenuestart,'yyyymmdd')) as life,
 x.billing_cycle
from 
 quantification.pob_txns_export_ssp_quant p
 inner join 
    ( 
     select
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
        finrepo.kgen_product_info@orafin.db.yellowpages.com k
       group by 
        k.product_code,
        k.product_issue_num
      ) x
      inner join finrepo.kgen_product_info@orafin.db.yellowpages.com k
       on k.product_code = x.product_code and
          k.product_issue_num = x.product_issue_num and
          k.version = x.version
    ) x
   on 
    x.product_code = p.pob_productcode and
    x.product_issue_num = p.pob_dirissuenumber
where
 p.pob_alloc_map = 'O' and
 p.status = 'C' and
 p.new_record_n is null and
 p.pob_source = 'SFDC' and
 nvl(p.txn_status_code,'NO') = 'NO' and
 p.pob_transactionamount > 0 and
 p.created_date <= add_months(last_day(to_date('2021-12-01','yyyy-mm-dd')),1) and
 p.pob_producttype = 'Y' and 
 substr(p.pob_revenuestart,0,4) = 2021 and
 months_between(to_date(p.pob_revenueend,'yyyymmdd')+1,to_date(p.pob_revenuestart,'yyyymmdd')) = 12 and
 x.billing_cycle >= 13
 ) all_pobs
ON (q.id = all_pobs.id)
WHEN MATCHED THEN 
 UPDATE 
   set q.pob_revenueend_1 = all_pobs.pob_revenueend_1,
       q.pob_transactionamount_1 = all_pobs.pob_transactionamount_1,
       q.quantify_flag_1 = 'Y';
commit;