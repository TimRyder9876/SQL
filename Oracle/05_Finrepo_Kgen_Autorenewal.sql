DEFINE VDATE = '2024-12-01';

MERGE INTO quantification.pob_txns_export_ssp_quant q
USING
(
with
print_pobs as
(
 select /*+ parallel(pq,16) */
  pq.id,
  pq.order_code,
  pq.pob_code,
  pq.cust_code,
  pq.pob_billingaccount,
  pq.pob_productcode,
  pq.pob_dirissuenumber,
  pq.pob_svorderid,
  pq.pob_svitemid,
  pq.pob_udac,
  pq.pob_producttype,
  pq.signature_date,
  pq.pob_alloc_map,
  pq.pob_source,
  pq.pob_sspoverride,
  pq.pob_transactiondate, 
  pq.grouping_date,
  pq.txn_status_code, 
  pq.serial_id,
  pq.pob_transactionamount,
  pq.pob_revenuestart,
  pq.pob_revenueend,
  case when pq.serial_id is null then 'OrderItem'
       when pq.serial_id is not null and pq.txn_status_code is null then 'Serial'
       when pq.serial_id is not null and pq.txn_status_code is not null then 'SerialNew'
       else pq.txn_status_code
     end as linktype,
  x1.total_pob,
  x1.total_pos_pob,
  x1.total_penny_pob,
  x1.total_neg_pob
 from
  (
    select /*+ parallel(pq,16) */
     pq.id,
     pq.txn_status_code,
     pq.pob_transactionamount,
     count(pq.pob_transactionamount
         ) over
        (partition by pq.pob_productcode,
                      pq.pob_dirissuenumber,
                      pq.pob_udac,
                      pq.pob_svorderid,
                      pq.pob_svitemid) as total_pob_count,  
     sum(pq.pob_transactionamount
         ) over
        (partition by pq.pob_productcode,
                      pq.pob_dirissuenumber,
                      pq.pob_udac,
                      pq.pob_svorderid,
                      pq.pob_svitemid) as total_pob,  
     sum(case when pq.pob_transactionamount > 0.01 then pq.pob_transactionamount
              else 0
         end     
         ) over
        (partition by pq.pob_productcode,
                      pq.pob_dirissuenumber,
                      pq.pob_udac,
                      pq.pob_svorderid,
                      pq.pob_svitemid) as total_pos_pob,
     sum(case when pq.pob_transactionamount = 0.01 then pq.pob_transactionamount
              else 0
         end     
         ) over
        (partition by pq.pob_productcode,
                      pq.pob_dirissuenumber,
                      pq.pob_udac,
                      pq.pob_svorderid,
                      pq.pob_svitemid) as total_penny_pob,                              
     sum(case when pq.pob_transactionamount < 0 then pq.pob_transactionamount
              else 0
         end     
         ) over
        (partition by pq.pob_productcode,
                      pq.pob_dirissuenumber,
                      pq.pob_udac,
                      pq.pob_svorderid,
                      pq.pob_svitemid) as total_neg_pob 
    from
     quantification.pob_txns_export_ssp_quant pq
      inner join (select distinct /*+ parallel(pq,16) */
                   pq.order_code
                  from 
                   quantification.pob_txns_export_ssp_quant pq
                  where
                   pq.pob_alloc_map = 'O' and
                   pq.status = 'C' and
                   pq.created_date <= add_months(last_day(to_date('&VDATE','yyyy-mm-dd')),1) and
                   ((pq.pob_producttype = 'Y' and substr(pq.pob_revenuestart,0,4) = 2021) or
                    (pq.pob_producttype != 'Y' and substr(pq.pob_revenuestart,0,4) <= 2021 and substr(pq.pob_revenueend,0,4) >= 2021)
                   )
                  ) x 
       on pq.order_code = x.order_code
    where
     pq.pob_alloc_map = 'O' and
     pq.exclude_n is null and
     pq.pob_producttype = 'Y' and
     pq.pob_source in ('KGEN') and
     nvl(pq.is_coupon,'N') = 'N' and
     pq.created_date <= add_months(last_day(to_date('&VDATE','yyyy-mm-dd')),1) and
     pq.status = 'C' 
 ) x1
 inner join quantification.pob_txns_export_ssp_quant pq
  on pq.id = x1.id
where
 x1.total_pob <> 0  
),     

l4_print_autorenew_analysis as
(
select
 pp.id as pob_txn_export_id,
 pp.pob_code,
 pp.pob_productcode,
 pp.pob_dirissuenumber,
 pp.pob_svorderid,
 pp.pob_svitemid,
 pp.pob_udac,
 pp.pob_transactionamount,
 pp.linktype,
 pp.total_pob,
 pp.total_pos_pob,
 pp.total_penny_pob,
 pp.total_neg_pob,
 k.id as kgen_id,
 k.transaction_amount,
 k.contract_term,
 k.status,
 'Autorenewal Item' as Type
from 
  (
   select -- retrieve the latest version of an item
    k.sv_item_id,
    k.product_code,
    k.product_issue_num,
    k.udac,
    max(k.id) as id
   from
    print_pobs pp  
    inner join finrepo.sales_txns_kgen@pfinyp.db.yellowpages.com k
     on pp.pob_productcode = k.product_code and
        pp.pob_dirissuenumber = k.product_issue_num and
        pp.pob_svitemid = k.sv_item_id and
        pp.pob_udac = k.udac and
        k.status = 'P' and
        (k.ctcr_batch_prog_id = 'BCUPDT' or k.autorenew_ind = 'Y' or k.autorenew = 'A') and
        k.created_date <= last_day(to_date('&VDATE','yyyy-mm-dd'))
   group by 
    k.sv_item_id,
    k.product_code,
    k.product_issue_num,
    k.udac    
  ) x1
  inner join finrepo.sales_txns_kgen@pfinyp.db.yellowpages.com k
   on k.id = x1.id
  inner join print_pobs pp
   on pp.pob_productcode = k.product_code and
      pp.pob_dirissuenumber = k.product_issue_num and
      pp.pob_svitemid = k.sv_item_id and
      pp.pob_udac = k.udac and
      pp.pob_source = 'KGEN' and
      pp.linktype in('OrderItem')
union all
select
 pp.id as pob_txn_export_id,
 pp.pob_code,
 pp.pob_productcode,
 pp.pob_dirissuenumber,
 pp.pob_svorderid,
 pp.pob_svitemid,
 pp.pob_udac,
 pp.pob_transactionamount,
 pp.linktype,
 pp.total_pob,
 pp.total_pos_pob,
 pp.total_penny_pob,
 pp.total_neg_pob,
 k.id as kgen_id,
 k.transaction_amount,
 k.contract_term,
 k.status,
 'Autorenewal Item' as Type
from 
 print_pobs pp
 inner join finrepo.sales_txns_kgen@pfinyp.db.yellowpages.com k
  on pp.pob_source = 'KGEN' and
     pp.linktype in('Serial','SerialNew') and
     pp.serial_id = k.serial_id and
     (k.ctcr_batch_prog_id = 'BCUPDT' or k.autorenew_ind = 'Y' or k.autorenew = 'A')
)

 select pob_txn_export_id
 from l4_print_autorenew_analysis
 ) l4
ON (q.id = l4.pob_txn_export_id)
WHEN MATCHED THEN 
 UPDATE 
   set q.exclude_n = 'Y', 
       q.kgen = 'Y', 
       q.kgen_reason = case when q.kgen_reason is null then 'Autorenewal'
                       else q.kgen_reason || '; Autorenewal'
                     end;
commit;  
