select
 x2.pob_udac || x2.directory_tier__c as key,
 x2.pob_udac,
 x2.directory_tier__c,
 case when x2.min_rate = x2.max_rate then to_char(x2.max_rate)
      else x2.min_rate||' to '||x2.max_rate
   end as rate  
from
 (
        select 
         x.pob_udac,
         sps.directory_tier__c,
         min(spdt.list_price__c*12) as min_rate,
         max(spdt.list_price__c*12) as max_rate
        from
          (
            select distinct
             sci.pob_productcode,
             sci.pob_dirissuenumber,
             sci.pob_udac,
             sci.directory_tier
            from 
             ssp_cust_items sci
            where
             sci.collection_code = '20241001-20240331' and
             ssp_use_flg = 0 and
             sci.pob_producttype not in('O','V')
          ) x
         inner join sfdc_price_by_dir_tier__c@PFINYP.DB.YELLOWPAGES.COM spdt
           on x.pob_udac = spdt.udac__c and
              x.pob_productcode = spdt.directory_number__c and
              x.pob_dirissuenumber = spdt.dir_issue__c and
              spdt.isdeleted = 0
         inner join SFDC_PRODUCT_SCHEDULE__C@PFINYP.DB.YELLOWPAGES.COM sps
          on sps.directory_code__c = spdt.directory_number__c and
             sps.directory_issue_num__c = spdt.dir_issue__c and
             sps.isdeleted = 0
        group by 
         x.pob_udac,
         sps.directory_tier__c

   ) x2  
union all

select
 spdt.udac__c||spdt.directory_tier__c as key,
 spdt.udac__c,
 spdt.directory_tier__c,
 to_char(spdt.list_price__c) as rate
from
 sfdc_price_by_dir_tier__c@PFINYP.DB.YELLOWPAGES.COM spdt
where
 spdt.directory_number__C is null and
 spdt.isdeleted = 0
order by
 1,2
