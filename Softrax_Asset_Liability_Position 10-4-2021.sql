DECLARE @MONTH INTEGER = 7;
DECLARE @YEAR INTEGER = 2021;

with 
fiscal_period as
 (select 
   cp.sxId,
   cp.sxMonth,
   cp.sxCode,
   cp.sxFiscalYear    
  from 
   sxCalendarPeriods CP
  where 
   cp.sxRowDeleted = 'N' and
   cp.sxMonth+(sxFiscalYear*12) <= (@YEAR*12)+@MONTH -- input parameter
 )

select
 x.thru_date,
 x.order_group,
 case when x.accountingentrytrxamt <0 then 'Liability'
      else 'Asset'
   end as AssLia_flg,
 x.accountingentrytrxamt
from
 (
    select
		    max(fp.sxCode) as thru_date,
			substring(og.sxExtOrderId,0,len(og.sxExtOrderId)-3) as order_group,
			sum(case when ae.sxtype = 'D' then 1
			         when ae.sxtype = 'C' then -1 
				end * ae.sxtransactionalamt) as accountingentrytrxamt
        from    
            dbo.sxOrderGroups as og
			inner join sxRevItemToGroups rig
		    on rig.sxGroupId = og.sxId and
			   rig.sxRowDeleted = 'N' and
			   rig.sxType = 'O'
            inner join sxRevenueItems ri
			on ri.sxid = rig.sxRevItemId and
	           ri.sxRowDeleted = 'N'
            inner join sxAccountingEntry ae
			on ae.sxrevitemid = ri.sxid and
		       ae.sxRowDeleted = 'N' and
			   ae.sxBookId = 5 and
		       ae.sxacctstr in ('DS810-23410-9000-999999-00000-000000000000000',
				                 'DS810-23435-9000-000000-00000-000000000000000')
            inner join sxJournalEntry je
            on je.sxid = ae.sxjournalentryid and
               je.sxRowDeleted = 'N' 
            inner join fiscal_period fp
			on fp.sxId = sxPeriodId 
    where
		    og.sxrowdeleted = 'N' and
			og.sxstatus = 'A'
    group by
            substring(og.sxExtOrderId,0,len(og.sxExtOrderId)-3)
    having
            sum(case when ae.sxtype = 'D' then 1
			         when ae.sxtype = 'C' then -1 
			      end * ae.sxtransactionalamt) <> 0
 ) x
order by 
 2
