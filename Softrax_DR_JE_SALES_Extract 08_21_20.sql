select 
 Concat(Year(ri.sxCreateDate),Right(Concat(0,Month(ri.sxCreateDate)),2),Right(Concat(0,Day(je.sxCreateDate)),2)) as POBCreateDate
 ,Concat(Year(je.sxJEDate),Right(Concat(0,Month(je.sxJEDate)),2),Right(Concat(0,Day(je.sxJEDate)),2)) as JEDate
 ,Concat(Year(ri.sxTrxDate),Right(Concat(0,Month(ri.sxTrxDate)),2),Right(Concat(0,Day(ri.sxTrxDate)),2)) as TrxDate
 ,ri.ExtractDate
 ,ri.sxCode as KgenJournalId
 ,substring(jd.sxAcctStr,7,5) as GLAccount
 ,ab.sxCode AS Book
 ,ri.sxSource
 ,jd.sxJournalEntryId 
 ,left(jd.sxAcctStr,5) as BusinessUnit
 ,case when ae.sxForecastDetId is null then 'AR-UNEARNED'
       else 'UNEARNED-REVENUE'
	   end as JE_Type
 ,at.sxCode as LogicalAcnt
 ,p.sxCode as ActivityType
 ,isnull(ri.MBSDeptCode,'') as MBSDeptCode
 ,isnull(ri.MBSInvoiceID,'') as MBSInvoiceID	
 ,CG.sxExtCustomerId
 ,ri.BillingAccount
 ,isnull(ri.SVItemID,'') as SVItemID
 ,isnull(ri.SVOrderId,'') as SVOrderId 
 ,ri.ChargeType
 ,isnull(ri.UDAC,'') as UDAC
 ,case when ae.sxType = 'D' then 1 else -1 end * ae.sxTransactionalAmt as Je_Amount
 ,isnull(ri.PrintCode,'') as PrintCode 
 ,isnull(ri.DirectoryIssueNumber,0) as DirectoryIssueNumber
 ,isnull(ri.ReasonCode,'') as ReasonCode
 ,Concat(Year(rf.sxStartDate),Right(Concat(0,Month(rf.sxStartDate)),2),Right(Concat(0,Day(rf.sxStartDate)),2)) as StartDate
 ,Concat(Year(rf.sxEndDate),Right(Concat(0,Month(rf.sxEndDate)),2),Right(Concat(0,Day(rf.sxEndDate)),2)) as EndDate
 ,cast(ri.sxTermInDays as INT) as sxTermInDays
 ,isnull(ri.ProductType,'') as ProductType 
 ,ri.AccountType
 ,ri.AccountSubType
 ,ri.sxStatus

from dbo.sxJournalEntry je
     inner join sxaccountingbooks as ab
	  on ab.sxId = je.sxBookId and
         ab.sxCode = 'STD'
     inner join sxcalendarperiods as cp
      on cp.sxid = je.sxperiodid and
		 convert(varchar,dateadd(d,-(day(getdate())),getdate()),106) between sxStartDate and sxEndDate
	 inner join sxjournaldetail as jd  
      on jd.sxjournalentryid = je.sxid and
         jd.sxrowdeleted = 'N'  
     inner join sxaccounttypes as at 
      on at.sxId = jd.sxAcctTypeId
     inner join sxaccountingentry as ae 
      on ae.sxjournaldetailid = jd.sxid and 
         ae.sxrowdeleted = 'N'
     inner join sxrevenueitems ri 
      on ae.sxrevitemid = ri.sxid and
         ri.sxrowdeleted = 'N' and
         ri.producttype in ('V','R','H','S') and
         ri.sxsource = 'KGEN' 
     inner join dbo.sxProducts P
	  on p.sxId = ri.sxProductId 
     inner join dbo.sxRevenueForecasts rf
	  on rf.sxRevenueItemId = ri.sxId
     left join dbo.sxRevItemToGroups rigC
	  on rigC.sxRevItemId = ri.sxId and
	     rigC.sxType = 'C' and
	     rigC.sxRowDeleted = 'N'
     left join dbo.sxCustomerGroups CG
	  on CG.sxID = rigC.sxGroupId and
	     CG.sxRowDeleted = 'N'
	 where je.sxRowDeleted = 'N'
	   and at.sxCode = 'DR'
	   and ae.sxForecastDetId is not null
	   --pull by AR, DR, and SALES
	   --when pulling DR also include the ae.sxForecastDetId is(not) null piece
          -- for breakout between AR entry and Revenue entry



		 	    