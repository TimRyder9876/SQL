select 
 je.sxCode
 ,jd.sxJournalEntryId 
 ,Concat(Year(je.sxJEDate),Right(Concat(0,Month(je.sxJEDate)),2),Right(Concat(0,Day(je.sxJEDate)),2)) as JEDate
 ,left(jd.sxAcctStr,5) as BusinessUnit
 ,substring(jd.sxAcctStr,7,5) as GLAccount
 ,ab.sxCode AS Book
 ,isnull(ri.UDAC,'') as UDAC
 ,ri.sxStatus
 ,sum(case when ae.sxType = 'D' then 1 else -1 end * ae.sxFunctionalAmt) as acctFuncAmt

from dbo.sxJournalEntry je
     inner join sxaccountingbooks as ab
	  on ab.sxId = je.sxBookId
         and ab.sxCode = 'STD'
     inner join sxcalendarperiods as cp
      on cp.sxid = je.sxperiodid 
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
         ri.sxrowdeleted = 'N'
     inner join dbo.sxProducts P
	  on p.sxId = ri.sxProductId 
	 where je.sxRowDeleted = 'N'
	   and at.sxCode = 'SALES'
	   and Year(je.sxJEDate) = 2022
	   and Month(je.sxJEDate) = 2

group by
 je.sxCode
 ,Concat(Year(je.sxJEDate),Right(Concat(0,Month(je.sxJEDate)),2),Right(Concat(0,Day(je.sxJEDate)),2))
 ,substring(jd.sxAcctStr,7,5)
 ,ab.sxCode
 ,ri.sxSource
 ,jd.sxJournalEntryId 
 ,left(jd.sxAcctStr,5)
 ,at.sxCode
 ,p.sxCode
 ,isnull(ri.UDAC,'')
 ,ri.sxStatus
order by
 1,2,3,4,5



		 	    