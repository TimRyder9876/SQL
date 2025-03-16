
		select 
			sum(rfd.sxTrxAmount) as Amount,
			-- 12-08-2021 --change accounting period to look to see if record posted in a different period. If so, then show the posted date.
			concat(year(RFD.SXRECOGNITIONDATE),right(concat(0,month(RFD.SXRECOGNITIONDATE)),2)) as Accounting_Period,
			ab.sxCode as Accounting_Book,
			ri.Alloc_Map as Pob_Alloc_Mapping,
			ri.producttype,
			ri.udac,
			case when rfd.sxStatus = 'JE' then 'Posted'
			 else ''
			 end as Schedule_Status,
			isnull(rfd.sxJEEntryCode,'') as Journal_Batch_Id,
			isnull(ri.sxSource,'') as Source_System,
			isnull(rfd.sxJEEntryId,'') as Posted_Batch_Id,
			isnull(rfd.sxJePeriodCode,'') as Posted_Period_Code
		from 
			sxRevenueItems RI
			inner join sxAccountingBooks AB
			on ab.sxID = ri.sxBookId and
				ab.sxRowDeleted = 'N' and
				ab.sxCode = 'ASC606_ME'
			inner join sxRevenueForecasts RF
			on rf.sxRevenueItemId = ri.sxId and
				rf.sxStatus = 'A' and
				rf.sxRowDeleted = 'N'
			inner join sxRevForecastDetails RFD
			on rfd.sxRevForecastId = rf.sxId and
			   rfd.sxStatus = 'P' and
			   rfd.sxRowDeleted = 'N'
		where
			ri.sxRowDeleted = 'N' and
			ri.sxStatus = 'A'
		group by
			concat(year(RFD.SXRECOGNITIONDATE),right(concat(0,month(RFD.SXRECOGNITIONDATE)),2)),
			ab.sxCode,
			ri.Alloc_Map,
			ri.producttype,
			ri.udac,
			case when rfd.sxStatus = 'JE' then 'Posted'
			 else ''
			 end,
			ri.sxSource,
			rfd.sxJEEntryCode,
			rfd.sxJEEntryId,
			rfd.sxJePeriodCode

  

