with gl_matrix as
(select
         gr.logical_account,
         gr.acnt_type,
         x.account_type_prty,
         gr.acnt_sub_type,
         x.acnt_sub_type_prty,
         gr.charging_type,
         x.charging_type_prty,
         gr.payment_method,
         x.pmt_method_prty,
         gr.reason_code,
         x.reason_code_prty,
         gr.deposit_bank_code,
         x.deposit_bank_prty,
         gr.business_entity,
         x.business_ent_prty,
         gr.section_category,
         x.sec_category_prty,
         gr.product_type,
         x.product_type_prty,
         gr.channel_code,
         x.channel_code_prty,
         gr.gl_physical_acnt
        from 
         refread.gl_reporting gr,
         (select
           g.gl_logical_acnt,
           case when nvl(g.pmt_method_prty,0) = 0 then 0 else power(2,10-g.pmt_method_prty) end as pmt_method_prty,
           case when nvl(g.account_type_prty,0) = 0 then 0 else power(2,10-g.account_type_prty) end as account_type_prty,
           case when nvl(g.acnt_sub_type_prty,0) = 0 then 0 else power(2,10-g.acnt_sub_type_prty) end as acnt_sub_type_prty,
           case when nvl(g.charging_type_prty,0) = 0 then 0 else power(2,10-g.charging_type_prty) end as charging_type_prty,
           case when nvl(g.product_type_prty,0) = 0 then 0 else power(2,10-g.product_type_prty) end as product_type_prty,
           case when nvl(g.reason_code_prty,0) = 0 then 0 else power(2,10-g.reason_code_prty) end as reason_code_prty,
           case when nvl(g.deposit_bank_prty,0) = 0 then 0 else power(2,10-g.deposit_bank_prty) end as deposit_bank_prty,
           case when nvl(g.business_ent_prty,0) = 0 then 0 else power(2,10-g.business_ent_prty) end as business_ent_prty,
           case when nvl(g.sec_category_prty,0) = 0 then 0 else power(2,10-g.sec_category_prty) end as sec_category_prty,
           case when nvl(g.channel_code_prty,0) = 0 then 0 else power(2,10-g.channel_code_prty) end as channel_code_prty
          from 
            refread.gl_lgcl_acnt g
          where
            g.effective_to_date > sysdate) x
        where
         gr.effective_to_date > sysdate and
         gr.logical_account = x.gl_logical_acnt
         and gr.gl_physical_acnt = '12125'
        order by
         1,2,3,4,5,6,7,8,9,10,11)

  select
   m.je_date,
   m.trans_id,
   m.journal_id,
   m.logical_acnt,
   m.acnt_type,
   m.acnt_sub_type,
   m.charging_type,
   m.payment_method,
   m.reason_code,
   m.deposit_bank,
   m.business_entity,
   m.section_category,
   m.product_type,
   m.channel_code,
   m.activity_amt,
   m.match_value,
   z.gl_physical_acnt
  from
     (select /*+ parallel(j,8) */
       trunc(j.ctcr_batch_prog_dt) as je_date,
       j.trans_id,
       j.journal_id,
       j.logical_acnt,
       j.acnt_type,
       j.acnt_sub_type,
       j.charging_type,
       j.payment_method,
       j.reason_code,
       j.deposit_bank,
       j.business_entity,
       j.section_category,
       j.product_type,
       j.channel_code,
       case when j.cr_dr_ind = 'DR' then 1 else -1 end *j.activity_amt as activity_amt,
       max(decode(z.ACNT_TYPE,'*',0,z.account_type_prty)+
        decode(z.ACNT_SUB_TYPE,'*',0,z.acnt_sub_type_prty)+
        decode(z.CHARGING_TYPE,'*',0,z.charging_type_prty)+
        decode(z.PAYMENT_METHOD,'*',0,z.pmt_method_prty)+
        decode(z.REASON_CODE,'*',0,z.reason_code_prty)+
        decode(z.DEPOSIT_BANK_CODE,'*',0,z.deposit_bank_prty)+
        decode(z.BUSINESS_ENTITY,'*',0,z.business_ent_prty)+
        decode(z.SECTION_CATEGORY,'*',0,z.sec_category_prty)+
        decode(z.PRODUCT_TYPE,'*',0,z.product_type_prty)+
        decode(z.CHANNEL_CODE,'*',0,z.channel_code_prty)) as match_value
      from
       journal j,
       gl_matrix z
      where
       j.logical_acnt = z.logical_account and
       j.acnt_type = decode(z.ACNT_TYPE,'*',j.acnt_type,z.ACNT_TYPE) and
       j.acnt_sub_type = decode(z.ACNT_SUB_TYPE,'*',j.acnt_sub_type,z.ACNT_SUB_TYPE) and
       nvl(j.charging_type,'NULL') = decode(z.CHARGING_TYPE,'*',nvl(j.charging_type,'NULL'),z.CHARGING_TYPE) and
       nvl(j.payment_method,'NULL') = decode(z.PAYMENT_METHOD,'*',nvl(j.payment_method,'NULL'),z.PAYMENT_METHOD) and 
       nvl(j.reason_code,'NULL') = decode(z.REASON_CODE,'*',nvl(j.reason_code,'NULL'),z.REASON_CODE) and
       nvl(j.deposit_bank,'NULL') = decode(z.DEPOSIT_BANK_CODE,'*',nvl(j.deposit_bank,'NULL'),z.DEPOSIT_BANK_CODE) and
       nvl(j.business_entity,'NULL') = decode(z.BUSINESS_ENTITY,'*',nvl(j.business_entity,'NULL'),z.BUSINESS_ENTITY) and
       nvl(j.section_category,'NULL') = decode(z.SECTION_CATEGORY,'*',nvl(j.section_category,'NULL'),z.SECTION_CATEGORY) and
       nvl(j.product_type,'NULL') = decode(z.PRODUCT_TYPE,'*',nvl(j.product_type,'NULL'),z.PRODUCT_TYPE) and
       nvl(j.channel_code,'NULL') = decode(z.CHANNEL_CODE,'*',nvl(j.channel_code,'NULL'),z.CHANNEL_CODE) and

       --last_day(trunc(j.ctcr_batch_prog_dt)) = to_date('2019-12-31','yyyy-mm-dd')
       trunc(j.ctcr_batch_prog_dt) <= to_date('2024-05-31','yyyy-mm-dd') and
       trunc(j.ctcr_batch_prog_dt) >= to_date('2024-05-01','yyyy-mm-dd') 
      group by
       trunc(j.ctcr_batch_prog_dt),
       j.trans_id,
       j.journal_id,
       j.logical_acnt,
       j.acnt_type,
       j.acnt_sub_type,
       j.charging_type,
       j.payment_method,
       j.reason_code,
       j.deposit_bank,
       j.business_entity,
       j.section_category,
       j.product_type,
       j.channel_code,
       case when j.cr_dr_ind = 'DR' then 1 else -1 end *j.activity_amt 
       ) m,
       gl_matrix z
      where
       m.logical_acnt = z.logical_account and
       m.acnt_type = decode(z.ACNT_TYPE,'*',m.acnt_type,z.ACNT_TYPE) and
       m.acnt_sub_type = decode(z.ACNT_SUB_TYPE,'*',m.acnt_sub_type,z.ACNT_SUB_TYPE) and
       nvl(m.charging_type,'NULL') = decode(z.CHARGING_TYPE,'*',nvl(m.charging_type,'NULL'),z.CHARGING_TYPE) and
       nvl(m.payment_method,'NULL') = decode(z.PAYMENT_METHOD,'*',nvl(m.payment_method,'NULL'),z.PAYMENT_METHOD) and 
       nvl(m.reason_code,'NULL') = decode(z.REASON_CODE,'*',nvl(m.reason_code,'NULL'),z.REASON_CODE) and
       nvl(m.deposit_bank,'NULL') = decode(z.DEPOSIT_BANK_CODE,'*',nvl(m.deposit_bank,'NULL'),z.DEPOSIT_BANK_CODE) and
       nvl(m.business_entity,'NULL') = decode(z.BUSINESS_ENTITY,'*',nvl(m.business_entity,'NULL'),z.BUSINESS_ENTITY) and
       nvl(m.section_category,'NULL') = decode(z.SECTION_CATEGORY,'*',nvl(m.section_category,'NULL'),z.SECTION_CATEGORY) and
       nvl(m.product_type,'NULL') = decode(z.PRODUCT_TYPE,'*',nvl(m.product_type,'NULL'),z.PRODUCT_TYPE) and
       nvl(m.channel_code,'NULL') = decode(z.CHANNEL_CODE,'*',nvl(m.channel_code,'NULL'),z.CHANNEL_CODE) and
      (decode(z.ACNT_TYPE,'*',0,z.account_type_prty)+
        decode(z.ACNT_SUB_TYPE,'*',0,z.acnt_sub_type_prty)+
        decode(z.CHARGING_TYPE,'*',0,z.charging_type_prty)+
        decode(z.PAYMENT_METHOD,'*',0,z.pmt_method_prty)+
        decode(z.REASON_CODE,'*',0,z.reason_code_prty)+
        decode(z.DEPOSIT_BANK_CODE,'*',0,z.deposit_bank_prty)+
        decode(z.BUSINESS_ENTITY,'*',0,z.business_ent_prty)+
        decode(z.SECTION_CATEGORY,'*',0,z.sec_category_prty)+
        decode(z.PRODUCT_TYPE,'*',0,z.product_type_prty)+
        decode(z.CHANNEL_CODE,'*',0,z.channel_code_prty)) = match_value


