
DECLARE
    record_count INT;
    start_date VARCHAR(40);
    end_date VARCHAR(40);
BEGIN

/******************************************************************************
 NAME:       [sp_po_mgmt_pur_item]
 PURPOSE:    Purchasing Table in SAP MM application, which stores Purchasing Document Item data.
 SYSTEM:     REDSHIFT(KORTEX)
 
 REVISIONS:
 Ver        Date        Author           Description
 ---------  ----------  ---------------  ------------------------------------
 1.0        11/22/2022   INCS1H06         Created.
 1.1		08/31/2023   Payal			  Addition of fields for the procurement landscape project
 2.0        11/30/2023	 Payal            Removed Orphan Records
 2.1		07/19/2024	 Adithyan		  Updated the keys in delete statement
 
 This stored procedure takes the daily delta from Kortex staging area and inserts the records into domain table po_mgmt_pur_item_1.
 
 ******************************************************************************/

    start_date := from_date;
    end_date := to_date;

    -- If both start date and end date is passed then delete the records
    -- from start date to end date in domain table
    IF start_date <> '' AND end_date <> '' THEN
        DELETE FROM klg_nga_kna.prcurmt.po_mgmt_pur_item WHERE kortex_upld_ts > start_date AND kortex_upld_ts <= end_date;
        GET DIAGNOSTICS record_count = ROW_COUNT;
		RAISE INFO 'record_count deleted from model = %, start_date = %, end_date = %', record_count, start_date, end_date;
    END IF;

    -- If start date and end date is present then we will consider that
    -- Otherwise max kortex upload timestamp will be start date
    -- and current timestamp will be end date
    IF start_date = '' THEN
        SELECT coalesce(DATEADD(day,-7,MAX(kortex_upld_ts)),'1990-01-01') FROM klg_nga_kna.prcurmt.po_mgmt_pur_item INTO start_date;        
    END IF;

    IF end_date = '' THEN
        SELECT GETDATE() INTO end_date;
    END IF;

    -- basic date and count validations
    IF TO_TIMESTAMP(start_date, 'YYYY-MM-DD HH:MI:SS') > TO_TIMESTAMP(end_date, 'YYYY-MM-DD HH:MI:SS') THEN 
        RAISE EXCEPTION 'start date = %, cannot be greater than end date = %', start_date, end_date;
    END IF;

    SELECT COUNT(1) FROM klg_nga_kna.stage.kna_ecc_po_ekpo WHERE kortex_upld_ts > start_date AND kortex_upld_ts <= end_date INTO record_count;
    IF record_count = 0 THEN RAISE
        info 'Records are not present for the given start date= %, and end date= %', start_date, end_date;
    END IF;
   
   
   -- get original load date from CDPOS and CDHDR tables
    DROP TABLE IF EXISTS CDHDR_CDPOS_EKPO_TEMP_TABLE;
    CREATE TEMP TABLE CDHDR_CDPOS_EKPO_TEMP_TABLE AS (
        SELECT
            hdr.obj_id AS objectid,
            hdr.chg_doc_nbr AS changenr,
            cast(
                (hdr.chg_dt :: text + ' ' :: text + hdr.chg_time :: text) AS TIMESTAMP
            ) AS udatetime,
            hdr.chg_dt AS udate,
            hdr.chg_time AS utime,
            pos.table_nm AS tabname,
            pos.table_key AS tabkey,
            pos.field_nm AS fname,
            pos.old_uom AS unit_old,
            pos.old_val AS value_old
        FROM
            klg_nga_kna.info_sec_mgmt.chg_mgmt_chg_log_hdr hdr
            INNER JOIN klg_nga_kna.info_sec_mgmt.chg_mgmt_chg_log_item pos ON (
                hdr.obj_id = pos.obj_id
                AND hdr.chg_doc_nbr = pos.chg_doc_nbr
            )
        WHERE (pos.table_nm = 'EKPO' AND pos.field_nm = 'MENGE')
        AND (EXTRACT(YEAR FROM udatetime) BETWEEN EXTRACT(YEAR FROM start_date::DATE) AND EXTRACT(YEAR FROM end_date::DATE))
    );

    DROP TABLE IF EXISTS EKPO_ORIG_LOAD_DT_TEMP_TABLE;
    CREATE TEMP TABLE EKPO_ORIG_LOAD_DT_TEMP_TABLE AS (
        SELECT DISTINCT
            ldt.objectid,
            ldt.changenr,
            ldt.udatetime,
            ldt.udate,
            ldt.utime,
            ldt.tabname,
            ldt.tabkey,
            ldt.fname,
            ldt.unit_old AS orig_pur_order_qty_uom,
            ldt.value_old::DECIMAL(15,3) AS orig_pur_order_qty
        FROM
            CDHDR_CDPOS_EKPO_TEMP_TABLE ldt
            INNER JOIN (
                SELECT
                    b.objectid,
                    min(a.changenr) AS changenr,
                    a.tabname,
                    b.tabkey,
                    a.udatetime
                FROM
                    CDHDR_CDPOS_EKPO_TEMP_TABLE a
                    INNER JOIN (
                        SELECT
                            objectid,
                            tabname,
                            tabkey,
                            min(udatetime) AS udatetime
                        FROM
                            CDHDR_CDPOS_EKPO_TEMP_TABLE
                        GROUP BY
                            objectid,
                            tabname,
                            tabkey
                    ) b ON (
                        a.objectid = b.objectid
                        AND a.tabkey = b.tabkey
                        AND a.tabname = b.tabname
                        AND a.udatetime = b.udatetime
                    )
                GROUP BY
                    b.objectid,
                    a.tabname,
                    b.tabkey,
                    a.udatetime
            ) c ON (
                c.changenr = ldt.changenr
                AND c.objectid = ldt.objectid
                AND c.tabkey = ldt.tabkey
                AND c.tabname = ldt.tabname
                AND c.udatetime = ldt.udatetime
            )
    );
   
   
   -- create material uom temp table
	DROP TABLE IF EXISTS MATRL_ALTN_UOM__TEMP;
	CREATE TEMP TABLE MATRL_ALTN_UOM__TEMP AS (
	    SELECT matrl_nbr,
	        buom,
	        altn_uom,
	        conv_fctr
	    FROM matrl_mstr.matrl_altn_uom
	);

    -- query to be executed between dates with records
    RAISE INFO 'query to be executed between dates start_date = %, and end_date = %, with record_count=%', start_date, end_date, record_count;

    DROP TABLE IF EXISTS stage;
    CREATE TEMP TABLE stage AS(
       with tmp_ste as ( SELECT
			DISTINCT
				itm.ebeln,
				itm.ebelp,
				itm.matnr,
				itm.menge,
				itm.meins,
				itm.netpr,
				itm.matkl,
				itm.werks,
				CASE WHEN itm.meins <> '' and itm.meins <> 'CS' THEN
			      (round(itm.menge / (SELECT conv_fctr FROM MATRL_ALTN_UOM__TEMP WHERE matrl_nbr = itm.matnr AND altn_uom =itm.meins ),6))
	    		ELSE 0
		  		END AS pur_order_base_qty,
				CASE WHEN itm.meins <> '' and itm.meins <> 'CS' THEN
					    (round (pur_order_base_qty * (SELECT conv_fctr FROM MATRL_ALTN_UOM__TEMP WHERE matrl_nbr = itm.matnr AND altn_uom ='CS'),6))
		    	ELSE itm.menge
		  		END AS pur_order_cs_qty,
				ldt1.orig_pur_order_qty,
				CASE WHEN ldt1.orig_pur_order_qty_uom <> '' and ldt1.orig_pur_order_qty_uom <> 'CS' THEN
			      (round(ldt1.orig_pur_order_qty / (SELECT conv_fctr FROM MATRL_ALTN_UOM__TEMP WHERE matrl_nbr = itm.matnr AND altn_uom =ldt1.orig_pur_order_qty_uom ),6))
	    		ELSE 0
		  		END AS orig_order_base_qty,
				CASE WHEN ldt1.orig_pur_order_qty_uom <> '' and ldt1.orig_pur_order_qty_uom <> 'CS' THEN
					    (round (orig_order_base_qty * (SELECT conv_fctr FROM MATRL_ALTN_UOM__TEMP WHERE matrl_nbr = itm.matnr AND altn_uom ='CS'),6))
		    	ELSE ldt1.orig_pur_order_qty
		  		END AS orig_pur_order_cs_qty,
                CAST(
                CASE
                    WHEN ul.buom = 'CS' THEN 1/ul.conv_fctr
                    WHEN ul.buom <> 'CS' and ldt1.orig_pur_order_qty_uom IS NOT NULL THEN cs.conv_fctr/ul.conv_fctr
                    ELSE 0
	                END as DECIMAL(15,3)
		           )as cs_per_ul_fctr,
	        	CAST(   
	        	CASE WHEN lyr.buom = 'CS' THEN 1/lyr.conv_fctr
	                 WHEN lyr.buom <> 'CS' and ldt1.orig_pur_order_qty_uom IS NOT NULL THEN cs.conv_fctr/lyr.conv_fctr
	                 ELSE 0
	                END as DECIMAL(15,3)
	            )as cs_per_lyr_fctr,
                itm.knttp,
				itm.loekz,
				itm.pstyp,
				itm.mwskz,
				itm.peinh,
				itm.konnr,
				itm.ktpnr,
				itm.aedat,
				itm.txz01,
				itm.bukrs,
				itm.lgort,
				itm.bednr,
				itm.infnr,
				itm.ktmng,
				itm.bprme,
				itm.netwr,
				itm.brtwr,
				itm.webaz,
				itm.ntgew,
				itm.gewei,
				itm.mtart,
				itm.afnam,
				itm.elikz,
				itm.erekz,
				itm.lmein,
				itm.webre,
				itm.xersy,
				itm.repos,
				itm.wepos,
				itm.zwert,
				itm.banfn,
				itm.bnfpo,
				itm.brgew,
				itm.prio_urg,
				itm.plifz,
				itm.src_nm ,
				md5(itm.src_nm|| itm.ebeln||itm.ebelp) as hash_key,
				itm.kortex_upld_ts,
				itm.kortex_dprct_ts
		FROM klg_nga_kna.stage.kna_ecc_po_ekpo as itm
--		LEFT OUTER JOIN klg_nga_kna.log.log_exec_delvry_item del on (del.ref_doc_nbr=itm.ebeln)
		LEFT OUTER JOIN EKPO_ORIG_LOAD_DT_TEMP_TABLE ldt1 ON (
            ldt1.objectid = itm.ebeln
            AND CAST(SUBSTRING(ldt1.tabkey, 14, 6) AS INT) = CAST(itm.ebelp AS INT)
            AND ldt1.tabname = 'EKPO' AND ldt1.FNAME = 'MENGE')
        LEFT OUTER JOIN MATRL_ALTN_UOM__TEMP ul ON (
        itm.matnr = ul.matrl_nbr
        AND ul.altn_uom = 'UL')
	    LEFT OUTER JOIN MATRL_ALTN_UOM__TEMP lyr ON (
	        itm.matnr = lyr.matrl_nbr
	        AND lyr.altn_uom = 'LYR')
	    LEFT OUTER JOIN MATRL_ALTN_UOM__TEMP cs ON (
	        itm.matnr = cs.matrl_nbr
	        AND cs.altn_uom = 'CS')
		WHERE 
			itm.kortex_upld_ts > start_date
		AND 
			itm.kortex_upld_ts <= end_date
		AND 
			 itm.ebeln in (select pur_doc_nbr from klg_nga_kna.prcurmt.po_mgmt_pur_hdr)
			 ) 
		SELECT *,
			nvl(orig_pur_order_qty,menge) as u_orig_pur_order_qty,
			nvl(orig_pur_order_cs_qty,pur_order_cs_qty) as u_orig_pur_order_cs_qty
		FROM tmp_ste
    );
	
   	   --Deleting last 7 days records  
    DELETE FROM klg_nga_kna.prcurmt.po_mgmt_pur_item 
    WHERE kortex_upld_ts > start_date AND kortex_upld_ts <= end_date;

   
    -- Delete existing records
    DELETE from klg_nga_kna.prcurmt.po_mgmt_pur_item
    USING stage WHERE (stage.ebeln = po_mgmt_pur_item.pur_doc_nbr
    and stage.ebelp = po_mgmt_pur_item.pur_doc_line_nbr
    );
    
	--Inserting the data into the table klg_nga_kna.prcurmt.po_mgmt_pur_item	
    INSERT INTO klg_nga_kna.prcurmt.po_mgmt_pur_item 
    (
        select
			stg.ebeln as pur_doc_nbr,
			stg.ebelp as pur_doc_line_nbr,
			stg.matnr as matrl_nbr,
			stg.menge as pur_order_qty,
			stg.meins as puom,
			stg.netpr as pur_net_price_val,
			stg.matkl as matrl_group_cd,
			stg.werks as ship_to_loc_nbr,
			stg.knttp as acct_asgmt_catg_cd,
			stg.loekz as pur_item_del_ind,
			stg.pstyp as item_catg_cd,
			stg.mwskz as pur_tax_cd,
			stg.peinh as price_unit_val,
			stg.konnr as pur_item_agrmt_nbr,
			stg.ktpnr as pur_item_agrmt_line_nbr,
			stg.aedat as pur_item_cre_dt,
			stg.txz01 as pur_item_txt,
			stg.bukrs as co_cd,
			stg.lgort as strg_loc_cd,
			stg.bednr as reqmt_track_nbr,
			stg.infnr as pir_nbr,
			stg.ktmng as pur_trgt_qty,
			stg.bprme as pur_price_uom,
			stg.netwr as net_order_val,
			stg.brtwr as gross_order_val,
			stg.webaz as gr_proc_day_nbr,
			stg.ntgew as net_wgt_val,
			stg.gewei as wgt_uom,
			stg.mtart as matrl_type_cd,
			stg.afnam as reqr_id,
			stg.elikz as delvry_cmplt_ind,
			stg.erekz as final_invc_ind,
			stg.lmein as buom,
			stg.webre as allow_invc_verf_ind,
			stg.xersy as allow_ers_ind,
			stg.repos as allow_ir_ind,
			stg.wepos as allow_gr_ind,
			stg.zwert as agrmt_trgt_val,
			stg.banfn as pur_req_nbr,
			stg.bnfpo as pur_req_line_nbr,
			stg.brgew as gross_wgt_val,
			stg.prio_urg as reqmt_urg_cd,
			stg.plifz as plan_delvry_day_nbr,
			stg.src_nm as src_nm,
			stg.hash_key,
			stg.kortex_upld_ts,
			stg.kortex_dprct_ts,
			stg.pur_order_cs_qty,
			stg.u_orig_pur_order_qty as orig_pur_order_qty,
			stg.u_orig_pur_order_cs_qty as orig_pur_order_cs_qty,
			stg.cs_per_ul_fctr,
			stg.cs_per_lyr_fctr
    	FROM stage stg
	);

	-- count check after successful load
	GET DIAGNOSTICS record_count = ROW_COUNT;
    RAISE INFO 'ekpo : klg_nga_kna.prcurmt.po_mgmt_pur_item successfully loaded with record_count = %, start_date = %, end_date = %', record_count, start_date, end_date;
    
   	COMMIT;

   -- Raise exception if any
	EXCEPTION WHEN OTHERS THEN
		RAISE INFO 'An exception occurred in klg_nga_kna.prcurmt.sp_po_mgmt_pur_item : %',SQLERRM;
		rollback;

END;
