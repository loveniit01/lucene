package com.eseal.indexing;

import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;

import com.eseal.conn.DBConnection;

@Service
public class LuceneIndexing {

	public LuceneIndexing() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String... str) {
		LuceneIndexing indexing = new LuceneIndexing();
		indexing.indexData();
	}

	@Autowired
	DBConnection conn;

	PreparedStatement ps = null;
	ResultSet rs = null;

	@Value("${index.path}")
	private String index_location;

	public void indexData() {

		List<String> specialCharacter = new ArrayList<>();
		specialCharacter.add("\\");
		specialCharacter.add("+");
		specialCharacter.add("-");
		specialCharacter.add("&&");
		specialCharacter.add("||");
		specialCharacter.add("!");
		specialCharacter.add("(");
		specialCharacter.add(")");
		specialCharacter.add("{");
		specialCharacter.add("}");
		specialCharacter.add("[");
		specialCharacter.add("]");
		specialCharacter.add("^");
		specialCharacter.add("\"");
		specialCharacter.add("~");
		specialCharacter.add("*");
		specialCharacter.add("?");
		specialCharacter.add(":");

		// System.out.println(specialCharacter);

		/**
		 * get data from database
		 */

		System.out.println("===loxcation---" + index_location);

		/*
		 * String sql = "\n" + "SELECT\n" + "     *\n" + " FROM\n" + "     (\n" +
		 * "         SELECT\n" + "             nor_be_id,\n" +
		 * "             nor_be_no,\n" + "             nor_be_dt,\n" +
		 * "             nor_be_loc,\n" + "             nor_be_countryoforigin,\n" +
		 * "             nor_be_pos,\n" + "             nor_be_apprasing_group,\n" +
		 * "             nor_be_cha_no,\n" + "             nor_be_iec,\n" +
		 * "             nor_be_imp_name,\n" + "             iscurrent,\n" +
		 * "             unlocode,\n" + "             nor_item_cth,\n" +
		 * "             nor_item_desc1,\n" + "             nor_item_desc2,\n" +
		 * "             nor_item_model,\n" + "             nor_item_brand,\n" +
		 * "             nor_item_bcd_notn,\n" + "             nor_item_bcd_nsno,\n" +
		 * "             nor_item_cvd_notn,\n" + "             nor_item_cvd_nsno\n" +
		 * "         FROM\n" + "             (\n" + "                 SELECT\n" +
		 * "                     nor_be_id,\n" + "                     nor_be_no,\n" +
		 * "                     TO_CHAR(nor_be_dt,'DD/MM/YYYY') AS nor_be_dt,\n" +
		 * "                     nor_be_loc,\n" +
		 * "                     nor_be_countryoforigin,\n" +
		 * "                     nor_be_pos,\n" +
		 * "                     nor_be_apprasing_group,\n" +
		 * "                     nor_be_cha_no,\n" +
		 * "                     nor_be_iec,\n" +
		 * "                     nor_be_imp_name,\n" +
		 * "                     'N' AS iscurrent,\n" +
		 * "                     unlocode,\n" + "                     nor_item_cth,\n" +
		 * "                     nor_item_desc1,\n" +
		 * "                     nor_item_desc2,\n" +
		 * "                     nor_item_model,\n" +
		 * "                     nor_item_brand,\n" +
		 * "                     nor_item_bcd_notn,\n" +
		 * "                     nor_item_bcd_nsno,\n" +
		 * "                     nor_item_cvd_notn,\n" +
		 * "                     nor_item_cvd_nsno,\n" +
		 * "                     ROW_NUMBER() OVER(\n" +
		 * "                         ORDER BY\n" +
		 * "                             nor_be_dt DESC,nor_be_loc ASC,nor_be_no DESC\n"
		 * + "                     ) rnk\n" + "                 FROM\n" +
		 * "                     m_location,\n" + "                     imp_be_duty,\n"
		 * + "                     imp_be_igm_cargo_sea,\n" +
		 * "                     nor_trans_data,\n" +
		 * "                     imp_be_ctx\n" + "                 WHERE\n" +
		 * "                     imp_be_duty.duty_be_id = nor_trans_data.nor_be_id\n" +
		 * "                     AND imp_be_duty.duty_inv_no = nor_trans_data.nor_item_inv_no\n"
		 * +
		 * "                     AND imp_be_duty.duty_item_no = nor_trans_data.nor_item_no\n"
		 * +
		 * "                     AND imp_be_igm_cargo_sea.ics_be_id (+) = nor_trans_data.nor_be_id\n"
		 * +
		 * "                     AND imp_be_ctx.ctx_be_id = nor_trans_data.nor_be_id\n"
		 * +
		 * "                     AND m_location.location_id = nor_trans_data.nor_be_loc\n"
		 * + "                     AND ROWNUM < 65000     \n" +
		 * "                 ORDER BY\n" + "                     nor_be_no \n" +
		 * "             )\n" + "     )";
		 */

		String sql = "SELECT\r\n" + "     nor_be_no,\r\n" + "     nor_be_id,\r\n"
				+ "   TO_CHAR(nor_be_dt,'YYYYMMDD') as nor_be_dt,\r\n" + "     nor_be_loc,\r\n"
				+ "     nor_be_countryoforigin,\r\n" + "     nor_be_pos,\r\n" + "     nor_be_apprasing_group,\r\n"
				+ "     nor_be_cha_no,\r\n" + "     nor_be_iec,\r\n" + "     nor_be_imp_name,\r\n"
				+ "     unlocode,\r\n" + "     nor_item_cth,\r\n" + "     nor_item_desc1,\r\n"
				+ "     nor_item_desc2,\r\n" + "     nor_item_model,\r\n" + "     nor_item_brand,\r\n"
				+ "     nor_item_bcd_notn,\r\n" + "     nor_item_bcd_nsno,\r\n" + "     nor_item_cvd_notn,\r\n"
				+ "     nor_item_cvd_nsno,\r\n" + "     nor_inv_freight,\r\n"
				+ "     nor_inv_assd_tot_duty_foregone,\r\n" + "     nor_inv_sup_nm,\r\n" + "     nor_inv_ass_val,\r\n"
				+ "     nor_item_gen_desc,\r\n" + "     nor_be_wbe_dt,\r\n" + "     nor_be_tot_duty,\r\n"
				+ "     nor_be_first_chk,\r\n" + "     nor_be_tot_duty_foregone,\r\n" + "     nor_be_prior_be,\r\n"
				+ "     nor_item_tarf_notn,\r\n" + "     nor_be_kachchabe,\r\n" + "     nor_be_hss,\r\n"
				+ "     nor_be_portoforigin,\r\n" + "     nor_inv_svb_dt,\r\n" + "     nor_inv_payment_terms,\r\n"
				+ "     nor_item_no,\r\n" + "     nor_item_bcd_rta,\r\n" + "     nor_item_and_amt,\r\n"
				+ "     nor_hdf_high_risk_importers,\r\n" + "     nor_hdf_dormancy,\r\n" + "     nor_hdf_turnover,\r\n"
				+ "     nor_imp_city,\r\n" + "     nor_item_bcd_amt_fg,\r\n" + "     nor_item_and_amts,\r\n"
				+ "     nor_inv_sup_cntry,\r\n" + "     nor_item_cvd05_notn,\r\n" + "     nor_item_cvd05_nsno,\r\n"
				+ "     nor_be_igm_no,\r\n" + "     nor_be_igm_yr,\r\n" + "     nor_item_section,\r\n"
				+ "     nor_idf_tot_be,\r\n" + "     nor_be_inw_dt,\r\n" + "     nor_hdf_ports,\r\n"
				+ "     nor_hdf_cha_age,\r\n" + "     nor_hdf_cha_acp,\r\n" + "     nor_hdf_tot_amendment,\r\n"
				+ "     nor_hdf_tot_bes,\r\n" + "     nor_hdf_import_offence,\r\n" + "     nor_cdf_value_loading,\r\n"
				+ "     nor_idf_tot_duty,\r\n" + "     nor_idf_iec_age,\r\n" + "     nor_idf_iec_dormancy,\r\n"
				+ "     nor_idf_iec_last_usage,\r\n" + "     nor_idf_tot_redemption_fine,\r\n"
				+ "     nor_idf_tot_ports,\r\n" + "     nor_idf_tot_penalty,\r\n" + "     nor_idf_tot_chas,\r\n"
				+ "     nor_idf_tot_value_loaded,\r\n" + "     nor_idf_tot_amendments,\r\n" + "     nor_idf_ischa,\r\n"
				+ "     nor_idf_tot_turnover,\r\n" + "     nor_item_duty_fg,\r\n" + "     nor_item_para_no,\r\n"
				+ "     nor_item_svb_flg,\r\n" + "     nor_customrevenuedefaulter,\r\n" + "     nor_city,\r\n"
				+ "     nor_be_pos_grp_cd,\r\n" + "     nor_imp_add1,\r\n" + "     nor_imp_add2,\r\n"
				+ "     nor_imp_class,\r\n" + "     nor_imp_pin,\r\n" + "     nor_be_coo_grp_cd,\r\n"
				+ "     nor_inv_misc_rt,\r\n" + "     nor_inv_valuation_mthd,\r\n" + "     nor_inv_insurance,\r\n"
				+ "     nor_item_itemc,\r\n" + "     nor_item_cvd_amt_fg,\r\n" + "     nor_item_oth1_nsno,\r\n"
				+ "     nor_item_and_rta,\r\n" + "     nor_be_cc,\r\n" + "     nor_item_safg_rta,\r\n"
				+ "     nor_item_safg_amt,\r\n" + "     nor_idf_istrader,\r\n" + "     nor_be_pos_nm,\r\n"
				+ "     nor_item_uav,\r\n" + "     nor_iec_status,\r\n" + "     nor_iec_issue_dt,\r\n"
				+ "     nor_cha_add1,\r\n" + "     nor_item_cvd_rta,\r\n" + "     nor_item_joint_description,\r\n"
				+ "     nor_be_cntry_pos,\r\n" + "     nor_item_coo_grp_cd,\r\n" + "     nor_pan,\r\n"
				+ "     nor_be_port_nm,\r\n" + "     nor_inv_svb_no,\r\n" + "     nor_be_countryofconsignment,\r\n"
				+ "     nor_be_govt,\r\n" + "     nor_be_wbe_no,\r\n" + "     nor_be_wcc,\r\n"
				+ "     nor_be_tot_ass_val,\r\n" + "     nor_be_green_channel,\r\n" + "     nor_be_warehouse_cd,\r\n"
				+ "     nor_be_sec_48,\r\n" + "     nor_be_tot_fine,\r\n" + "     nor_be_tot_penal,\r\n"
				+ "     nor_inv_actual_inv_no,\r\n" + "     nor_inv_sell_add1,\r\n" + "     nor_inv_brk_nm,\r\n"
				+ "     nor_inv_no,\r\n" + "     nor_inv_dt,\r\n" + "     nor_inv_contract_no,\r\n"
				+ "     nor_inv_contract_dt,\r\n" + "     nor_inv_assd_frgt,\r\n" + "     nor_inv_assed_misc_chrg,\r\n"
				+ "     nor_inv_svb_flg,\r\n" + "     nor_inv_tot_duty_foregone,\r\n"
				+ "     nor_inv_assd_tot_duty,\r\n" + "     nor_inv_val,\r\n" + "     nor_inv_ins_rt,\r\n"
				+ "     nor_inv_royalty,\r\n" + "     nor_inv_assd_landing,\r\n" + "     nor_item_hlth_nsno,\r\n"
				+ "     nor_item_assess_val,\r\n" + "     nor_item_final,\r\n" + "     nor_item_crg,\r\n"
				+ "     nor_item_ceth,\r\n" + "     nor_item_ad1_notn,\r\n" + "     nor_item_ad1_nsno,\r\n"
				+ "     nor_item_ad2_nsno,\r\n" + "     nor_item_oth_notn,\r\n" + "     nor_item_cess_notn,\r\n"
				+ "     nor_item_cess_nsno,\r\n" + "     nor_item_sapt_notn,\r\n" + "     nor_inv_tot_duty,\r\n"
				+ "     nor_be_branch_slno,\r\n" + "     nor_inv_svb_load_on_duty,\r\n" + "     nor_inv_disc_amt,\r\n"
				+ "     nor_item_end_use,\r\n" + "     nor_be_sec_46,\r\n" + "     nor_be_mode_of_submission,\r\n"
				+ "     nor_item_duty,\r\n" + "     nor_item_bcd_amt,\r\n" + "     nor_item_upi_curcd,\r\n"
				+ "     nor_inv_assd_inv_val,\r\n" + "     nor_inv_assd_assval,\r\n" + "     nor_inv_agency_comm,\r\n"
				+ "     nor_inv_ins_val,\r\n" + "     nor_be_type,\r\n" + "     nor_item_oth1_notn,\r\n"
				+ "     nor_be_mawb_no,\r\n" + "     nor_be_mawb_dt,\r\n" + "     nor_be_hawb_no,\r\n"
				+ "     nor_be_hawb_dt,\r\n" + "     nor_be_totpkg,\r\n" + "     nor_be_gross_wt,\r\n"
				+ "     nor_be_uqc,\r\n" + "     nor_be_pkg_cd,\r\n" + "     nor_be_tot_frgt,\r\n"
				+ "     nor_be_tot_ins,\r\n" + "     nor_be_a_tot_inv_val,\r\n" + "     nor_be_a_tot_frgt,\r\n"
				+ "     nor_be_a_tot_ins,\r\n" + "     nor_be_a_tot_misc,\r\n" + "     nor_be_a_tot_ass_val,\r\n"
				+ "     nor_be_a_tot_duty,\r\n" + "     nor_be_a_tot_duty_foregone,\r\n" + "     nor_be_pan,\r\n"
				+ "     nor_be_cus_site,\r\n" + "     nor_be_port_cd,\r\n" + "     nor_item_hlth_notn,\r\n"
				+ "     nor_inv_sell_name,\r\n" + "     nor_item_uqc,\r\n" + "     nor_item_manufacturer_nm,\r\n"
				+ "     nor_item_scd_notn,\r\n" + "     nor_item_and_notn,\r\n" + "     nor_item_acc,\r\n"
				+ "     nor_item_sapt_nsno,\r\n" + "     nor_inv_po_dt,\r\n" + "     nor_item_upi,\r\n"
				+ "     nor_item_ritc_cd,\r\n" + "     nor_inv_letterofcredit_no,\r\n" + "     nor_inv_sup_add1,\r\n"
				+ "     nor_inv_term,\r\n" + "     nor_inv_misc_chrg,\r\n" + "     nor_inv_relationship,\r\n"
				+ "     nor_inv_assd_ins_chrg,\r\n" + "     nor_inv_broker_comm,\r\n" + "     nor_item_ad2_notn,\r\n"
				+ "     nor_item_oth_nsno,\r\n" + "     nor_item_ecenvat_notn,\r\n" + "     nor_item_tarf_nsno,\r\n"
				+ "     nor_item_scd_nsno,\r\n" + "     nor_item_rsp,\r\n" + "     nor_be_modeoftransport,\r\n"
				+ "     nor_item_and_nsno,\r\n" + "     nor_inv_hss_load_rta,\r\n" + "     nor_inv_nature,\r\n"
				+ "     nor_inv_po_no,\r\n" + "     nor_inv_letterofcredit_dt,\r\n" + "     nor_rim_notn_no,\r\n"
				+ "     nor_rim_notn_slno,\r\n" + "     nor_rim_sb_inv_serno,\r\n" + "     nor_rim_sb_item_no,\r\n"
				+ "     nor_rim_sb_inv_no,\r\n" + "     nor_rim_exp_frt,\r\n" + "     nor_rim_exp_ins,\r\n"
				+ "     nor_rim_bcd_amt,\r\n" + "     nor_rim_cvd_amt,\r\n" + "     nor_rim_sad_amt,\r\n"
				+ "     nor_rim_bcd_amt_fg,\r\n" + "     nor_rim_cvd_amt_fg,\r\n" + "     nor_item_ecenvat_nsno,\r\n"
				+ "     nor_be_is_compliant,\r\n" + "     nor_be_filed_through_sc,\r\n"
				+ "     nor_item_joint_description2,\r\n" + "     nor_idf_iec_isproprietor,\r\n"
				+ "     nor_idf_iec_iscompany,\r\n" + "     nor_idf_iec_ishuf,\r\n"
				+ "     nor_idf_iec_ispartnership,\r\n" + "     nor_idf_iec_isassociateconcern,\r\n"
				+ "     nor_idf_iec_istrust,\r\n" + "     nor_idf_iec_isindividualbody,\r\n"
				+ "     nor_idf_iec_islocalbody,\r\n" + "     nor_idf_iec_isjuridicalperson,\r\n"
				+ "     nor_idf_iec_isgovernment,\r\n" + "     nor_item_svb_no,\r\n" + "     nor_item_svb_dt,\r\n"
				+ "     nor_item_svb_load,\r\n" + "     nor_item_svb_cs,\r\n" + "     nor_item_svb_load_on_duty,\r\n"
				+ "     nor_item_svb_sflg,\r\n" + "     nor_item_svb_dsflg,\r\n" + "     nor_inv_svb_sflg,\r\n"
				+ "     nor_inv_svb_dsflg,\r\n" + "     nor_inv_svb_ass_val,\r\n"
				+ "     nor_inv_auth_eco_operator,\r\n" + "     nor_inv_auth_eco_cntry,\r\n"
				+ "     nor_inv_auth_eco_role,\r\n" + "     nor_be_ucr_no,\r\n" + "     nor_inv_licence_fee,\r\n"
				+ "     nor_item_policy_yr,\r\n" + "     nor_item_cvd05_amt,\r\n" + "     nor_item_cvd05_rta,\r\n"
				+ "     nor_item_shedu_amt,\r\n" + "     nor_item_shedu_rta,\r\n" + "     nor_item_oth1_amt,\r\n"
				+ "     nor_item_oth1_rta,\r\n" + "     nor_item_cvd_amt,\r\n" + "     nor_be_is_aeo_supplier,\r\n"
				+ "     nor_item_mnfr_typ,\r\n" + "     nor_item_mnfr_code,\r\n" + "     nor_item_mnfr_add1,\r\n"
				+ "     nor_item_mnfr_add2,\r\n" + "     nor_item_mnfr_city,\r\n" + "     nor_item_mnfr_state,\r\n"
				+ "     nor_item_mnfr_pin,\r\n" + "     nor_item_mnfr_cntry,\r\n" + "     nor_item_src_cntry,\r\n"
				+ "     nor_item_transit_cntry,\r\n" + "     nor_item_typ_antidump,\r\n" + "     nor_rim_inv_no,\r\n"
				+ "     nor_rim_item_no,\r\n" + "     nor_rim_sb_no,\r\n" + "     nor_rim_sb_dt,\r\n"
				+ "     nor_be_isdov_hit,\r\n" + "     nor_rim_port,\r\n" + "     nor_item_add_notn,\r\n"
				+ "     nor_item_add_nsno,\r\n" + "     nor_item_safg_notn,\r\n" + "     nor_item_safg_nsno,\r\n"
				+ "     nor_item_shedu_notn,\r\n" + "     nor_item_shedu_nsno,\r\n" + "     nor_item_joint_notn_no,\r\n"
				+ "     nor_item_joint_notn_srno,\r\n" + "     nor_item_inv_no,\r\n" + "     nor_be_count,\r\n"
				+ "     nor_idf_tot_duty_foregone,\r\n" + "     nor_be_is_acp_client,\r\n" + "     nor_cha_name,\r\n"
				+ "     nor_cha_pan,\r\n" + "     nor_be_isrisky,\r\n" + "     nor_be_is_aeo_client,\r\n"
				+ "     nor_item_cess_amt,\r\n" + "     nor_be_isuqc_hit,\r\n" + "     nor_be_noinv,\r\n"
				+ "     nor_be_supplier_id,\r\n" + "     nor_be_supplier_cntry,\r\n" + "     nor_be_new_supplier,\r\n"
				+ "     nor_be_supplier_iec_cnt,\r\n" + "     nor_be_supplier_iec_cha_cnt,\r\n"
				+ "     nor_be_tot_igm_no,\r\n" + "     nor_be_tot_ctr_no,\r\n" + "     nor_be_noi,\r\n"
				+ "     nor_be_ispgaiechit,\r\n" + "     nor_item_cth_slno,\r\n" + "     nor_be_sender_id,\r\n"
				+ "     nor_igm_be_id,\r\n" + "     nor_igm_mrk_num1,\r\n" + "     nor_igm_mrk_num2,\r\n"
				+ "     nor_igm_mrk_num3,\r\n" + "     nor_igm_gateway_inward_dt,\r\n" + "     nor_ctr_lcfc,\r\n"
				+ "     nor_ctr_seal_no,\r\n" + "     nor_igm_no,\r\n" + "     nor_igm_gross_wt,\r\n"
				+ "     nor_igm_uqc,\r\n" + "     nor_igm_pkg_cd,\r\n" + "     nor_igm_dt,\r\n"
				+ "     nor_igm_gateway_igm_no,\r\n" + "     nor_igm_gateway_igm_yr,\r\n"
				+ "     nor_igm_port_report,\r\n" + "     nor_igm_mawb_no,\r\n" + "     nor_igm_mawb_dt,\r\n"
				+ "     nor_igm_hawb_no,\r\n" + "     nor_igm_hawb_dt,\r\n" + "     nor_igm_tot_pkg,\r\n"
				+ "     nor_ctr_no,\r\n" + "     nor_ctr_truck_no,\r\n" + "     nor_igm_yr,\r\n"
				+ "     nor_ctr_ooc_by,\r\n" + "     nor_ctr_ooc_no,\r\n" + "     nor_ctr_exm_by,\r\n"
				+ "     hss_imp_add2,\r\n" + "     hss_imp_city,\r\n" + "     hss_iec,\r\n" + "     hss_imp_name,\r\n"
				+ "     hss_imp_add1,\r\n" + "     rsp_notifi_sno,\r\n" + "     rsp_notifi_no,\r\n" + "     rsp,\r\n"
				+ "     rsp_qty_desc,\r\n" + "     depb_cvd_notn,\r\n" + "     depb_cvd_nsno,\r\n"
				+ "     depb_bcd_notn,\r\n" + "     depb_bcd_nsno,\r\n" + "     eou_cex_cd,\r\n"
				+ "     iid_master_nm,\r\n" + "     iid_igm_no,\r\n" + "     iid_igm_yr,\r\n"
				+ "     iid_vessel_nm,\r\n" + "     iid_inw_dt,\r\n" + "     iid_voyage_no,\r\n"
				+ "     iid_prior_igm,\r\n" + "     iid_smtp_dt,\r\n" + "     iid_smtp_no,\r\n"
				+ "     misc_charge,\r\n" + "     oshd_is_riskydescriptionhit,\r\n" + "     oshd_bill_score_avg,\r\n"
				+ "     oshd_is_cc_target_hit,\r\n" + "     oshd_is_brvhit,\r\n" + "     oshd_is_dovrulehit,\r\n"
				+ "     oshd_is_facilitated,\r\n" + "     oshd_is_sb_targethit,\r\n"
				+ "     oshd_is_nsb_target_hit,\r\n" + "     oshd_is_sb_rule_hit,\r\n"
				+ "     oshd_is_nsb_rule_hit,\r\n" + "     oshd_is_rule_hit,\r\n" + "     oshd_is_target_hit,\r\n"
				+ "     oshd_is_intervened,\r\n" + "     oshd_is_interventionhit,\r\n"
				+ "     oshd_is_acprandomhit,\r\n" + "     oshd_is_scorerandomhit,\r\n" + "     cert_no,\r\n"
				+ "     cert_dt,\r\n" + "     cert_type,\r\n" + "     cert_status,\r\n" + "     cert_regn_no,\r\n"
				+ "     cert_regn_dt,\r\n" + "     cert_cd,\r\n" + "     duty_igst_duty_amount,\r\n"
				+ "     duty_compensatory_amount,\r\n" + "     duty_igst_duty_amt_forgone,\r\n"
				+ "     duty_comp_amt_forgone,\r\n" + "     duty_ad_flg,\r\n" + "     duty_typ,\r\n"
				+ "     duty_exempt_notn_type,\r\n" + "     duty_uqc,\r\n" + "     duty_igst_levy_notn,\r\n"
				+ "     duty_igst_levy_nsno,\r\n" + "     duty_comp_levy_notn,\r\n" + "     duty_comp_levy_nsno,\r\n"
				+ "     duty_igst_exemption_notn,\r\n" + "     duty_igst_exemption_nsno,\r\n"
				+ "     duty_comp_exemption_notn,\r\n" + "     duty_comp_exemption_nsno,\r\n"
				+ "     duty_igst_exemption_cus_notn,\r\n" + "     duty_igst_exemption_cus_nsno,\r\n"
				+ "     duty_comp_exemption_cus_notn,\r\n" + "     duty_comp_exemption_cus_nsno,\r\n"
				+ "     duty_igst_rate,\r\n" + "     duty_compensatory_rate,\r\n" + "     duty_cth_levy_nsno,\r\n"
				+ "     bond_cd,\r\n" + "     bond_bank_gurantee_rate,\r\n" + "     bond_iscontinuity,\r\n"
				+ "     bond_sts,\r\n" + "     bond_no,\r\n" + "     bond_amt,\r\n" + "     bond_bank_gurantee_amt,\r\n"
				+ "     bond_portofregistration,\r\n" + "     exhg_rate,\r\n" + "     exhg_bank_nm,\r\n"
				+ "     exhg_certf_no,\r\n" + "     exhg_certf_dt,\r\n" + "     lic_istransferred,\r\n"
				+ "     lic_dt,\r\n" + "     lic_release_advise_no,\r\n" + "     lic_policy_para_no,\r\n"
				+ "     lic_debit_unit_of_measure,\r\n" + "     lic_debit_qty,\r\n" + "     lic_regn_no,\r\n"
				+ "     lic_cd,\r\n" + "     lic_no,\r\n" + "     lic_release_advise_dt,\r\n" + "     lic_regn_dt,\r\n"
				+ "     lic_debit_val,\r\n" + "     lic_policy_yr,\r\n" + "     lic_debit_duty,\r\n"
				+ "     isit_info_typ,\r\n" + "     isit_info_qfr,\r\n" + "     isit_info_cd,\r\n"
				+ "     isit_info_txt,\r\n" + "     isit_info_uqc,\r\n" + "     isit_info_msr,\r\n"
				+ "     isco_const_no,\r\n" + "     isco_const_elmnt_nm,\r\n" + "     isco_const_elmnt_cd,\r\n"
				+ "     isco_const_pct,\r\n" + "     isco_const_ypct,\r\n" + "     isco_act_ingr,\r\n"
				+ "     ispr_prod_batch_no,\r\n" + "     ispr_prod_batch_qty,\r\n" + "     ispr_uqc,\r\n"
				+ "     ispr_mnf_dt,\r\n" + "     ispr_exp_dt,\r\n" + "     ispr_best_bfr,\r\n"
				+ "     ispr_tot_shelf_life,\r\n" + "     ispr_resid_shelf_life,\r\n"
				+ "     ispr_resid_shelf_life_pct,\r\n" + "     itsc_ctrl_cd,\r\n" + "     itsc_ctrl_loc,\r\n"
				+ "     itsc_ctrl_start_dt,\r\n" + "     itsc_ctrl_end_dt,\r\n" + "     itsc_ctrl_res_cd,\r\n"
				+ "     itsc_ctrl_res_txt,\r\n" + "     ctx_typ,\r\n" + "     ctx_no,\r\n" + "     ics_goods_desc,\r\n"
				+ "     ics_gross_wt,\r\n" + "     ics_hawb_no,\r\n" + "     ics_hawb_dt,\r\n" + "     ics_igm_no,\r\n"
				+ "     ics_igm_yr,\r\n" + "     ics_imp_nm,\r\n" + "     ics_line_no,\r\n" + "     ics_mawb_no,\r\n"
				+ "     ics_mawb_dt,\r\n" + "     ics_port_ship,\r\n" + "     ics_tot_pkg,\r\n" + "     ics_uqc,\r\n"
				+ "     ics_vessel_cd,\r\n" + "     ics_voyage_no,\r\n" + "     ics_port_dest,\r\n" + "     pen_us,\r\n"
				+ "     pen_amt,\r\n" + "     sts_ag,\r\n" + "     csh_duty_amt,\r\n" + "     amd_cd,\r\n"
				+ "     perm_cd,\r\n" + "     hss_be_id,\r\n" + "     rsp_be_id,\r\n" + "     depb_be_id,\r\n"
				+ "     eou_be_id,\r\n" + "     iid_be_id,\r\n" + "     misc_be_id,\r\n" + "     oshd_be_id,\r\n"
				+ "     cert_be_id,\r\n" + "     duty_be_id,\r\n" + "     bond_be_id,\r\n" + "     exhg_be_id,\r\n"
				+ "     lic_be_id,\r\n" + "     isit_be_id,\r\n" + "     isco_be_id,\r\n" + "     ispr_be_id,\r\n"
				+ "     itsc_be_id,\r\n" + "     ctx_be_id,\r\n" + "     ics_be_id,\r\n" + "     pen_be_id,\r\n"
				+ "     sts_be_id,\r\n" + "     csh_be_id,\r\n" + "     amd_be_id,\r\n" + "perm_be_id, LOCATION_ID"
				+ ", TO_CHAR(NOR_BE_CREATION_DATE,'YYYYMMDD') AS NOR_BE_CREATION_DATE" + " FROM\r\n"
				+ "     nor_trans_data,\r\n" + "     nor_igms_cont_data,\r\n" + "     imp_be_hss,\r\n"
				+ "     imp_be_rsp,\r\n" + "     imp_be_depb,\r\n" + "     imp_be_exportorientedunit,\r\n"
				+ "     imp_be_igm_icd,\r\n" + "     imp_be_misc_charges,\r\n" + "     imp_onsub_hitting_details,\r\n"
				+ "     imp_be_certificate,\r\n" + "     imp_be_duty,\r\n" + "     imp_be_bond,\r\n"
				+ "     imp_be_exchange,\r\n" + "     imp_be_licence,\r\n" + "     imp_be_item_sw_info_type,\r\n"
				+ "     imp_be_item_sw_const,\r\n" + "     imp_be_item_sw_prod,\r\n" + "     imp_be_item_sw_ctrl,\r\n"
				+ "     imp_be_ctx,\r\n" + "     imp_be_igm_cargo_sea,\r\n" + "     imp_be_penal,\r\n"
				+ "     imp_be_status,\r\n" + "     imp_be_cash,\r\n" + "     imp_be_amend,\r\n"
				+ "     imp_be_permission,\r\n" + "     m_location\r\n" + " WHERE\r\n"
				+ "     nor_trans_data.nor_be_id = nor_igms_cont_data.nor_igm_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_hss.hss_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_rsp.rsp_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_depb.depb_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_exportorientedunit.eou_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_igm_icd.iid_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_misc_charges.misc_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_onsub_hitting_details.oshd_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_certificate.cert_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_duty.duty_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_bond.bond_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_exchange.exhg_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_licence.lic_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_item_sw_info_type.isit_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_item_sw_const.isco_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_item_sw_prod.ispr_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_item_sw_ctrl.itsc_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_ctx.ctx_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_igm_cargo_sea.ics_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_penal.pen_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_status.sts_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_cash.csh_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_amend.amd_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_id = imp_be_permission.perm_be_id (+)\r\n"
				+ "     AND nor_trans_data.nor_be_loc = m_location.location_id (+)\r\n" + " "
				
				+ "		AND imp_be_duty.duty_inv_no = nor_trans_data.nor_item_inv_no"
				+ "		AND imp_be_duty.duty_item_no = nor_trans_data.nor_item_no"
				+ "    AND ROWNUM < 265000\r\n"
				+ " ORDER BY\r\n" + "     nor_be_no";

		// String sql = "select NOR_BE_SENDER_ID from NOR_TRANS_DATA";

		System.out.println("===" + sql);
		IndexWriter writer = null;
		try {
			// Analyzer analyzer2 = new StandardAnalyzer();
			// IndexWriterConfig config = new IndexWriterConfig(analyzer2);
			// IndexWriter writer = new
			// IndexWriter(FSDirectory.open(Paths.get(index_location)), config);

			ps = DBConnection.getConnection().prepareStatement(sql);

			rs = ps.executeQuery();
			rs.getRow();
			System.out.println("row count====" + rs.getRow());
			Date start = new Date();

			Directory dir = FSDirectory.open(Paths.get(index_location));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			boolean create = true;

			if (create) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}

			writer = new IndexWriter(dir, iwc);
			// int x = 1/0;
			int i = 1;
			System.out.println("row count====" + rs.getRow());

			while (rs.next()) {

				String nor_be_id = rs.getString("nor_be_id");
				System.out.println(i + "~~~~~~~~~~~~~~~~~~~~~~~~~~~" + (nor_be_id));
				Document document = new Document();
				document.add(new StringField("nor_be_id",
						escaping_Special_Characters(stringConvert(nor_be_id), specialCharacter), Field.Store.YES));
				document.add(new StringField("nor_be_no",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_no")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_be_dt",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_dt")), specialCharacter),
						Field.Store.YES));

				document.add(new StringField("nor_be_loc",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_loc")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_be_countryoforigin", escaping_Special_Characters(
						stringConvert(rs.getString("nor_be_countryoforigin")), specialCharacter), Field.Store.YES));
				document.add(new StringField("nor_be_pos",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_pos")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_be_apprasing_group", escaping_Special_Characters(
						stringConvert(rs.getString("nor_be_apprasing_group")), specialCharacter), Field.Store.YES));
				document.add(new StringField("nor_be_cha_no",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_cha_no")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_be_iec",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_iec")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_be_imp_name",
						escaping_Special_Characters(stringConvert(rs.getString("nor_be_imp_name")), specialCharacter),
						Field.Store.YES));
				// document.add(new StringField("iscurrent",
				// escaping_Special_Characters(stringConvert(rs.getString("iscurrent")),
				// specialCharacter), Field.Store.YES));
				document.add(new StringField("unlocode",
						escaping_Special_Characters(stringConvert(rs.getString("unlocode")), specialCharacter),
						Field.Store.YES));
				// document.add(
				// new StringField("NOR_ITEM_CTH",
				// escaping_Special_Characters(stringConvert(rs.getString("NOR_ITEM_CTH ")),
				// Field.Store.YES));
				document.add(new StringField("nor_item_desc1",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_desc1")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_desc2",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_desc2")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_model",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_model")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_brand",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_brand")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_bcd_notn",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_bcd_notn")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_bcd_nsno",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_bcd_nsno")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_cvd_notn",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_cvd_notn")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("nor_item_cvd_nsno",
						escaping_Special_Characters(stringConvert(rs.getString("nor_item_cvd_nsno")), specialCharacter),
						Field.Store.YES));

				document.add(new StringField("nor_igm_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("nor_igm_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("hss_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("hss_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("rsp_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("rsp_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("depb_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("depb_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("eou_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("eou_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("iid_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("iid_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("misc_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("misc_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("oshd_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("oshd_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("cert_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("cert_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("duty_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("duty_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("bond_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("bond_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("exhg_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("exhg_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("lic_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("lic_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("isit_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("isit_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("isco_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("isco_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("ispr_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("ispr_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("itsc_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("itsc_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("ctx_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("ctx_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("ics_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("ics_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("pen_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("pen_be_id")), specialCharacter),
						Field.Store.YES));

				document.add(new StringField("sts_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("sts_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("csh_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("csh_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("amd_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("amd_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("perm_be_id",
						escaping_Special_Characters(stringConvert(rs.getString("perm_be_id")), specialCharacter),
						Field.Store.YES));
				document.add(new StringField("LOCATION_ID",
						escaping_Special_Characters(stringConvert(rs.getString("LOCATION_ID")), specialCharacter),
						Field.Store.YES));

				document.add(new StringField("NOR_BE_CREATION_DATE", escaping_Special_Characters(
						stringConvert(rs.getString("NOR_BE_CREATION_DATE")), specialCharacter), Field.Store.YES));

				// writer.updateDocument(new Term("nor_be_id", rs.getString("nor_be_id")),
				// document);

				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					// New index, so we just add the document (no old document can be there):
					System.out.println("adding " + index_location);
					writer.addDocument(document);
				} else {
					// Existing index (an old copy of this document may have been indexed) so
					// we use updateDocument instead to replace the old one matching the exact
					// path, if present:
					System.out.println("updating " + document);
					writer.updateDocument(new Term("path", document.toString()), document);
				}
				i++;
			}
			writer.close();
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (Exception e) {
				// TODO: handle exception
			}
			ps = null;
			rs = null;
		}

	}

	public String stringConvert(String getValue) {
		if (getValue == null) {
			// blank defined as null_blank
			return "null_blank";
		} else
			return getValue;

	}

	public String escaping_Special_Characters(String getValue, List<String> splCharacter) {

		for (String chractr : splCharacter) {
			if (getValue.contains(chractr)) {
				getValue = getValue.replace(chractr, "");
			}
		}
		return getValue;

	}

	/*
	 * String sql =
	 * "select NOR_BE_NO , NOR_BE_ID , NOR_BE_DT , NOR_BE_LOC , NOR_BE_COUNTRYOFORIGIN , NOR_BE_POS , NOR_BE_APPRASING_GROUP , NOR_BE_CHA_NO , NOR_BE_IEC , NOR_BE_IMP_NAME , UNLOCODE , NOR_ITEM_CTH , NOR_ITEM_DESC1 , NOR_ITEM_DESC2 , NOR_ITEM_MODEL , NOR_ITEM_BRAND , NOR_ITEM_BCD_NOTN , NOR_ITEM_BCD_NSNO , NOR_ITEM_CVD_NOTN , NOR_ITEM_CVD_NSNO , NOR_INV_FREIGHT , NOR_INV_ASSD_TOT_DUTY_FOREGONE , NOR_INV_SUP_NM , NOR_INV_ASS_VAL , NOR_ITEM_GEN_DESC , NOR_BE_WBE_DT , NOR_BE_TOT_DUTY , NOR_BE_FIRST_CHK , NOR_BE_TOT_DUTY_FOREGONE , NOR_BE_PRIOR_BE , NOR_ITEM_TARF_NOTN , NOR_BE_KACHCHABE , NOR_BE_HSS , NOR_BE_PORTOFORIGIN , NOR_INV_SVB_DT , NOR_INV_PAYMENT_TERMS , NOR_ITEM_NO , NOR_ITEM_BCD_RTA , NOR_ITEM_AND_AMT , NOR_HDF_HIGH_RISK_IMPORTERS , NOR_HDF_DORMANCY , NOR_HDF_TURNOVER , NOR_IMP_CITY , NOR_ITEM_BCD_AMT_FG , NOR_ITEM_AND_AMTS , NOR_INV_SUP_CNTRY , NOR_ITEM_CVD05_NOTN , NOR_ITEM_CVD05_NSNO , NOR_BE_IGM_NO , NOR_BE_IGM_YR , NOR_ITEM_SECTION , NOR_IDF_TOT_BE , NOR_BE_INW_DT , NOR_HDF_PORTS , NOR_HDF_CHA_AGE , NOR_HDF_CHA_ACP , NOR_HDF_TOT_AMENDMENT , NOR_HDF_TOT_BES , NOR_HDF_IMPORT_OFFENCE , NOR_CDF_VALUE_LOADING , NOR_IDF_TOT_DUTY , NOR_IDF_IEC_AGE , NOR_IDF_IEC_DORMANCY , NOR_IDF_IEC_LAST_USAGE , NOR_IDF_TOT_REDEMPTION_FINE , NOR_IDF_TOT_PORTS , NOR_IDF_TOT_PENALTY , NOR_IDF_TOT_CHAS , NOR_IDF_TOT_VALUE_LOADED , NOR_IDF_TOT_AMENDMENTS , NOR_IDF_ISCHA , NOR_IDF_TOT_TURNOVER , NOR_ITEM_DUTY_FG , NOR_ITEM_PARA_NO , NOR_ITEM_SVB_FLG , NOR_CUSTOMREVENUEDEFAULTER , NOR_CITY , NOR_BE_POS_GRP_CD , NOR_IMP_ADD1 , NOR_IMP_ADD2 , NOR_IMP_CLASS , NOR_IMP_PIN , NOR_BE_COO_GRP_CD , NOR_INV_MISC_RT , NOR_INV_VALUATION_MTHD , NOR_INV_INSURANCE , NOR_ITEM_ITEMC , NOR_ITEM_CVD_AMT_FG , NOR_ITEM_OTH1_NSNO , NOR_ITEM_AND_RTA , NOR_BE_CC , NOR_ITEM_SAFG_RTA , NOR_ITEM_SAFG_AMT , NOR_IDF_ISTRADER , NOR_BE_POS_NM , NOR_ITEM_UAV , NOR_IEC_STATUS , NOR_IEC_ISSUE_DT , NOR_CHA_ADD1 , NOR_ITEM_CVD_RTA , NOR_ITEM_JOINT_DESCRIPTION , NOR_BE_CNTRY_POS , NOR_ITEM_COO_GRP_CD , NOR_PAN , NOR_BE_PORT_NM , NOR_INV_SVB_NO , NOR_BE_COUNTRYOFCONSIGNMENT , NOR_BE_GOVT , NOR_BE_WBE_NO , NOR_BE_WCC , NOR_BE_TOT_ASS_VAL , NOR_BE_GREEN_CHANNEL , NOR_BE_WAREHOUSE_CD , NOR_BE_SEC_48 , NOR_BE_TOT_FINE , NOR_BE_TOT_PENAL , NOR_INV_ACTUAL_INV_NO , NOR_INV_SELL_ADD1 , NOR_INV_BRK_NM , NOR_INV_NO , NOR_INV_DT , NOR_INV_CONTRACT_NO , NOR_INV_CONTRACT_DT , NOR_INV_ASSD_FRGT , NOR_INV_ASSED_MISC_CHRG , NOR_INV_SVB_FLG , NOR_INV_TOT_DUTY_FOREGONE , NOR_INV_ASSD_TOT_DUTY , NOR_INV_VAL , NOR_INV_INS_RT , NOR_INV_ROYALTY , NOR_INV_ASSD_LANDING , NOR_ITEM_HLTH_NSNO , NOR_ITEM_ASSESS_VAL , NOR_ITEM_FINAL , NOR_ITEM_CRG , NOR_ITEM_CETH , NOR_ITEM_AD1_NOTN , NOR_ITEM_AD1_NSNO , NOR_ITEM_AD2_NSNO , NOR_ITEM_OTH_NOTN , NOR_ITEM_CESS_NOTN , NOR_ITEM_CESS_NSNO , NOR_ITEM_SAPT_NOTN , NOR_INV_TOT_DUTY , NOR_BE_BRANCH_SLNO , NOR_INV_SVB_LOAD_ON_DUTY , NOR_INV_DISC_AMT , NOR_ITEM_END_USE , NOR_BE_SEC_46 , NOR_BE_MODE_OF_SUBMISSION , NOR_ITEM_DUTY , NOR_ITEM_BCD_AMT , NOR_ITEM_UPI_CURCD , NOR_INV_ASSD_INV_VAL , NOR_INV_ASSD_ASSVAL , NOR_INV_AGENCY_COMM , NOR_INV_INS_VAL , NOR_BE_TYPE , NOR_ITEM_OTH1_NOTN , NOR_BE_MAWB_NO , NOR_BE_MAWB_DT , NOR_BE_HAWB_NO , NOR_BE_HAWB_DT , NOR_BE_TOTPKG , NOR_BE_GROSS_WT , NOR_BE_UQC , NOR_BE_PKG_CD , NOR_BE_TOT_FRGT , NOR_BE_TOT_INS , NOR_BE_A_TOT_INV_VAL , NOR_BE_A_TOT_FRGT , NOR_BE_A_TOT_INS , NOR_BE_A_TOT_MISC , NOR_BE_A_TOT_ASS_VAL , NOR_BE_A_TOT_DUTY , NOR_BE_A_TOT_DUTY_FOREGONE , NOR_BE_PAN , NOR_BE_CUS_SITE , NOR_BE_PORT_CD , NOR_ITEM_HLTH_NOTN , NOR_INV_SELL_NAME , NOR_ITEM_UQC , NOR_ITEM_MANUFACTURER_NM , NOR_ITEM_SCD_NOTN , NOR_ITEM_AND_NOTN , NOR_ITEM_ACC , NOR_ITEM_SAPT_NSNO , NOR_INV_PO_DT , NOR_ITEM_UPI , NOR_ITEM_RITC_CD , NOR_INV_LETTEROFCREDIT_NO , NOR_INV_SUP_ADD1 , NOR_INV_TERM , NOR_INV_MISC_CHRG , NOR_INV_RELATIONSHIP , NOR_INV_ASSD_INS_CHRG , NOR_INV_BROKER_COMM , NOR_ITEM_AD2_NOTN , NOR_ITEM_OTH_NSNO , NOR_ITEM_ECENVAT_NOTN , NOR_ITEM_TARF_NSNO , NOR_ITEM_SCD_NSNO , NOR_ITEM_RSP , NOR_BE_MODEOFTRANSPORT , NOR_ITEM_AND_NSNO , NOR_INV_HSS_LOAD_RTA , NOR_INV_NATURE , NOR_INV_PO_NO , NOR_INV_LETTEROFCREDIT_DT , NOR_RIM_NOTN_NO , NOR_RIM_NOTN_SLNO , NOR_RIM_SB_INV_SERNO , NOR_RIM_SB_ITEM_NO , NOR_RIM_SB_INV_NO , NOR_RIM_EXP_FRT , NOR_RIM_EXP_INS , NOR_RIM_BCD_AMT , NOR_RIM_CVD_AMT , NOR_RIM_SAD_AMT , NOR_RIM_BCD_AMT_FG , NOR_RIM_CVD_AMT_FG , NOR_ITEM_ECENVAT_NSNO , NOR_BE_IS_COMPLIANT , NOR_BE_FILED_THROUGH_SC , NOR_ITEM_JOINT_DESCRIPTION2 , NOR_IDF_IEC_ISPROPRIETOR , NOR_IDF_IEC_ISCOMPANY , NOR_IDF_IEC_ISHUF , NOR_IDF_IEC_ISPARTNERSHIP , NOR_IDF_IEC_ISASSOCIATECONCERN , NOR_IDF_IEC_ISTRUST , NOR_IDF_IEC_ISINDIVIDUALBODY , NOR_IDF_IEC_ISLOCALBODY , NOR_IDF_IEC_ISJURIDICALPERSON , NOR_IDF_IEC_ISGOVERNMENT , NOR_ITEM_SVB_NO , NOR_ITEM_SVB_DT , NOR_ITEM_SVB_LOAD , NOR_ITEM_SVB_CS , NOR_ITEM_SVB_LOAD_ON_DUTY , NOR_ITEM_SVB_SFLG , NOR_ITEM_SVB_DSFLG , NOR_INV_SVB_SFLG , NOR_INV_SVB_DSFLG , NOR_INV_SVB_ASS_VAL , NOR_INV_AUTH_ECO_OPERATOR , NOR_INV_AUTH_ECO_CNTRY , NOR_INV_AUTH_ECO_ROLE , NOR_BE_UCR_NO , NOR_INV_LICENCE_FEE , NOR_ITEM_POLICY_YR , NOR_ITEM_CVD05_AMT , NOR_ITEM_CVD05_RTA , NOR_ITEM_SHEDU_AMT , NOR_ITEM_SHEDU_RTA , NOR_ITEM_OTH1_AMT , NOR_ITEM_OTH1_RTA , NOR_ITEM_CVD_AMT , NOR_BE_IS_AEO_SUPPLIER , NOR_ITEM_MNFR_TYP , NOR_ITEM_MNFR_CODE , NOR_ITEM_MNFR_ADD1 , NOR_ITEM_MNFR_ADD2 , NOR_ITEM_MNFR_CITY , NOR_ITEM_MNFR_STATE , NOR_ITEM_MNFR_PIN , NOR_ITEM_MNFR_CNTRY , NOR_ITEM_SRC_CNTRY , NOR_ITEM_TRANSIT_CNTRY , NOR_ITEM_TYP_ANTIDUMP , NOR_RIM_INV_NO , NOR_RIM_ITEM_NO , NOR_RIM_SB_NO , NOR_RIM_SB_DT , NOR_BE_ISDOV_HIT , NOR_RIM_PORT , NOR_ITEM_ADD_NOTN , NOR_ITEM_ADD_NSNO , NOR_ITEM_SAFG_NOTN , NOR_ITEM_SAFG_NSNO , NOR_ITEM_SHEDU_NOTN , NOR_ITEM_SHEDU_NSNO , NOR_ITEM_JOINT_NOTN_NO , NOR_ITEM_JOINT_NOTN_SRNO , NOR_ITEM_INV_NO , NOR_BE_COUNT , NOR_IDF_TOT_DUTY_FOREGONE , NOR_BE_IS_ACP_CLIENT , NOR_CHA_NAME , NOR_CHA_PAN , NOR_BE_ISRISKY , NOR_BE_IS_AEO_CLIENT , NOR_ITEM_CESS_AMT , NOR_BE_ISUQC_HIT , NOR_BE_NOINV , NOR_BE_SUPPLIER_ID , NOR_BE_SUPPLIER_CNTRY , NOR_BE_NEW_SUPPLIER , NOR_BE_SUPPLIER_IEC_CNT , NOR_BE_SUPPLIER_IEC_CHA_CNT , NOR_BE_TOT_IGM_NO , NOR_BE_TOT_CTR_NO , NOR_BE_NOI , NOR_BE_ISPGAIECHIT , NOR_ITEM_CTH_SLNO , "
	 * + " NOR_BE_SENDER_ID ,nor_igm_be_id," +
	 * " NOR_IGM_MRK_NUM1 , NOR_IGM_MRK_NUM2 , NOR_IGM_MRK_NUM3 , NOR_IGM_GATEWAY_INWARD_DT , NOR_CTR_LCFC , NOR_CTR_SEAL_NO , NOR_IGM_NO , NOR_IGM_GROSS_WT , NOR_IGM_UQC , NOR_IGM_PKG_CD , NOR_IGM_DT , NOR_IGM_GATEWAY_IGM_NO , NOR_IGM_GATEWAY_IGM_YR , NOR_IGM_PORT_REPORT , NOR_IGM_MAWB_NO , NOR_IGM_MAWB_DT , NOR_IGM_HAWB_NO , NOR_IGM_HAWB_DT , NOR_IGM_TOT_PKG , NOR_CTR_NO , NOR_CTR_TRUCK_NO , NOR_IGM_YR , NOR_CTR_OOC_BY , NOR_CTR_OOC_NO , NOR_CTR_EXM_BY , HSS_IMP_ADD2 , HSS_IMP_CITY , HSS_IEC , HSS_IMP_NAME , HSS_IMP_ADD1 , RSP_NOTIFI_SNO , RSP_NOTIFI_NO , RSP , RSP_QTY_DESC , DEPB_CVD_NOTN , DEPB_CVD_NSNO , DEPB_BCD_NOTN , DEPB_BCD_NSNO , EOU_CEX_CD , IID_MASTER_NM , IID_IGM_NO , IID_IGM_YR , IID_VESSEL_NM , IID_INW_DT , IID_VOYAGE_NO , IID_PRIOR_IGM , IID_SMTP_DT , IID_SMTP_NO , MISC_CHARGE , OSHD_IS_RISKYDESCRIPTIONHIT , OSHD_BILL_SCORE_AVG , OSHD_IS_CC_TARGET_HIT , OSHD_IS_BRVHIT , OSHD_IS_DOVRULEHIT , OSHD_IS_FACILITATED , OSHD_IS_SB_TARGETHIT , OSHD_IS_NSB_TARGET_HIT , OSHD_IS_SB_RULE_HIT , OSHD_IS_NSB_RULE_HIT , OSHD_IS_RULE_HIT , OSHD_IS_TARGET_HIT , OSHD_IS_INTERVENED , OSHD_IS_INTERVENTIONHIT , OSHD_IS_ACPRANDOMHIT , OSHD_IS_SCORERANDOMHIT , CERT_NO , CERT_DT , CERT_TYPE , CERT_STATUS , CERT_REGN_NO , CERT_REGN_DT , CERT_CD , DUTY_IGST_DUTY_AMOUNT , DUTY_COMPENSATORY_AMOUNT , DUTY_IGST_DUTY_AMT_FORGONE , DUTY_COMP_AMT_FORGONE , DUTY_AD_FLG , DUTY_TYP , DUTY_EXEMPT_NOTN_TYPE , DUTY_UQC , DUTY_IGST_LEVY_NOTN , DUTY_IGST_LEVY_NSNO , DUTY_COMP_LEVY_NOTN , DUTY_COMP_LEVY_NSNO , DUTY_IGST_EXEMPTION_NOTN , DUTY_IGST_EXEMPTION_NSNO , DUTY_COMP_EXEMPTION_NOTN , DUTY_COMP_EXEMPTION_NSNO , DUTY_IGST_EXEMPTION_CUS_NOTN , DUTY_IGST_EXEMPTION_CUS_NSNO , DUTY_COMP_EXEMPTION_CUS_NOTN , DUTY_COMP_EXEMPTION_CUS_NSNO , DUTY_IGST_RATE , DUTY_COMPENSATORY_RATE , DUTY_CTH_LEVY_NSNO , BOND_CD , BOND_BANK_GURANTEE_RATE , BOND_ISCONTINUITY , BOND_STS , BOND_NO , BOND_AMT , BOND_BANK_GURANTEE_AMT , BOND_PORTOFREGISTRATION , EXHG_RATE , EXHG_BANK_NM , EXHG_CERTF_NO , EXHG_CERTF_DT , LIC_ISTRANSFERRED , LIC_DT , LIC_RELEASE_ADVISE_NO , LIC_POLICY_PARA_NO , LIC_DEBIT_UNIT_OF_MEASURE , LIC_DEBIT_QTY , LIC_REGN_NO , LIC_CD , LIC_NO , LIC_RELEASE_ADVISE_DT , LIC_REGN_DT , LIC_DEBIT_VAL , LIC_POLICY_YR , LIC_DEBIT_DUTY , ISIT_INFO_TYP , ISIT_INFO_QFR , ISIT_INFO_CD , ISIT_INFO_TXT , ISIT_INFO_UQC , ISIT_INFO_MSR , ISCO_CONST_NO , ISCO_CONST_ELMNT_NM , ISCO_CONST_ELMNT_CD , ISCO_CONST_PCT , ISCO_CONST_YPCT , ISCO_ACT_INGR , ISPR_PROD_BATCH_NO , ISPR_PROD_BATCH_QTY , ISPR_UQC , ISPR_MNF_DT , ISPR_EXP_DT , ISPR_BEST_BFR , ISPR_TOT_SHELF_LIFE , ISPR_RESID_SHELF_LIFE , ISPR_RESID_SHELF_LIFE_PCT , ITSC_CTRL_CD , ITSC_CTRL_LOC , ITSC_CTRL_START_DT , ITSC_CTRL_END_DT , ITSC_CTRL_RES_CD , ITSC_CTRL_RES_TXT , CTX_TYP , CTX_NO , ICS_GOODS_DESC , ICS_GROSS_WT , ICS_HAWB_NO , ICS_HAWB_DT , ICS_IGM_NO , ICS_IGM_YR , ICS_IMP_NM , ICS_LINE_NO , ICS_MAWB_NO , ICS_MAWB_DT , ICS_PORT_SHIP , ICS_TOT_PKG , ICS_UQC , ICS_VESSEL_CD , ICS_VOYAGE_NO , ICS_PORT_DEST , PEN_US , PEN_AMT , STS_AG , CSH_DUTY_AMT , AMD_CD , PERM_CD from nor_trans_data ,  nor_igms_cont_data,  imp_be_hss,  imp_be_rsp,  imp_be_depb,  imp_be_exportorientedunit,  imp_be_igm_icd,  imp_be_misc_charges,  imp_onsub_hitting_details,  imp_be_certificate, imp_be_duty,  imp_be_bond,  imp_be_exchange,  imp_be_licence, imp_be_item_sw_info_type, imp_be_item_sw_const,  imp_be_item_sw_prod, imp_be_item_sw_ctrl, imp_be_ctx,  imp_be_igm_cargo_sea, imp_be_penal, imp_be_status,  imp_be_cash, imp_be_amend, imp_be_permission, M_LOCATION WHERE  nor_trans_data.nor_be_id = nor_igms_cont_data.nor_igm_be_id( +)  AND nor_trans_data.nor_be_id = imp_be_hss.hss_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_rsp.rsp_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_depb.depb_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_exportorientedunit.eou_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_igm_icd.iid_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_misc_charges.misc_be_id(+)  AND nor_trans_data.nor_be_id = imp_onsub_hitting_details.oshd_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_certificate.cert_be_id(+) AND nor_trans_data.nor_be_id = imp_be_duty.duty_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_bond.bond_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_exchange.exhg_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_licence.lic_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_item_sw_info_type.isit_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_item_sw_const.isco_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_item_sw_prod.ispr_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_item_sw_ctrl.itsc_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_ctx.ctx_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_igm_cargo_sea.ics_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_penal.pen_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_status.sts_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_cash.csh_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_amend.amd_be_id(+)  AND nor_trans_data.nor_be_id = imp_be_permission.perm_be_id(+)  and NOR_TRANS_DATA.NOR_BE_LOC = M_LOCATION.LOCATION_ID(+) AND ROWNUM < 65000 ORDER BY nor_be_no"
	 * ;
	 */

}
