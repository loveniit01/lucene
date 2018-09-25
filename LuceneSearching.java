package com.eseal.searching;

import java.io.BufferedReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.springframework.stereotype.Service;

import com.eseal.utility.ExportData;

@Service
public class LuceneSearching {

	public LuceneSearching() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LuceneSearching searching = new LuceneSearching();
		searching.dataSearch();
	}

	String index_location = "C:/Users/CBEC PROJECT/Documents/LuceneIndexData";

	public List<ExportData> dataSearch() {
		//
		List<ExportData> dataExport = null;
		String startDate = dateFormatChange("2017/06/21");
		String endDate = dateFormatChange("2017/07/12");
		try {
			Date start_time = new Date();
			IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index_location)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			BufferedReader in = null;

			// QueryParser parser = new QueryParser("nor_be_id", analyzer);
			// Query query = parser.parse(parser.escape("010031001084210617"));

			// QueryParser parser = new QueryParser("NOR_BE_CREATION_DATE", analyzer);
			// Query query = parser.parse(parser.escape("20170621"));

			/**
			 * ============multi field search query=========================
			 */

			BooleanQuery.Builder bq = new BooleanQuery.Builder();

			Query qf = new TermQuery(new Term("nor_be_id", "010031001084210617"));

			String qq = "[" + startDate + " TO " + endDate + "]";

			QueryParser queryParser = new QueryParser("NOR_BE_CREATION_DATE", analyzer);

			Query query = queryParser.parse(qq);

			// Query ql = new TermQuery(new Term("NOR_BE_CREATION_DATE", qq));

			bq.add(qf, BooleanClause.Occur.MUST);

			bq.add(query, BooleanClause.Occur.MUST);

			System.out.println("==>>>  " + bq.build());

			TopDocs results = searcher.search(bq.build(), 5 * 100);

			/**
			 * //
			 * ----------------------------end-------------------------------------------------------
			 */

			// TopDocs results = searcher.search(query, 5 * 1000000);
			ScoreDoc[] hits = results.scoreDocs;
			System.out.println("F o  u n  d ~~" + hits.length);
			Date end_time = new Date();
			System.out.println("Time: " + (end_time.getTime() - start_time.getTime()) + "ms");

			int numTotalHits = Math.toIntExact(results.totalHits);
			System.out.println(numTotalHits + " total matching documents");

			int start = 0;
			int end = Math.min(numTotalHits, 100);

			dataExport = new ArrayList<>();

			for (int i = 0; i < hits.length; ++i) {

				int docId = hits[i].doc;
				// System.out.println("--doc id = = "+docId);
				Document document = searcher.doc(docId);

				// System.out.print(document.get("nor_be_id")+"|");
				// System.out.print(document.get("nor_be_no")+"|");
				// System.out.print(document.get("nor_be_dt")+"|");
				// System.out.print(document.get("nor_be_loc")+"|");
				// System.out.print(document.get("nor_be_countryoforigin")+"|");
				// System.out.print(document.get("nor_be_pos")+"|");
				// System.out.print(document.get("nor_be_apprasing_group")+"|");
				// System.out.print(document.get("nor_be_cha_no")+"|");
				// System.out.print(document.get("nor_be_iec")+"|");
				// System.out.print(document.get("nor_be_imp_name")+"|");
				// System.out.print(document.get("iscurrent")+"|");
				//
				// System.out.print(document.get("unlocode")+"|");
				//// System.out.println(document.get("nor_item_cth"));
				// System.out.print(document.get("nor_item_desc1")+"|");
				// System.out.print(document.get("nor_item_desc2")+"|");
				// System.out.print(document.get("nor_item_model")+"|");
				// System.out.print(document.get("nor_item_brand")+"|");
				// System.out.print(document.get("nor_item_bcd_notn")+"|");
				// System.out.print(document.get("nor_item_bcd_nsno")+"|");
				//
				// System.out.print(document.get("nor_item_cvd_notn")+"|");
				// System.out.print(document.get("nor_item_cvd_nsno")+"|");
				// System.out.print(document.get("NOR_BE_CREATION_DATE")+"|");
				// System.out.println();

				ExportData ed = new ExportData();
				ed.setNOR_BE_ID(document.get("nor_be_id") );
				ed.setNOR_BE_NO(document.get("nor_be_no") );
				ed.setNOR_BE_DT(document.get("nor_be_dt") );
				ed.setNOR_BE_LOC(document.get("nor_be_loc") );
				ed.setNOR_BE_COUNTRYOFORIGIN(document.get("nor_be_countryoforigin") );
				ed.setNOR_BE_POS(document.get("nor_be_pos") );
				ed.setNOR_BE_APPRASING_GROUP(document.get("nor_be_apprasing_group") );
				ed.setNOR_BE_CHA_NO(document.get("nor_be_cha_no") );
				ed.setNOR_BE_IEC(document.get("nor_be_iec") );
				ed.setNOR_BE_IMP_NAME(document.get("nor_be_imp_name") );
				ed.setISCURRENT(document.get("iscurrent") );
				ed.setUNLOCODE(document.get("unlocode") );
				//// System.out.println(document.get("nor_item_cth"));
				ed.setNOR_ITEM_DESC1(document.get("nor_item_desc1") );
				ed.setNOR_ITEM_DESC2(document.get("nor_item_desc2") );
				ed.setNOR_ITEM_MODEL(document.get("nor_item_model") );
				ed.setNOR_ITEM_BRAND(document.get("nor_item_brand") );
				ed.setNOR_ITEM_BCD_NOTN(document.get("nor_item_bcd_notn") );
				ed.setNOR_ITEM_BCD_NSNO(document.get("nor_item_bcd_nsno") );
				//
				ed.setNOR_ITEM_CVD_NOTN(document.get("nor_item_cvd_notn") );
				ed.setNOR_ITEM_CVD_NSNO(document.get("nor_item_cvd_nsno") );
				dataExport.add(ed);
			}

			// --------------------------------------------------------------------------------------------

		} catch (Exception e) {
			// TODO: handle exception
		} finally {

		}
		return dataExport;
	}

	public String dateFormatChange(String date) {
		date = date.replaceAll("/", "");
		return date;
	}

}
