/**
 * 
 */
package com.pj.lucene;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.BooleanClause;

/**
 * @author pradeep
 *
 */
public class LuceneDatabase {

	/**
	 * 
	 */
	public LuceneDatabase() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LuceneDatabase database = new LuceneDatabase();
//		database.dependentDropDownIndex();
		database.dependentDropDownSearch();
	}

	String index_location = "C:/Users/pradeep/Desktop/index/table";
	
	public String dependentDropDownSearch() {
try {
	
	IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index_location)));
	IndexSearcher searcher = new IndexSearcher(reader);
	BooleanQuery.Builder bq = new BooleanQuery.Builder();
	Query qf = new TermQuery(new Term("risk_indicator", "c"));
	bq.add(qf, BooleanClause.Occur.MUST);
	TopDocs results = searcher.search(bq.build(), 5 * 100);
	ScoreDoc[] hits = results.scoreDocs;
	System.out.println("F o  u n  d ~~" + hits.length);
	
	System.out.println(reader.maxDoc());
	
	for (int i=0; i<reader.maxDoc(); i++) {
	    
	    Document doc = reader.document(i);
	    String docId = doc.get("risk_indicator");
	    System.out.println("---"+docId);
	    // do something with docId here...
	}
	
} catch (Exception e) {
	// TODO: handle exception
	e.printStackTrace();
}
return null;
		
	}

	public String dependentDropDownIndex() {
		IndexWriter writer = null;
		try {
			String sql = "select * from dependentdd";
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn = null;
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/pjtest?autoReconnect=true&useSSL=false",
					"root", "R00t@123");
			PreparedStatement ps = conn.prepareStatement(sql);

			ResultSet rs = ps.executeQuery();

			Directory dir = FSDirectory.open(Paths.get(index_location));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			writer = new IndexWriter(dir, iwc);

			while (rs.next()) {
				Document document = new Document();
				document.add(new StringField("id", rs.getString("id"), Field.Store.YES));
				document.add(new StringField("risk_indicator", rs.getString("risk_indicator"), Field.Store.YES));
				document.add(new StringField("risk_factor", rs.getString("risk_factor"), Field.Store.YES));
				document.add(new StringField("column", rs.getString("column"), Field.Store.YES));
				writer.updateDocument(new Term("path", document.toString()), document);
			}
			writer.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}

	
}
