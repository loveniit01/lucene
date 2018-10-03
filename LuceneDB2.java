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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LuceneDB2 {

	public LuceneDB2() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LuceneDB2 db2 = new LuceneDB2();
		// db2.dependentDropDownIndex();
		db2.dependentDropDownSearch();
	}

	String index_location = "C:/Users/pradeep/Desktop/index/table2";

	public String dependentDropDownIndex() {
		IndexWriter writer = null;
		try {
			String sql = "select film.film_id, film.title, film.description, film.release_year, "
					+ " ll.name,actor.first_name, actor.last_name,ll.name, DATE_FORMAT(film.last_update, '%d/%m/%Y')"
					+ " as date_  from film,film_actor,actor,inventory, `language` ll where "
					+ " film.film_id =film_actor.film_id and  film_actor.actor_id=actor.actor_id  and "
					+ " film.language_id = ll.language_id limit 1000000 ";
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn = null;
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sakila?autoReconnect=true&useSSL=false",
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
				document.add(new StringField("film_id", rs.getString("film_id"), Field.Store.YES));
				document.add(new StringField("title", rs.getString("title"), Field.Store.YES));
				document.add(new StringField("description", rs.getString("description"), Field.Store.YES));
				document.add(new StringField("release_year", rs.getString("release_year"), Field.Store.YES));

				document.add(new StringField("name", rs.getString("name"), Field.Store.YES));
				document.add(new StringField("first_name", rs.getString("first_name"), Field.Store.YES));
				document.add(new StringField("last_name", rs.getString("last_name"), Field.Store.YES));
				document.add(new StringField("date_", rs.getString("date_"), Field.Store.YES));

				writer.updateDocument(new Term("path", document.toString()), document);
			}
			writer.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}

	public String dependentDropDownSearch() {
		try {

			IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index_location)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer =  new StandardAnalyzer();
			BooleanQuery.Builder bq = new BooleanQuery.Builder();
			{
				Query qf = new TermQuery(new Term("film_id", "1"));//3481
				bq.add(qf, BooleanClause.Occur.MUST);
			}
			{
				Query qf = new TermQuery(new Term("title", "ACADEMY DINOSAUR"));//2322
				bq.add(qf, BooleanClause.Occur.MUST);
			}
			{
				Query qf = new TermQuery(new Term("first_name", "PENELOPE"));//12782
				bq.add(qf, BooleanClause.Occur.MUST);
			}
			{
				Query qf = new TermQuery(new Term("first_name", "GUINESS"));//14500
				bq.add(qf, BooleanClause.Occur.SHOULD);
			}
			{
				Query qf = new TermQuery(new Term("first_name", "DUSTIN"));//14500
				bq.add(qf, BooleanClause.Occur.MUST_NOT);
			}
			System.out.println("==="+bq.build());
			TopDocs results = searcher.search(bq.build(), 5 * 1000000);
			
			QueryParser parser =  new QueryParser("", analyzer);
			Query query =  parser.parse("(film_id:(1) AND title:(ACADEMY DINOSAUR))");
			System.out.println("==="+query);
//			TopDocs results = searcher.search(query, 5 * 1000000);
			ScoreDoc[] hits = results.scoreDocs;
			System.out.println("F o  u n  d ~~" + hits.length);

			System.out.println(hits.length);

			for (int i = 0; i < hits.length; ++i) {

				int docId = hits[i].doc;
				Document document = searcher.doc(docId);
//				System.out.print(document.get("film_id") + "|");
//				System.out.print(document.get("title") + "|");
//				System.out.print(document.get("description") + "|");
//				System.out.print(document.get("release_year") + "|");
//				System.out.print(document.get("name") + "|");
//				System.out.print(document.get("first_name") + "|");
//				System.out.print(document.get("last_name") + "|");
//				System.out.print(document.get("date_") + "|");

			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}
}