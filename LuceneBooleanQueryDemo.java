package com.pj.lucene;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class LuceneBooleanQueryDemo {

	public static final String FILES_TO_INDEX_DIRECTORY = "C:\\Users\\pradeep\\Desktop\\index";
	public static final String INDEX_DIRECTORY = "C:\\Users\\pradeep\\Desktop\\search";

	public static final String FIELD_PATH = "path";
	public static final String FIELD_CONTENTS = "contents";

	public static void main(String[] args) throws Exception {

//		createIndex();

		// Directory directory = FSDirectory.getDirectory();
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(FILES_TO_INDEX_DIRECTORY)));
		IndexSearcher indexSearcher = new IndexSearcher(reader);

		Query query1 = new TermQuery(new Term(FIELD_CONTENTS, "mushrooms"));
		Query query2 = new TermQuery(new Term(FIELD_CONTENTS, "steak"));

		BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
		booleanQuery.add(query1, BooleanClause.Occur.MUST);
		booleanQuery.add(query2, BooleanClause.Occur.MUST);
		displayQuery(booleanQuery);
		Query query = booleanQuery.build();

		TopDocs results = indexSearcher.search(query, 500);
		displayHits(results, indexSearcher);

		booleanQuery = new BooleanQuery.Builder();
		booleanQuery.add(query1, BooleanClause.Occur.MUST);
		booleanQuery.add(query2, BooleanClause.Occur.MUST_NOT);
		displayQuery(booleanQuery);
		displayHits(results, indexSearcher);

		booleanQuery = new BooleanQuery.Builder();
		booleanQuery.add(query1, BooleanClause.Occur.MUST);
		booleanQuery.add(query2, BooleanClause.Occur.SHOULD);
		displayQuery(booleanQuery);
		displayHits(results, indexSearcher);

		searchIndexWithQueryParser("+contents:mushrooms +contents:steak");
		searchIndexWithQueryParser("mushrooms steak");
		searchIndexWithQueryParser("bacon eggs");
		searchIndexWithQueryParser("(mushrooms steak) OR (bacon eggs)");
		searchIndexWithQueryParser("(mushrooms steak) AND (bacon eggs)");
		searchIndexWithQueryParser("(mush*ms OR raspberries) AND (ste?k)");

	}

	public static void createIndex() throws CorruptIndexException, LockObtainFailedException, IOException {

		Directory dir = FSDirectory.open(Paths.get(FILES_TO_INDEX_DIRECTORY));
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

		IndexWriter writer = new IndexWriter(dir, iwc);
		final Path docDir = Paths.get(FILES_TO_INDEX_DIRECTORY);
		indexDocs(writer, docDir);

		writer.close();
		System.out.println("indexing complete");

	}

	private static void indexDocs(IndexWriter writer, Path path) throws IOException {
		// TODO Auto-generated method stub

		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
					} catch (Exception e) {
						// TODO: handle exception
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}

	}

	private static void indexDoc(IndexWriter writer, Path file, long lastModified) {
		// TODO Auto-generated method stub
		System.out.println("==>>" + file.toString() + "== last modifier==" + lastModified);
		try (InputStream stream = Files.newInputStream(file)) {
			Document doc = new Document();
			Scanner scanner = new Scanner(stream);
			BufferedReader aa = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			String pj="";
			while(scanner.hasNext())
			{
				pj= pj+scanner.nextLine()+"\n";
			}
			System.out.println("======= >>>>>>"+pj);
//			
//			System.out.println("~~~" + aa.toString());
			Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			doc.add(pathField);
			doc.add(new LongPoint("modified", lastModified));
			doc.add(new TextField("contents", pj,Field.Store.YES ));
			System.out.println("====D o c = = = = "+doc.get("contents"));
			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				System.out.println("adding " + file);
				writer.addDocument(doc);

			} else {
				System.out.println("updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public static void searchIndexWithQueryParser(String searchString)
			throws IOException, ParserException, ParseException {
		System.out.println("Searching for '" + searchString + "' using QueryParser");
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(FILES_TO_INDEX_DIRECTORY)));
		IndexSearcher indexSearcher = new IndexSearcher(reader);

		org.apache.lucene.queryparser.classic.QueryParser queryParser = new org.apache.lucene.queryparser.classic.QueryParser(
				FIELD_CONTENTS, new StandardAnalyzer());
		Query query = queryParser.parse(searchString);
		System.out.println("Type of query: " + query.getClass().getSimpleName());
		displayQuery(query);
		TopDocs results = indexSearcher.search(query, 500);
		displayHits(results, indexSearcher);
	}

	public static void displayHits(TopDocs results, IndexSearcher searcher) throws CorruptIndexException, IOException {
		ScoreDoc[] hits = results.scoreDocs;
		int numTotalHits = Math.toIntExact(results.totalHits);
		System.out.println(numTotalHits + " total matching documents");
		int start = 0;
		int end = Math.min(numTotalHits, 500);
		// Iterator<Hit> it = hits.iterator();

		for (int i = start; i < end; i++) {
			if (true) {
				System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);
//				continue;
			}

			Document doc = searcher.doc(hits[i].doc);
			String path = doc.get("path");
//			System.out.println("====p a t h==="+path);
			if (path != null) {
				System.out.println((i + 1) + ". " + path);
				String title = doc.get("contents");
//				System.out.println("=== t i t l e ==="+title);
				if (title != null) {
					System.out.println("   Title: " + doc.get("contents"));
				}
			}
		}
		System.out.println();
	}

	public static void displayQuery(Builder booleanQuery) {
		Query query = booleanQuery.build();
		System.out.println("Query: " + query.toString());
	}

	public static void displayQuery(Query booleanQuery) {
		System.out.println("Query: " + booleanQuery.toString());
	}
}
