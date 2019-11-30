package org.northcoder.titlelookupservice;

import org.northcoder.titlelookupservice.analyzers.Ngram35Analyzer;
import org.northcoder.titlelookupservice.analyzers.SimpleTokenAnalyzer;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.logging.log4j.LogManager;

/**
 *
 */
public class TitleSearcher {

    private static final String BASE_PATH = "/your/path/here/";
    
    private static final Path NGRAM_INDEX_PATH = Paths.get(BASE_PATH + "title_data_Ngram35Analyzer");
    private static final Analyzer NGRAM_ANALYZER = new Ngram35Analyzer();

    private static final Path SIMPLE_INDEX_PATH = Paths.get(BASE_PATH + "title_data_SimpleTokenAnalyzer");
    private static final Analyzer SIMPLE_ANALYZER = new SimpleTokenAnalyzer();

    private static Directory directory = null;
    public static org.apache.logging.log4j.Logger LOGGER = LogManager.getRootLogger();

    public static Map<String, BigDecimal> searchIndex(String searchTerm, boolean fuzzySearch)
            throws IOException, ParseException {
        Path indexPath;
        Analyzer analyzer;
        Query query;
        Map<String, BigDecimal> titleScores = new HashMap();
        if (fuzzySearch) {
            indexPath = NGRAM_INDEX_PATH;
            analyzer = NGRAM_ANALYZER;
            query = getNgramQuery(searchTerm, analyzer);
        } else {
            indexPath = SIMPLE_INDEX_PATH;
            analyzer = SIMPLE_ANALYZER;
            query = getStandardQuery(searchTerm);
        }

        directory = new MMapDirectory(indexPath);
        try (DirectoryReader dirReader = DirectoryReader.open(directory)) {
            IndexSearcher indexSearcher = new IndexSearcher(dirReader);

            LOGGER.info(String.format("Lucene search:\n  Analyzer: [%s]\n  Term    : [%s]\n  Query   : [%s]",
                    analyzer.getClass().getSimpleName(), searchTerm, query.toString()));
            ScoreDoc[] hits = indexSearcher.search(query, 100).scoreDocs;
            for (ScoreDoc hit : hits) {
                BigDecimal score = new BigDecimal(String.valueOf(hit.score))
                        .setScale(4, RoundingMode.HALF_EVEN);
                titleScores.put(indexSearcher.doc(hit.doc).get("title_id"), score);
            }
        }
        return titleScores;
    }

    private static Query getNgramQuery(String searchTerm, Analyzer analyzer)
            throws IOException, ParseException {
        QueryParser parser = new QueryParser("title_data", analyzer);
        return parser.parse(QueryParser.escape(searchTerm));
    }

    private static Query getStandardQuery(String searchTerm) {
        String[] searchTerms = searchTerm.split(" ");
        BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        for (String term : searchTerms) {
            PrefixQuery pQuery = new PrefixQuery(new Term("title_data", QueryParser.escape(term)));
            BooleanClause bc = new BooleanClause(pQuery, BooleanClause.Occur.MUST);
            bqBuilder.add(bc);
        }
        return bqBuilder.build();
    }

}
