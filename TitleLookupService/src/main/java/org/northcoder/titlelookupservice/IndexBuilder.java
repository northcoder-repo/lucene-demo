package org.northcoder.titlelookupservice;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.time.ZonedDateTime;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.northcoder.titlelookupservice.analyzers.Ngram35Analyzer;
import org.northcoder.titlelookupservice.analyzers.SimpleTokenAnalyzer;

/**
 *
 */
public class IndexBuilder {

    private static final Path ROOT_PATH = Paths.get("/your/path/here/");

    private static final Path NGRAM_INDEX_PATH = Paths.get(ROOT_PATH + "title_data_Ngram35Analyzer");
    private static final Analyzer NGRAM_ANALYZER = new Ngram35Analyzer();

    private static final Path SIMPLE_INDEX_PATH = Paths.get(ROOT_PATH + "title_data_SimpleTokenAnalyzer");
    private static final Analyzer SIMPLE_ANALYZER = new SimpleTokenAnalyzer();

    private static Directory directory = null;
    public static org.apache.logging.log4j.Logger LOGGER = LogManager.getRootLogger();

    public static void buildIndex(String indexType) throws IOException, SQLException, ClassNotFoundException {

        Path indexPath;
        Analyzer analyzer;

        switch (indexType) {
            case "simple":
                indexPath = SIMPLE_INDEX_PATH;
                analyzer = SIMPLE_ANALYZER;
                break;
            case "ngram":
                indexPath = NGRAM_INDEX_PATH;
                analyzer = NGRAM_ANALYZER;
                break;
            default:
                return;
        }

        // We're using MMapDirectory here, not FSDirectory (which is what the Lucene demo code
        // uses). See https://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html
        // Under the covers it uses FSDirectory for writing, anyway. See the Javadoc here:
        // https://lucene.apache.org/core/8_3_0/core/index.html
        directory = new MMapDirectory(indexPath);

        if (!DirectoryReader.indexExists(directory)) {
            LOGGER.info(String.format("Starting - %s index will be built...", indexType));

            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE);

            Connection conn = DbConn.INST.getDbConn();
            PreparedStatement pstmt = null;
            ResultSet rs;
            List<String> titleIDs = new ArrayList();
            Map<String, BigDecimal> titleIdBatch = new HashMap();
            int batchSize = 1000;
            DAO dao = new DAO();
            try {
                pstmt = conn.prepareStatement(DAO.INDEXER_TITLE_ID_SQL,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
                pstmt.setFetchSize(Integer.MIN_VALUE);
                rs = pstmt.executeQuery();
                int j = 0;
                while (rs.next()) {
                    j++;
                    titleIDs.add(rs.getString("title_id"));
                    if (j % 100000.0 == 0) {
                        LOGGER.info(String.format("Collected %,d title IDs", j));
                    }
                }
                LOGGER.info(String.format("Collected %,d title IDs", titleIDs.size()));
                rs.close();
                pstmt.close();
                int i = 0;
                ZonedDateTime start = ZonedDateTime.now();
                try (final IndexWriter indexWriter = new IndexWriter(directory, iwc)) {
                    for (String titleID : titleIDs) {
                        i++;
                        titleIdBatch.put(titleID, BigDecimal.ZERO);
                        if (titleIdBatch.size() == batchSize) {
                            processTitleBatch(dao, titleIdBatch, indexWriter, i, start,
                                    titleIDs.size());
                            titleIdBatch.clear();
                        }
                    }
                    if (!titleIdBatch.isEmpty()) {
                        // the final partial batch:
                        processTitleBatch(dao, titleIdBatch, indexWriter, i, start,
                                titleIDs.size());
                    }
                }
                long elapsedSeconds = getElapsedSeconds(start);
                LOGGER.info(String.format("Indexed %,d documents in %d minutes",
                        titleIDs.size(), Math.round(elapsedSeconds / 60.0)));
                LOGGER.info(String.format("at a rate of approx. %,d documents per second.",
                        Math.round((titleIDs.size() * 1.0) / elapsedSeconds)));
                LOGGER.info("Finished.");
            } catch (SQLException e) {
                System.err.print(e);
            } finally {
                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (SQLException e) {
                    // no action
                }
            }
        }
    }

    private static void processTitleBatch(DAO dao, Map<String, BigDecimal> titleIdBatch,
            IndexWriter indexWriter, int i, ZonedDateTime start,
            int total) throws IOException {
        List<TitleSearchResult> results = dao.doSelect(titleIdBatch);
        for (TitleSearchResult result : results) {
            writeLuceneDocToIndex(result, indexWriter);
            if (i++ % 10000 == 0) {
                int minutesRemaining = getApproxMinutesRemaining(start, i, total);
                LOGGER.info(String.format("Indexed %,d documents", i - 1));
                LOGGER.info(String.format("Approx %,d minutes remaining", minutesRemaining));
            }
        }
    }

    private static int getApproxMinutesRemaining(ZonedDateTime start, int docsSoFar, int totalDocs) {
        long elapsedSeconds = getElapsedSeconds(start);
        return Math.round((((totalDocs * elapsedSeconds) / docsSoFar) - elapsedSeconds) / 60);
    }

    private static long getElapsedSeconds(ZonedDateTime start) {
        ZonedDateTime now = ZonedDateTime.now();
        Duration duration = Duration.between(start, now);
        return duration.toSeconds();
    }

    private static void writeLuceneDocToIndex(TitleSearchResult result, IndexWriter indexWriter) throws IOException {
        Document doc = new Document();
        doc.add(new Field("title_id", result.getTitleID(), TextField.TYPE_STORED));
        doc.add(new Field("title_data", result.getIndexData(), TextField.TYPE_NOT_STORED));
        // These are all ADDs - we don't use indexWriter.updateDocument() here:
        indexWriter.addDocument(doc);
    }

}
