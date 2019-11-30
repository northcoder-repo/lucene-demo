package org.northcoder.titlelookupservice;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Collections;

/**
 *
 */
public class DAO {

    private static Connection conn;

    private static final String RAW_QUERY_SQL = String.join(" ",
            "select ", 
            "  t.title_id,", 
            "  t.primary_title,",
            "  srs.primary_title as series_title,",
            "  te.season_number,", 
            "  te.episode_number,",
            "  ct.content_type_name,", 
            "  t.start_year as year,",
            "  group_concat(distinct tal_a.talent_name",
            "    order by tp_a.ord separator ', ') as directors,",
            "  group_concat(distinct tal_b.talent_name",
            "    order by tp_b.ord separator ', ') as actors",
            "from imdb.title t inner join imdb.content_type ct",
            "on t.content_type_id = ct.content_type_id",
            "and t.is_adult = 0",
            "left outer join imdb.title_principal tp",
            "on t.title_id = tp.title_id",
            "and tp.category_id = 2",
            "left outer join imdb.talent tal",
            "on tp.talent_id = tal.talent_id",
            "left outer join imdb.title_principal tp_a",
            "on t.title_id = tp_a.title_id",
            "and tp_a.category_id = 2",
            "left outer join imdb.talent tal_a",
            "on tp_a.talent_id = tal_a.talent_id",
            "left outer join imdb.title_principal tp_b",
            "on t.title_id = tp_b.title_id",
            "and tp_b.category_id in (1, 7, 8)",
            "left outer join imdb.talent tal_b",
            "on tp_b.talent_id = tal_b.talent_id",
            "left outer join imdb.title_episode te",
            "on t.title_id = te.title_id",
            "left outer join imdb.title srs",
            "on te.parent_title_id = srs.title_id",
            "where t.title_id in (%s)",
            "group by t.title_id");

    public static final String INDEXER_TITLE_ID_SQL = String.join(" ",
            "select t.title_id from imdb.title t where t.is_adult = 0");

    public DAO() throws SQLException, ClassNotFoundException {
        conn = DbConn.INST.getDbConn();
    }

    public List<TitleSearchResult> doSelect(Map<String, BigDecimal> scores) {

        StringBuilder sb = new StringBuilder();
        scores.keySet().forEach((_item) -> {
            sb.append("?, ");
        });
        String cookedQuerySql = String.format(RAW_QUERY_SQL, sb.substring(0, sb.length() - 2));

        PreparedStatement pstmt = null;
        List<TitleSearchResult> results = new ArrayList();
        try {
            if (conn != null) {
                pstmt = conn.prepareStatement(cookedQuerySql);

                int j = 0;
                for (String titleID : scores.keySet()) {
                    j++;
                    pstmt.setString(j, titleID);
                }

                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    String titleID = rs.getString("title_id");
                    String title = rs.getString("primary_title");
                    String contentType = rs.getString("content_type_name");
                    Integer year = rs.getInt("year");
                    String directors = rs.getString("directors");
                    String actors = rs.getString("actors");
                    String seriesTitle = rs.getString("series_title");
                    Integer seasonNumber = rs.getInt("season_number");
                    Integer episodeNumber = rs.getInt("episode_number");
                    results.add(new TitleSearchResult(titleID, contentType, title,
                            year, directors, actors, seriesTitle, seasonNumber,
                            episodeNumber, scores.get(titleID)));
                }
                pstmt.close();
            }
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
        Collections.sort(results);
        return results;
    }

}
