package org.northcoder.titlelookupservice;

import io.javalin.Javalin;
import io.javalin.http.Handler;
import static io.javalin.apibuilder.ApiBuilder.*; // before, get, post...
import java.math.BigDecimal;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

/**
 *
 */
public class TitleLookup {

    private static DAO dao;

    public static void main(String[] args) {

        Javalin app = Javalin.create(config -> {
            // NOT suitable for production:
            config.enableCorsForAllOrigins();
        }).start(7000);

        app.routes(() -> {
            before(SETUP);

            // valid values for :indexType are "simple" and "ngram":
            get("/build_index/:indexType", BUILD_INDEX);
            
            post("/titles", LOOKUP);
        });
    }

    private static final Handler SETUP = (ctx) -> {
        dao = new DAO();
    };

    private static final Handler LOOKUP = (ctx) -> {
        Map<String, BigDecimal> scores = new HashMap();
        String searchTerm = ctx.formParam("searchTerm");
        boolean fuzzySearch = (ctx.formParam("fuzzySearch") != null);
        if (searchTerm != null && !searchTerm.isBlank()) {
            scores = TitleSearcher.searchIndex(searchTerm, fuzzySearch);
        }
        if (!scores.isEmpty()) {
            ctx.json(dao.doSelect(scores));
        } else {
            ctx.json(Collections.EMPTY_LIST);
        }
    };
    
    private static final Handler BUILD_INDEX = (ctx) -> {
        IndexBuilder.buildIndex(ctx.pathParam("indexType"));
        ctx.json("Finished");
    };
}
