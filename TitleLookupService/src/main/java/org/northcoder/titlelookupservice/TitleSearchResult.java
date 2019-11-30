package org.northcoder.titlelookupservice;

import java.math.BigDecimal;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *
 */
public class TitleSearchResult implements Comparable<TitleSearchResult> {

    private final String titleID;
    private final String contentType;
    private final String primaryTitle;
    private final Integer year;
    private final String directors;
    private final String actors;
    private final String seriesTitle;
    private final Integer seasonNumber;
    private final Integer episodeNumber;
    // the lucene matching score:
    private final BigDecimal score;
    // to support index building:
    private String indexableTitle;
    // to support search results display:
    private String displayTitle;

    public TitleSearchResult(
            String titleID,
            String contentType,
            String primaryTitle,
            Integer year,
            String directors,
            String actors,
            String seriesTitle,
            Integer seasonNumber,
            Integer episodeNumber,
            BigDecimal score) {
        this.titleID = titleID;
        this.contentType = contentType;
        this.primaryTitle = primaryTitle;
        this.year = year;
        this.directors = directors;
        this.actors = actors;
        this.seriesTitle = seriesTitle;
        this.seasonNumber = seasonNumber;
        this.episodeNumber = episodeNumber;
        this.score = score;
        makeIndexableTitle();
        makeDisplayTitle();
    }

    private void makeIndexableTitle() {
        String tempPrimaryTitle = primaryTitle;
        if (tempPrimaryTitle != null && !tempPrimaryTitle.isBlank()) {
            // Here we perform some optimizations to help de-clutter
            // cases where data in a title does not help searches (for
            // example, many episode titles are of the form "Episode #nnn"
            // or "Episode dated nn/nn/nnnn").
            if (tempPrimaryTitle.toLowerCase().startsWith("episode #")
                    || tempPrimaryTitle.toLowerCase().startsWith("episode dated")) {
                tempPrimaryTitle = "";
            }
        }

        String tempSeriesTitle = seriesTitle;
        if (tempSeriesTitle != null && !tempSeriesTitle.isBlank()) {
            // for series, index the series name along with the episode name
            indexableTitle = String.format("%s %s", tempSeriesTitle, tempPrimaryTitle);
        } else {
            // just index the primary name (movie, short, etc):
            indexableTitle = tempPrimaryTitle;
        }
    }

    private void makeDisplayTitle() {
        StringBuilder sb = new StringBuilder();
        if (seriesTitle != null && !seriesTitle.isBlank()) {
            sb.append(seriesTitle);
            if (seasonNumber != null && seasonNumber > 0 && episodeNumber != null
                    && episodeNumber > 0) {
                sb.append(" S").append(String.format("%02d", seasonNumber))
                        .append(" E").append(String.format("%03d", episodeNumber));
            }
            sb.append(" - ");
        }
        sb.append(primaryTitle);
        displayTitle = sb.toString();
    }

    public String getTitleID() {
        return titleID;
    }

    public String getContentType() {
        return contentType;
    }

    @JsonIgnore
    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public Integer getYear() {
        if (year == 0) {
            return null;
        } else {
            return year;
        }
    }

    public BigDecimal getScore() {
        return score;
    }

    public String getDirectors() {
        return directors;
    }

    public String getActors() {
        return actors;
    }

    @JsonIgnore
    public String getSeriesTitle() {
        return seriesTitle;
    }

    @JsonIgnore
    public Integer getSeasonNumber() {
        return seasonNumber;
    }

    @JsonIgnore
    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    public String getDisplayTitle() {
        return displayTitle;
    }

    @JsonIgnore
    public String getIndexData() {
        return String.format("%s %s %s %s %s",
                indexableTitle, contentType, year, directors, actors);
    }

    @Override
    public int compareTo(TitleSearchResult other) {

        // match score:
        int i = getScore().compareTo(other.getScore());
        if (i != 0) {
            return -i; // highest match scores first!
        }

        // title:
        i = getDisplayTitle().compareTo(other.getDisplayTitle());
        if (i != 0) {
            return i;
        }

        // year:
        if (getYear() != null && other.getYear() != null) {
            i = getYear().compareTo(other.getYear());
            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

}
