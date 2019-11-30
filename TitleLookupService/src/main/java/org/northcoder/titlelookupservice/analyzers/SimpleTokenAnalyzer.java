package org.northcoder.titlelookupservice.analyzers;

import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

/**
 *
 */
public class SimpleTokenAnalyzer extends StopwordAnalyzerBase {
    
    public SimpleTokenAnalyzer(){   
        super();
    }

    @Override
    protected StopwordAnalyzerBase.TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer();
        TokenStream tokenStream = source;
        tokenStream = new LowerCaseFilter(tokenStream);
        tokenStream = new StopFilter(tokenStream, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        tokenStream = new ASCIIFoldingFilter(tokenStream);
        return new StopwordAnalyzerBase.TokenStreamComponents(source, tokenStream);
    }
   
}
