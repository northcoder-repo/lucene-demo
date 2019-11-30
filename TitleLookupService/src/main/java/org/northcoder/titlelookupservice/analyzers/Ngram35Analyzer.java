package org.northcoder.titlelookupservice.analyzers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 *
 */
public class Ngram35Analyzer extends Analyzer {

    public Ngram35Analyzer() {
        super();
    }

    @Override
    protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream tokenStream = new LowerCaseFilter(source);
        tokenStream = new ASCIIFoldingFilter(tokenStream);
        tokenStream = new NGramTokenFilter(tokenStream, 3, 5, true);
        return new Analyzer.TokenStreamComponents(source, tokenStream);
    }

}
