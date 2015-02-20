package webreduce.terms;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import webreduce.cleaning.CustomAnalyzer;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;

/**
 * Used for extracting normalized document terms.
 */
public class LuceneNormalizer {
	protected int minTermLength;
	protected Set<String> stopWords;
	Analyzer analyzer;

	public LuceneNormalizer() {
		this.minTermLength = 0;
		analyzer = new CustomAnalyzer();
	}


	public Set<String> setOfTerms(String phrase) throws IOException {
		Set<String> terms = new HashSet<String>();

		TokenStream ts;
		CharTermAttribute cta;

		if (phrase.length() == 0)
			return terms;
		ts = analyzer.tokenStream(null, new StringReader(phrase));
		cta = ts.addAttribute(CharTermAttribute.class);
		ts.reset();
		while (ts.incrementToken())
			terms.add(cta.toString());
		ts.end();
		ts.close();

		return terms;
	}

	public Set<String> topNTerms(String phrase, int topN) throws IOException {
		Set<String> result = new HashSet<String>();
		Multiset<String> terms = HashMultiset.create();
		// find most common number of columns throughout all rows
		TokenStream ts;
		CharTermAttribute cta;

		if (phrase.length() == 0)
			return result;
		ts = analyzer.tokenStream(null, new StringReader(phrase));
		cta = ts.addAttribute(CharTermAttribute.class);
		ts.reset();
		while (ts.incrementToken())
			terms.add(cta.toString());
		ts.end();
		ts.close();

		terms = Multisets.copyHighestCountFirst(terms);
		Iterator<String> iter = terms.elementSet().iterator();
		while(iter.hasNext()) {
			String s = iter.next();
			if (topN==0)
				break;
			result.add(s);
			topN--;
		}
		return result;
	}
	
}
