package webreduce.extraction.basic;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

import webreduce.data.Dataset;
import webreduce.data.HeaderPosition;
import webreduce.extraction.DocumentMetadata;
import webreduce.extraction.ExtractionAlgorithm;
import webreduce.extraction.StatsKeeper;
import webreduce.terms.LuceneNormalizer;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Uses simple heuristics to separate data from layout tables in a given
 * page.
 *
 */
public class BasicExtractionAlgorithm implements ExtractionAlgorithm {

	protected static final int TABLE_MIN_COLS = 2;
	protected static final double TABLE_MAX_SPARSENESS = 0.49;
	protected static final double TABLE_MAX_LINKS = 0.51;
	protected static final double MIN_ATTRIBUTE_SIZE_AVG = 4.0;
	protected static final double MAX_ATTRIBUTE_SIZE_AVG = 20.0;
	protected static final int TABLE_MIN_ROWS = 3;

	protected StatsKeeper stats;

	protected static final CharMatcher cleaner = CharMatcher.WHITESPACE;
	protected static final Joiner joiner = Joiner.on(" ").skipNulls();
	protected static final Whitelist whitelist = Whitelist.simpleText();

	protected boolean th_filter;
	protected boolean th_filter_strong;
	protected boolean th_filter_mark_only;
	protected boolean extract_terms;
	protected boolean extract_content;
	protected boolean extract_part_content;
	protected boolean save_reference;

	protected LuceneNormalizer termExtractor;

	public static enum TABLE_COUNTERS {
		TABLES_FOUND, TABLES_INSIDE_FORMS, NON_LEAF_TABLES, SMALL_TABLES, RELATIONS_FOUND, SPARSE_TABLE, LINK_TABLE, CALENDAR_FOUND, NON_REGULAR_TABLES, LANGDETECT_EXCEPTION, ENGLISH, NON_ENGLISH, TO_MANY_BADWORDS, SPANNING_TD, NO_HEADERS, MORE_THAN_ONE_HEADER, SHORT_ATTRIBUTE_NAMES, LONG_ATTRIBUTE_NAMES,
	}

	@Inject
	public BasicExtractionAlgorithm(StatsKeeper stats, @Named("extractTopNTerms") boolean th_extract_terms) {
		super();
		this.stats = stats;
		this.extract_terms = th_extract_terms;
		if (extract_terms)
			this.termExtractor = new LuceneNormalizer();
	}

	/* (non-Javadoc)
	 * @see webreduce.extraction.ExtractionAlgorithm#extract(org.jsoup.nodes.Document, webreduce.datastructures.DocumentMetadata)
	 */
	@Override
	public List<Dataset> extract(Document doc, DocumentMetadata metadata)
			throws IOException, InterruptedException {
		List<Dataset> result = new ArrayList<Dataset>();
		// Get page content and parse with JSoup
		String[] tags = null;
		int count = -1;
		// iterate tables tags; find relations
		main_loop: for (Element table : doc.getElementsByTag("table")) {
			// boolean isFiltered = false;
			stats.reportProgress();
			count += 1;
			stats.incCounter(TABLE_COUNTERS.TABLES_FOUND);

			// remove tables inside forms
			for (Element p : table.parents()) {
				if (p.tagName().equals("form")) {
					stats.incCounter(TABLE_COUNTERS.TABLES_INSIDE_FORMS);
					continue main_loop;
				}
			}

			// remove table with sub-tables
			Elements subTables = table.getElementsByTag("table");
			subTables.remove(table);
			if (subTables.size() > 0) {
				stats.incCounter(TABLE_COUNTERS.NON_LEAF_TABLES);
				continue;
			}

			// remove tables with less than N rows
			Elements trs = table.getElementsByTag("tr");
			if (trs.size() < TABLE_MIN_ROWS) {
				stats.incCounter(TABLE_COUNTERS.SMALL_TABLES);
				continue;
			}
			// remove tables with less than M columns
			int maxtdCount = 0;
			int[] tdCounts = new int[trs.size()];
			Multiset<Integer> colCounts = HashMultiset.create();
			for (int tr_idx = 0; tr_idx < trs.size(); tr_idx++) {
				Elements tds = trs.get(tr_idx).select("td, th");
				int td_size = tds.size();
				tdCounts[tr_idx] = td_size;
				colCounts.add(td_size);
				if (td_size > maxtdCount)
					maxtdCount = td_size;
			}
			// find most common number of columns throughout all rows
			colCounts = Multisets.copyHighestCountFirst(colCounts);
				int mostFrequentColCount = colCounts.entrySet().iterator().next()
						.getElement();
			if (mostFrequentColCount < TABLE_MIN_COLS) {
				stats.incCounter(TABLE_COUNTERS.SMALL_TABLES);
				continue;
			}
			// remove non-regular tables (there is a row with more columns
			// than the most common number)
			if (maxtdCount != mostFrequentColCount) {
				stats.incCounter(TABLE_COUNTERS.NON_REGULAR_TABLES);
				continue;
			}

			// eliminate tables with "rowspan" or "colspan" for now
			Elements colSpans = table.select("td[colspan], th[colspan]");
			Elements rowSpans = table.select("td[rowspan], th[rowspan]");
			if (colSpans.size() > 0 || rowSpans.size() > 0) {
				stats.incCounter(TABLE_COUNTERS.SPANNING_TD);
				continue;
			}

			// there should be header cells
			Boolean has_header = true;
			Elements headerCells = table.select("th");
			if (headerCells.size() == 0) {
				stats.incCounter(TABLE_COUNTERS.NO_HEADERS);
				has_header = false;
			}
			// stats.reportProgress();
			Optional<Dataset> r = doExtract(table, trs, mostFrequentColCount);
			if (!r.isPresent()) {
				continue;
			}
			Dataset er = r.get();

			// average length of first row
			String[] firstRow = er.getAttributes();
			double firstRowLengthSum = 0.0;
			for (int i = 0; i < firstRow.length; i++) {
				firstRowLengthSum += firstRow[i].length();
			}
			if ((firstRowLengthSum / firstRow.length) < MIN_ATTRIBUTE_SIZE_AVG) {
				stats.incCounter(TABLE_COUNTERS.SHORT_ATTRIBUTE_NAMES);
				continue;
			}
			if ((firstRowLengthSum / firstRow.length) > MAX_ATTRIBUTE_SIZE_AVG) {
				stats.incCounter(TABLE_COUNTERS.LONG_ATTRIBUTE_NAMES);
				continue;
			}

			er.tableNum = count;
			er.s3Link = metadata.getS3Link();
			er.recordOffset = metadata.getStart();
			er.recordEndOffset = metadata.getEnd();
			er.url = metadata.getUrl();

			if (tags == null && extract_terms) {
				String bodyContent = doc.select("body").text();
				Set<String> tagSet = termExtractor.topNTerms(bodyContent, 100);
				tags = tagSet.toArray(new String[] {});
				Arrays.sort(tags);
			}
			er.termSet = tags;
			er.hasHeader = has_header;
			Elements caption = table.select("caption");
			if (caption.size() == 1)
				er.setTitle(cleanCell(caption.get(0).text()));
			er.setPageTitle(doc.title());
			stats.incCounter(TABLE_COUNTERS.RELATIONS_FOUND);
			result.add(er);
		}
		return result;
	}

	protected Optional<Dataset> doExtract(Element table, Elements trs,
			int mostFrequentColCount) {
		// remove sparse tables (more than X% null cells)
		int tableSize = trs.size() * mostFrequentColCount;
		Optional<Dataset> r = asRelation(trs, mostFrequentColCount,
				((int) (TABLE_MAX_SPARSENESS * tableSize)),
				((int) (TABLE_MAX_LINKS * tableSize)));

		return r;
	}

	protected Optional<Dataset> asRelation(Elements input, int numCols,
			int nullLimit, int linkLimit) {
		int nullCounter = 0;
		int linkCounter = 0;
		int numRows = input.size();
		String[][] relation = new String[numCols][numRows];
		for (int r = 0; r < numRows; r++) {
			int c;
			Elements cells = input.get(r).select("td, th");
			int td_size = cells.size();
			for (c = 0; c < td_size; c++) {
				Element cell = cells.get(c);
				Elements links = cell.select("a");
				if (links.size() > 0)
					linkCounter += 1;
				String cellStr = cleanCell(cell.text());
				if (cellStr.length() == 0) {
					nullCounter += 1;
				}
				relation[c][r] = cellStr;
			}
			for (int c_fill = c; c_fill < numCols; c_fill++) {
				relation[c_fill][r] = ""; // just fill with empty string for now
			}
			if (nullCounter > nullLimit) {
				stats.incCounter(TABLE_COUNTERS.SPARSE_TABLE);
				return Optional.absent();
			}
			if (linkCounter > linkLimit) {
				stats.incCounter(TABLE_COUNTERS.LINK_TABLE);
				return Optional.absent();
			}
		}
		Dataset result = new Dataset();
		result.relation = relation;
		result.headerPosition = headerPosition(input);
		return Optional.of(result);
	}

	protected HeaderPosition headerPosition(Elements input) {
		// header in firstRow
		boolean fr = true;
		// header in firstCol
		boolean fc = true;

		Elements firstRow = input.get(0).children();
		int rowLength = firstRow.size();
		for (int i = 1; i < rowLength; i++) {
			if (!firstRow.get(i).tag().getName().equals("th")) {
				fr = false;
				break;
			}

		}

		int numRows = input.size();
		for (int i = 1; i < numRows; i++) {
			Elements tds = input.get(i).children();
			// no cells or first cell is not th
			if (tds.size() == 0 || !tds.get(0).tag().getName().equals("th")) {
				fc = false;
				break;
			}

		}

		HeaderPosition result = null;
		if (fr && fc)
			result = HeaderPosition.MIXED;
		else if (fr)
			result = HeaderPosition.FIRST_ROW;
		else if (fc)
			result = HeaderPosition.FIRST_COLUMN;
		else
			result = HeaderPosition.NONE;

		return result;
	}

	protected static String cleanCell(String cell) {
		cell = Jsoup.clean(cell, whitelist);
		cell = StringEscapeUtils.unescapeHtml4(cell);
		cell = cleaner.trimAndCollapseFrom(cell, ' ');
		return cell;
	}

	/* (non-Javadoc)
	 * @see webreduce.extraction.ExtractionAlgorithm#getStatsKeeper()
	 */
	@Override
	public StatsKeeper getStatsKeeper() {
		return stats;
	}

	private static int NUM_RUNS = 50;

	public static void main(String[] args) throws MalformedURLException,
			IOException, InterruptedException {
		ExtractionAlgorithm ea = new BasicExtractionAlgorithm(
				new StatsKeeper.HashMapStats(), true);

		for (String url : new String[] {
				"http://en.wikipedia.org/wiki/List_of_countries_by_population",
				"http://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)",
				"http://en.wikipedia.org/wiki/BRIC" }) {
			InputStream in = new URL(url).openStream();

			long startTime = System.nanoTime();
			for (int i = 0; i < NUM_RUNS; i++) {
				Document doc = Jsoup.parse(in, null, "");
				DocumentMetadata dm = new DocumentMetadata(0, 0, "", "");
				List<Dataset> result = ea.extract(doc, dm);
				for (Dataset er : result) {
					 System.out.println(Arrays.deepToString(er.relation));
					 System.out.println(er.getHeaderPosition());
				}
			}
			long endTime = System.nanoTime();
			System.out.println("Time: "
					+ (((float) (endTime - startTime)) / NUM_RUNS) / 1000000);
			// System.out.println(ea.stats.statsAsMap().toString());
		}
	}

}
