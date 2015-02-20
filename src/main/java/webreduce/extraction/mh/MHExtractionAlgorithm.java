package webreduce.extraction.mh;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import webreduce.data.Dataset;
import webreduce.data.TableType;
import webreduce.extraction.DocumentMetadata;
import webreduce.extraction.ExtractionAlgorithm;
import webreduce.extraction.StatsKeeper;
import webreduce.extraction.basic.BasicExtractionAlgorithm;
import webreduce.extraction.mh.tools.ClassificationResult;
import webreduce.extraction.mh.tools.TableConvert;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Uses simple heuristics to separate data from layout tables in a given
 * page.
 *
 */
public class MHExtractionAlgorithm extends BasicExtractionAlgorithm {

	protected static final int TABLE_MIN_ROWS = 2;
	protected static final int TABLE_MIN_COLS = 2;

	public static enum TABLE_COUNTERS {
		TABLES_FOUND, TABLES_INSIDE_FORMS, NON_LEAF_TABLES, SMALL_OR_IRREGULAR_TABLES, RELATIONS_FOUND, NO_HEADERS
	}
	private final TableClassification tableClassifier;
	private final TableConvert tableConverter;

	@Inject
	public MHExtractionAlgorithm(StatsKeeper stats, @Named("extractTopNTerms") boolean th_extract_terms, TableClassification tableClassifier) {
		super(stats, th_extract_terms);
		this.tableClassifier = tableClassifier;
		this.tableConverter = new TableConvert(TABLE_MIN_ROWS, TABLE_MIN_COLS);
	}

	public List<Dataset> extract(Document doc, DocumentMetadata metadata) throws IOException,
			InterruptedException {
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

			// there should be header cells
			Boolean has_header = true;
			Elements headerCells = table.select("th");
			if (headerCells.size() == 0) {
				stats.incCounter(TABLE_COUNTERS.NO_HEADERS);
				has_header = false;
			}
			// stats.reportProgress();
			// MHs table extraction and classification
			Optional<Element[][]> convertedTable = tableConverter.toTable(table);
			if (!convertedTable.isPresent()) {
				stats.incCounter(TABLE_COUNTERS.SMALL_OR_IRREGULAR_TABLES);
				continue;
			}
			ClassificationResult cResult = tableClassifier.classifyTable(convertedTable.get());

			stats.incCounter(cResult.getTableType());
			if (cResult.getTableType() == TableType.LAYOUT) {
				continue;
			}

			Dataset ds = new Dataset();
			ds.relation = toArrayOfString(convertedTable.get());
			ds.headerPosition = headerPosition(table.getElementsByTag("tr"));
			ds.tableNum = count;
			ds.s3Link = metadata.getS3Link();
			ds.recordOffset = metadata.getStart();
			ds.recordEndOffset = metadata.getEnd();
			ds.url = metadata.getUrl();
			ds.tableType = cResult.getTableType();

			if (tags == null && extract_terms) {
				String bodyContent = doc.select("body").text();
				Set<String> tagSet = termExtractor.topNTerms(bodyContent, 100);
				tags = tagSet.toArray(new String[]{});
				Arrays.sort(tags);
			}
			ds.termSet = tags;
			ds.hasHeader = has_header;
			Elements caption = table.select("caption");
			if (caption.size() == 1)
				ds.setTitle(cleanCell(caption.get(0).text()));
			ds.setPageTitle(doc.title());
			stats.incCounter(TABLE_COUNTERS.RELATIONS_FOUND);
			result.add(ds);
		}
		return result;
	}

	/*
	 * Convert to DWTC output format.
	 * MH uses row-major, DWTC uses col-major layout.
	 * MH uses Element[][], DWTC uses String[][]
	 * MH may contain null, DWTC not
	 */
	private String[][] toArrayOfString(Element[][] table) {
		int numCols = table[0].length;
		int numRows = table.length;
		String[][] relation = new String[numCols][numRows];

		for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
			for (int colIndex = 0; colIndex < table[rowIndex].length; colIndex++) {
				Element cell = table[rowIndex][colIndex];
				String cellStr;
				if (cell == null)
					cellStr = "";
				else
					cellStr = cleanCell(cell.text());
				relation[colIndex][rowIndex] = cellStr;
			}
		}

		return relation;
	}

	private static int NUM_RUNS = 50;

	public static void main(String[] args) throws MalformedURLException,
			IOException, InterruptedException {
		TableClassification tableClassifier = new TableClassification("/SimpleCart_P1.mdl", "/RandomForest_P2.mdl");
		ExtractionAlgorithm ea = new MHExtractionAlgorithm(
				new StatsKeeper.HashMapStats(), true, tableClassifier);

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
//			System.out.println(ea.stats.statsAsMap().toString());
		}
	}

}
