package webreduce.extraction.mh.features;

/*
 * Class contains only feature calculations for phase 1
 * (binary classification between layout and non-layout tables)
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jsoup.nodes.Element;
import webreduce.extraction.mh.tools.CellTools;
import webreduce.extraction.mh.tools.ContentType;
import webreduce.extraction.mh.tools.TableStats;
import webreduce.extraction.mh.tools.Tools;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;

public class FeaturesP1 {
	
	// most of the local features are calculated in batches for all rows/colums
	// we need a whitelist to filter out those columns and rows we don't need
	private static String featureWhiteList = "LOCAL_RATIO_IS_NUMBER_COL_1, LOCAL_RATIO_ANCHOR_ROW_1, RATIO_IMG, LOCAL_RATIO_ANCHOR_COL_1, LOCAL_LENGTH_VARIANCE_COL_1, LOCAL_RATIO_IMAGE_COL_1, LOCAL_RATIO_IMAGE_COL_0, LOCAL_SPAN_RATIO_COL_2, LOCAL_SPAN_RATIO_COL_1, LOCAL_AVG_LENGTH_ROW_2, LOCAL_RATIO_HEADER_ROW_0, RATIO_DIGIT, LOCAL_RATIO_IMAGE_ROW_0, RATIO_ALPHABETICAL, LOCAL_RATIO_IMAGE_ROW_1, LOCAL_RATIO_INPUT_COL_1, LOCAL_RATIO_INPUT_COL_0, LOCAL_RATIO_CONTAINS_NUMBER_ROW_2, LOCAL_AVG_LENGTH_COL_0, RATIO_EMPTY, AVG_ROWS, LOCAL_RATIO_INPUT_ROW_1, LOCAL_RATIO_CONTAINS_NUMBER_COL_2, LOCAL_RATIO_HEADER_COL_1, LOCAL_RATIO_INPUT_ROW_0, AVG_COLS";
	
	private ArrayList<AbstractTableListener> globalListeners;
	private ArrayList<AbstractTableListener> localListeners;
	private ArrayList<Attribute> attributeList; // attribute list WITHOUT class attribute
	
	private FastVector attributeVector; // vector of all atrributes PLUS class attribute
	private FastVector classAttrVector; // vector of strings of all possible class values
	private Attribute classAttr;
	
	public static List<String> getFeatureNames() {
		return Arrays.asList(featureWhiteList.split(", "));
	}
	
	// returns a FastVector containing all attributes plus
	// the class attribute as the last element
	public FastVector getAttrVector() {
		return attributeVector;
	}
	
	// returns a FastVector that contains all possible class
	// values, each represented as a String
	public FastVector getClassVector() {
		return classAttrVector;
	}
	
	public FeaturesP1() {
		attributeList = new ArrayList<Attribute>();
		attributeVector = new FastVector();
		for (String s : FeaturesP1.getFeatureNames()) {
			Attribute newAttr = new Attribute(s); // create new Feature with name from whitelist
			attributeList.add(newAttr);
			attributeVector.addElement(newAttr);
		}
		
		classAttrVector  = new FastVector(5);
		classAttrVector.addElement("LAYOUT");
		classAttrVector.addElement("RELATION");
		classAttrVector.addElement("ENTITY");
		classAttrVector.addElement("MATRIX");
		classAttrVector.addElement("NONE");
		classAttr = new Attribute("CLASS", classAttrVector);
		
		attributeVector.addElement(classAttr);
	}
	
	// returns an ArrayList of all attributes that
	// are used for this feature phase
	public ArrayList<Attribute> getAttrList() {
		return attributeList;
	}
	
	// adds all desired features to the computation list
	public void initializeFeatures() {
		// Add global features to computation list
		globalListeners = new ArrayList<AbstractTableListener>();
		globalListeners.add(new AvgRows());
		globalListeners.add(new AvgCols());
		globalListeners.add(new ContentRatios());
		
		// Add local features to computation list
		localListeners = new ArrayList<AbstractTableListener>();
		localListeners.add(new LocalAvgLength());
		localListeners.add(new LocalRatioSpan());
		localListeners.add(new LocalContentRatios());
		localListeners.add(new LocalLengthVariance());
	}
	
	public Instance computeFeatures(Element[][] convertedTable) {
		HashMap<String, Double> resultMap = new HashMap<String, Double>();
		TableStats tStats = new TableStats(convertedTable[0].length, convertedTable.length);
		
		initializeFeatures();
		
		// GLOBAL FEATURES
		
		// initialization event
		for (AbstractTableListener listener : globalListeners) {
			listener.start(tStats);
		}
		
		for (tStats.rowIndex = 0; tStats.rowIndex < tStats.getTableHeight(); tStats.rowIndex++) {
			for (tStats.colIndex = 0; tStats.colIndex < tStats.getTableWidth(); tStats.colIndex++) {
				
				// onCell event
				for (AbstractTableListener listener : globalListeners) {
					listener.computeCell(convertedTable[tStats.rowIndex][tStats.colIndex], tStats);
				}	
				
			}

		}
		
		// end event
		for (AbstractTableListener listener : globalListeners) {
			listener.end();
		}
		
		// compute results of all listeners and put them into the result map
		for (AbstractTableListener listener : globalListeners) {
			resultMap.putAll(listener.getResults());
		}
		

		// LOCAL FEATURES
		
		// PER-ROW
		// get the 2 first and 2 last rows
		int[] localRowIndexes = {0, 1, tStats.getTableHeight()-1};
		
		for (int i = 0; i < localRowIndexes.length; i++) {
			
			int currentRowIndex = localRowIndexes[i];
			
			// initialization event
			for (AbstractTableListener listener : localListeners) {
				listener.start(tStats);
			}
			
			// iterate cells within row
			for (tStats.colIndex = 0; tStats.colIndex < tStats.getTableWidth(); tStats.colIndex++) {
				// onCell event
				for (AbstractTableListener listener : localListeners) {
					listener.computeCell(convertedTable[currentRowIndex][tStats.colIndex], tStats);
				}
			}
			
			// onRowEnd event
			for (AbstractTableListener listener : localListeners) {
				listener.end();
			}
			
			// compute results of all listeners, rename them and put them 
			// into the result map
			for (AbstractTableListener listener : localListeners) {
				HashMap<String, Double> results = listener.getResults();
				for (Map.Entry<String, Double> entry : results.entrySet()) {
					// insert as ORIGINAL_ATTRIBUTE_NAME_ROW_X where ROW_X is the
					// specific row of the current loop
					resultMap.put(entry.getKey() + "_ROW_" + i, entry.getValue());
				}
				
			}
		}
		
		// PER-COL
		// get the 2 first and last columns
		int[] localColIndexes = {0, 1, tStats.getTableWidth()-1};
		
		for (int i = 0; i < localColIndexes.length; i++) {
			
			int currentColIndex = localColIndexes[i];
			
			// initialization event
			for (AbstractTableListener listener : localListeners) {
				listener.start(tStats);
			}
			
			// iterate cells within column
			for (tStats.rowIndex = 0; tStats.rowIndex < tStats.getTableHeight(); tStats.rowIndex++) {
				// onCell event
				for (AbstractTableListener listener : localListeners) {
					listener.computeCell(convertedTable[tStats.rowIndex][currentColIndex], tStats);
				}
			}
			
			// onColEnd event
			for (AbstractTableListener listener : localListeners) {
				listener.end();
			}
	
			// compute results of all listeners, rename them and put them 
			// into the result map
			for (AbstractTableListener listener : localListeners) {
				HashMap<String, Double> results = listener.getResults();
				for (Map.Entry<String, Double> entry : results.entrySet()) {
					// insert as ORIGINAL_ATTRIBUTE_NAME_COL_X where COL_X is the
					// specific column of the current loop
					resultMap.put(entry.getKey() + "_COL_" + i, entry.getValue());
				}
				
			}
		}

		// Create WEKA instance
		
//		Instance resultInstance = new Instance(featureCount);
		
		// only use features within whitelist
//		for (String entry : resultMap.keySet()) {
//			if (featureWhiteList.contains(entry)) {
//				Attribute newAttr = new Attribute(entry);
//				resultInstance.setValue(newAttr, resultMap.get(entry));
//			}
//		}
		
		// The order in which the features are stored into the instance
		// is CRUCIAL! It has to be the same order the models were gen-
		// erated with!
//		List<String> fnames = getFeatureNames();
//		for (int i = 0; i < fnames.size(); i++) {
//			String fname = fnames.get(i);
//			resultInstance.setValue(i, resultMap.get(fname));
//		}
		
		return Tools.createInstanceFromData(resultMap, attributeList, attributeVector);
	}
	
		
 
	// Features are implemented according to an Observer Pattern
	public abstract class AbstractTableListener {
		
		protected String featureName = "ABSTRACT_TABLE_LISTENER";
		
		public void start(TableStats stats) {
			initialize(stats);
		}
		
		// should be called once the table is ready for iteration
		protected abstract void initialize(TableStats stats);
		
		
		public void computeCell(Element content, TableStats stats) {
			onCell(content, stats);
		}
		
		// should be called each time a cell is inspected by the subject
		protected abstract void onCell(Element content, TableStats stats);
		
		public void end() {
			finalize();
		}
		
		public String getFeatureName() {
			return featureName;
		}
		
		// should be called once the table iteration has finished
		protected abstract void finalize();
		
		// pairs of feature names and feature values are given as result
		public abstract HashMap<String, Double> getResults();
	}
	
	
	////
	// GLOBAL FEATURES DEFINITION
	///
	
	// template
	public class BlankTableListener extends AbstractTableListener {
		
		public BlankTableListener() {
			featureName = "BLANK_FEATURE";
		}
		
		public void initialize(TableStats stats) {
			
		}
		
		public void onCell(Element content, TableStats stats) {
			
		}
		
		public void finalize() {
			
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(0));
			return result;
		}
	}
	
	public class AvgRows extends AbstractTableListener {
		private int cellCount;
		private double avgRows;
		private int tableWidth;
		
		public AvgRows() {
			featureName = "AVG_ROWS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = 0;
			tableWidth = stats.getTableWidth();
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellCount++;
			}
		}
		
		public void finalize() {
			avgRows = ((double) cellCount) / ((double) tableWidth);
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(avgRows));
			return result;
		}
	}
	
	public class AvgCols extends AbstractTableListener {
		private int cellCount;
		private double avgCols;
		private int tableHeight;
		
		public AvgCols() {
			featureName = "AVG_COLS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = 0;
			tableHeight = stats.getTableHeight();
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellCount++;
			}
		}
		
		public void finalize() {
			avgCols = ((double) cellCount) / ((double) tableHeight);
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(avgCols));
			return result;
		}
	}

	public class ContentRatios extends AbstractTableListener {
		private int cellCount, images, alphabetical, digits, empty;
		private double image_ratio, alphabetical_ratio,
		digit_ratio, empty_ratio;
		
		public ContentRatios() {
			featureName = "GROUP_GLOBAL_CONTENT_RATIOS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = 0;
			images = 0;
			alphabetical = 0;
			digits = 0;
			empty = 0;
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellCount++;
				
				ContentType ct = CellTools.getContentType(content);
				
				switch(ct) {
					case IMAGE: images++;
						break;
					case ALPHABETICAL: alphabetical++;
						break;
					case DIGIT: digits++;
						break;
					case EMPTY: empty++;
						break;
					default:
						break;
				}
			}
		}
		
		public void finalize() {
			image_ratio =			(cellCount > 0) ? ((double) images / (double) cellCount)		: 0.0;
			alphabetical_ratio =	(cellCount > 0) ? ((double) alphabetical / (double) cellCount)	: 0.0;
			digit_ratio =			(cellCount > 0) ? ((double) digits / (double) cellCount)		: 0.0;
			empty_ratio =			(cellCount > 0) ? ((double) empty / (double) cellCount)			: 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put("RATIO_IMG", new Double(image_ratio));
			result.put("RATIO_ALPHABETICAL", new Double(alphabetical_ratio));
			result.put("RATIO_DIGIT", new Double(digit_ratio));
			result.put("RATIO_EMPTY", new Double(empty_ratio));
			return result;
		}
	}	

	
	
	////
	// LOCAL FEATURES DEFINITION
	///
	
	public class LocalAvgLength extends AbstractTableListener {
		private ArrayList<Integer> cellLengths;
		private double average;
		
		public LocalAvgLength() {
			featureName = "LOCAL_AVG_LENGTH";
		}
		
		public void initialize(TableStats stats) {
			cellLengths = new ArrayList<Integer>();
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellLengths.add(CellTools.getCellLength(content));
			}
		}
		
		public void finalize() {
			double sum = 0.0;
			for (Integer length : cellLengths) {
				sum += length;
			}
			double totalCells = (double) cellLengths.size();
			average = (totalCells > 0) ? (sum / totalCells) : 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(average));
			return result;
		}
	}
	
	public class LocalLengthVariance extends AbstractTableListener {
		private ArrayList<Integer> cellLengths;
		private double average, variance;
		
		public LocalLengthVariance() {
			featureName = "LOCAL_LENGTH_VARIANCE";
		}
		
		public void initialize(TableStats stats) {
			cellLengths = new ArrayList<Integer>();
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellLengths.add(CellTools.getCellLength(content));
			}
		}
		
		public void finalize() {
			double sum = 0.0;
			for (Integer length : cellLengths) {
				sum += length;
			}
			
			double totalCells = (double) cellLengths.size();
			average = (totalCells > 0) ? (sum / totalCells) : 0.0;
			
			double varSum = 0.0;
			for (Integer length : cellLengths) {
				double inner = (length - average);
				double temp = Math.pow(inner, 2);
				varSum += temp;
			}
			variance = (totalCells > 0) ? (varSum / totalCells) : 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(variance));
			return result;
		}
	}
	
	public class LocalRatioSpan extends AbstractTableListener {
		private int nullCells;
		private int totalCells;
		private double ratio;
		
		public LocalRatioSpan() {
			featureName = "LOCAL_SPAN_RATIO";
		}
		
		public void initialize(TableStats stats) {
			nullCells = 0;
			totalCells = 0;
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content == null) {
				nullCells++;
			}
			totalCells++;
		}
		
		public void finalize() {
			ratio = (totalCells > 0) ? ((double) nullCells / (double) totalCells) : 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(ratio));
			return result;
		}
	}
	
	public class LocalContentRatios extends AbstractTableListener {
		private int cellCount, count_th, count_anchor, count_img, count_input,
			 count_contains_number, count_is_number;
		private double ratio_th, ratio_anchor, ratio_img, ratio_input,
			ratio_contains_number, ratio_is_number;
		
		public LocalContentRatios() {
			featureName = "GROUP_LOCAL_CONTENT_RATIOS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = count_th = count_anchor = count_img = count_input =
					 count_contains_number = count_is_number = 0;
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content == null) {
				return;
			}
			
			// local content types are not exclusive!
			// thus they aren't determined via getContentType(...) - which is exclusive
			if (content.getElementsByTag("th").size() > 0) {
				count_th++;
			}
			if (content.getElementsByTag("a").size() > 0) {
				count_anchor++;
			}
			if (content.getElementsByTag("img").size() > 0) {
				count_img++;
			}
			if (content.getElementsByTag("input").size() > 0) {
				count_input++;
			}
			String cleanedContent = CellTools.cleanCell(content.text());
			// check for digit
			if (cleanedContent.matches(".*\\d.*")) {
				count_contains_number++;
			}
			// check if only digit
			if (CellTools.isNumericOnly(cleanedContent)) {
				count_is_number++;
			}
			cellCount++;
		}
		
		public void finalize() {
			ratio_th =				(cellCount > 0) ? ((double) count_th / (double) cellCount)				: 0.0;
			ratio_anchor =			(cellCount > 0) ? ((double) count_anchor / (double) cellCount)			: 0.0;
			ratio_img =				(cellCount > 0) ? ((double) count_img / (double) cellCount)				: 0.0;
			ratio_input =			(cellCount > 0) ? ((double) count_input / (double) cellCount)			: 0.0;
			ratio_contains_number =	(cellCount > 0) ? ((double) count_contains_number / (double) cellCount)	: 0.0;
			ratio_is_number =		(cellCount > 0) ? ((double) count_is_number / (double) cellCount)		: 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put("LOCAL_RATIO_HEADER", new Double(ratio_th));
			result.put("LOCAL_RATIO_ANCHOR", new Double(ratio_anchor));
			result.put("LOCAL_RATIO_IMAGE", new Double(ratio_img));
			result.put("LOCAL_RATIO_INPUT", new Double(ratio_input));
			result.put("LOCAL_RATIO_CONTAINS_NUMBER", new Double(ratio_contains_number));
			result.put("LOCAL_RATIO_IS_NUMBER", new Double(ratio_is_number));
			return result;
		}
	}
	

}
