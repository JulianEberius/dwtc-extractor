package webreduce.extraction.mh.features;

/*
 * Class contains only feature calculations for phase 2
 * (multiclass classification for non-layout tables)
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

public class FeaturesP2 {
	
	// most of the local features are calculated in batches for all rows/colums
	// we need a whitelist to filter out those columns and rows we don't need
	private static String featureWhiteList = "LOCAL_RATIO_IS_NUMBER_COL_0, AVG_CELL_LENGTH, LOCAL_RATIO_IS_NUMBER_COL_2, LOCAL_RATIO_COLON_ROW_1, LOCAL_RATIO_ANCHOR_ROW_2, LOCAL_LENGTH_VARIANCE_COL_2, LOCAL_AVG_LENGTH_ROW_0, LOCAL_AVG_LENGTH_ROW_2, LOCAL_RATIO_HEADER_ROW_0, CUMULATIVE_CONTENT_CONSISTENCY, STD_DEV_ROWS, RATIO_ALPHABETICAL, LOCAL_RATIO_COMMA_COL_0, LOCAL_RATIO_CONTAINS_NUMBER_ROW_1, LOCAL_RATIO_CONTAINS_NUMBER_ROW_0, STD_DEV_COLS, LOCAL_RATIO_COLON_COL_0, MAX_COLS, LOCAL_RATIO_CONTAINS_NUMBER_COL_2, LOCAL_RATIO_HEADER_COL_1, LOCAL_RATIO_HEADER_COL_2, LOCAL_RATIO_CONTAINS_NUMBER_COL_0, AVG_COLS";
	
	private ArrayList<AbstractTableListener> globalListeners;
	private ArrayList<AbstractTableListener> localListeners;
	private ArrayList<Attribute> attributeList;
	
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
	
	public FeaturesP2() {
		attributeList = new ArrayList<Attribute>();
		attributeVector = new FastVector();
		for (String s : FeaturesP2.getFeatureNames()) {
			Attribute newAttr = new Attribute(s); // create new Feature with name from whitelist
			attributeList.add(newAttr);
			attributeVector.addElement(newAttr);
		}
		
		classAttrVector = new FastVector(5);
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
		globalListeners.add(new MaxCols());
		globalListeners.add(new AvgRows());
		globalListeners.add(new AvgCols());
		globalListeners.add(new AvgCellLength());
		globalListeners.add(new StdDevRows());
		globalListeners.add(new StdDevCols());
		globalListeners.add(new ContentRatios());
		globalListeners.add(new CumulativeContentTypeConsistency());
		
		// Add local features to computation list
		localListeners = new ArrayList<AbstractTableListener>();
		localListeners.add(new LocalAvgLength());
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
		// get the 2 first and last rows
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
//		
//		// only use features within whitelist
//		for (String entry : resultMap.keySet()) {
//			if (featureWhiteList.contains(entry)) {
//				Attribute newAttr = new Attribute(entry);
//				resultInstance.setValue(newAttr, resultMap.get(entry));
//			}
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
	
	public class MaxCols extends AbstractTableListener {
		private double maxCols;
		
		public MaxCols() {
			featureName = "MAX_COLS";
		}
		
		public void initialize(TableStats stats) {
			maxCols = stats.getTableWidth();
		}
		
		public void onCell(Element content, TableStats stats) {
			
		}

		public void finalize() {
			
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(maxCols));
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

	public class AvgCellLength extends AbstractTableListener {
		private int cellCount;
		private int totalLength;
		private double avgLength;
		
		public AvgCellLength() {
			featureName = "AVG_CELL_LENGTH";
		}
		
		public void initialize(TableStats stats) {
			cellCount = 0;
			totalLength = 0;
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				cellCount++;
				// totalLength += content.text().length();
				totalLength += CellTools.getCellLength(content);
			}
		}
		
		public void finalize() {
			avgLength = ((double) totalLength) / ((double) cellCount);
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(avgLength));
			return result;
		}
	}
	
	public class StdDevRows extends AbstractTableListener {
		private double stdDevRows;
		private int[] rowNum;
		
		public StdDevRows() {
			featureName = "STD_DEV_ROWS";
		}
		
		public void initialize(TableStats stats) {
			stdDevRows = 0;
			rowNum = new int[stats.getTableWidth()];
			
			for (int i = 0; i < rowNum.length; i++) {
				rowNum[i] = 0;
			}
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				rowNum[stats.colIndex] += 1;
			}
		}
		
		public void finalize() {
			double sum = 0;
			double avgRows = getAvgRows();
			for (int colIndex = 0; colIndex < rowNum.length; colIndex++) {
				double temp = rowNum[colIndex] - avgRows;
				sum += (double) Math.pow(temp, 2);
			}
			stdDevRows = (double) Math.sqrt(sum / rowNum.length);
		}
		
		private double getAvgRows() {
			int tempSum = 0;
			for (int colIndex = 0; colIndex < rowNum.length; colIndex++) {
				tempSum += rowNum[colIndex];
			}
			return (double) tempSum / (double) rowNum.length;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(stdDevRows));
			return result;
		}
	}
	
	public class StdDevCols extends AbstractTableListener {
		private double stdDevCols;
		private int[] colNum;
		
		public StdDevCols() {
			featureName = "STD_DEV_COLS";
		}
		
		public void initialize(TableStats stats) {
			stdDevCols = 0;
			colNum = new int[stats.getTableHeight()];
			
			for (int i = 0; i < colNum.length; i++) {
				colNum[i] = 0;
			}
		}
		
		public void onCell(Element content, TableStats stats) {
			if (content != null) {
				colNum[stats.rowIndex] += 1;
			}
		}
		
		public void finalize() {
			double sum = 0;
			double avgCols = getAvgCols();
			for (int rowIndex = 0; rowIndex < colNum.length; rowIndex++) {
				double temp = colNum[rowIndex] - avgCols;
				sum += (double) Math.pow(temp, 2);
			}
			stdDevCols = (double) Math.sqrt(sum / colNum.length);
		}
		
		private double getAvgCols() {
			int tempSum = 0;
			for (int rowIndex = 0; rowIndex < colNum.length; rowIndex++) {
				tempSum += colNum[rowIndex];
			}
			return (double) tempSum / (double) colNum.length;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(stdDevCols));
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

	public class ContentRatios extends AbstractTableListener {
		private int cellCount, images, alphabetical, digits;
		private double image_ratio, alphabetical_ratio,
		digit_ratio;
		
		public ContentRatios() {
			featureName = "GROUP_GLOBAL_CONTENT_RATIOS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = 0;
			images = 0;
			alphabetical = 0;
			digits = 0;
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
					default:
						break;
				}
			}
		}
		
		public void finalize() {
			image_ratio =			(cellCount > 0) ? ((double) images / (double) cellCount)		: 0.0;
			alphabetical_ratio =	(cellCount > 0) ? ((double) alphabetical / (double) cellCount)	: 0.0;
			digit_ratio =			(cellCount > 0) ? ((double) digits / (double) cellCount)		: 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put("RATIO_IMG", new Double(image_ratio));
			result.put("RATIO_ALPHABETICAL", new Double(alphabetical_ratio));
			result.put("RATIO_DIGIT", new Double(digit_ratio));
			return result;
		}
	}	

	
	public class CumulativeContentTypeConsistency extends AbstractTableListener {
		private double ctc_sum_r, ctc_r; // Cumulative Length Consistency of rows
		private double ctc_sum_c, ctc_c; // Cumulative Length Consistency of columns
		private double tableWidth, tableHeight;
		private double ctc; 

		private ArrayList<ContentType> typesOfRow;
		private ArrayList<ContentType>[] typesOfCols;

		public CumulativeContentTypeConsistency() {
			featureName = "CUMULATIVE_CONTENT_CONSISTENCY";
		}
		
		public void initialize(TableStats stats) {
			
			// typesOfRow is a temporary array, recreated for each row, that keeps track
			// of each ContentType within the row
			typesOfRow = new ArrayList<ContentType>();
			tableWidth = stats.getTableWidth(); 
			tableHeight = stats.getTableHeight();
			
			// typesOfCols has one ArrayList-slot for each column in the table
			// thus each column has its own ArrayList of ContentTypes
			typesOfCols = new ArrayList[stats.getTableWidth()];
			for (int i = 0; i < typesOfCols.length; i++) {
				typesOfCols[i] = new ArrayList<ContentType>();
			}
		}
		
		// returns the dominant ContentType within a List of ContentType values
		// takes the ContentType enum's priority order into account
		private ContentType getDominantType(List<ContentType> list) {
			ContentType dominantType = ContentType.EMPTY;
			HashMap<ContentType,Integer> frequencyMap = new HashMap<ContentType,Integer>();

			// put all occurrences of ContentTypes into a map together with their frequency count
			for (ContentType ct : list) {
				if (!frequencyMap.containsKey(ct)) {
					frequencyMap.put(ct, Collections.frequency(list, ct));
				}
			}
			
			int maxCount = 0;
			for (ContentType ct : frequencyMap.keySet()) {
				int currentCount = frequencyMap.get(ct);
				
				if (currentCount > maxCount) {
					dominantType = ct; // new dominant type determined
				} else if (currentCount == maxCount) {
					
					// as the ContentType enum itself is sorted by priority of the
					// content types compare the ordinal values
					
					// replace the dominant type with the current one only if they both
					// have the same frequency but the current one has higher priority
					// (= lesser ordinal value)
					if (ct.ordinal() < dominantType.ordinal()) {
						dominantType = ct;
					}
				}
			}		

			return dominantType;
		}

		public void onCell(Element content, TableStats stats) {
			
			// every cell that is non-empty
			if (content != null) {
				ContentType cellType = CellTools.getContentType(content);
				typesOfRow.add(cellType);
				typesOfCols[stats.colIndex].add(cellType);
			}
			
			/// ROWS - handle row end
			if (stats.colIndex == stats.getTableWidth() - 1) { // last column = row end reached

				// determine dominant type for row
				ContentType dominantType = getDominantType(typesOfRow);
				
				// compute the CTC for the current row
				double sumD = 0.0;
				for (ContentType ct : typesOfRow) {
					double d = (ct == dominantType) ? (d = 1.0) : (d = -1.0);
					sumD += d;
				}
				ctc_sum_r += sumD;
				typesOfRow.clear();

			}
			/// ROWS.END
			
			/// COLS - handle column end
			if (stats.rowIndex == stats.getTableHeight() - 1) { // last row = column end reached
					
				int colIndex = stats.colIndex; // index of the current column
				
				// determine dominant content type for column
				ContentType dominantType = getDominantType(typesOfCols[colIndex]);

				
				// compute the CTC for the current column
				double sumD = 0.0;
				for (ContentType ct : typesOfCols[colIndex]) {
					double d = (ct == dominantType) ? (d = 1.0) : (d = -1.0);
					sumD += d;
				}
				ctc_sum_c += sumD;
			}
			/// COLS.END
		}
		
		public void finalize() {
			// compute Cumulative Length Consistency over all rows
			ctc_r = ctc_sum_r / tableHeight;
			ctc_c = ctc_sum_c / tableWidth;
			ctc = Math.max(ctc_r, ctc_c);
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put(featureName, new Double(ctc));
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
	
	public class LocalContentRatios extends AbstractTableListener {
		private int cellCount, count_th, count_anchor, count_img, count_input, count_select,
			 count_contains_number, count_is_number, count_colon, count_comma;
		private double ratio_th, ratio_anchor, ratio_img, ratio_input, ratio_select,
			ratio_contains_number, ratio_is_number, ratio_colon, ratio_comma;
		
		public LocalContentRatios() {
			featureName = "GROUP_LOCAL_CONTENT_RATIOS";
		}
		
		public void initialize(TableStats stats) {
			cellCount = count_th = count_anchor = count_img = count_input = count_select =
					 count_contains_number = count_is_number = count_colon = count_comma = 0;
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
			if (content.getElementsByTag("select").size() > 0) {
				count_select++;
			}
			String cleanedContent = CellTools.cleanCell(content.text());
			if (cleanedContent.endsWith(":")) {
				count_colon++;
			}
			if (cleanedContent.contains(",")) {
				count_comma++;
			}
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
			ratio_select =			(cellCount > 0) ? ((double) count_select / (double) cellCount)			: 0.0;
			ratio_colon =			(cellCount > 0) ? ((double) count_colon / (double) cellCount)			: 0.0;
			ratio_contains_number =	(cellCount > 0) ? ((double) count_contains_number / (double) cellCount)	: 0.0;
			ratio_is_number =		(cellCount > 0) ? ((double) count_is_number / (double) cellCount)		: 0.0;
			ratio_comma =			(cellCount > 0) ? ((double) count_comma / (double) cellCount)			: 0.0;
		}
		
		public HashMap<String, Double> getResults() {
			HashMap<String, Double> result = new HashMap<String, Double>();
			result.put("LOCAL_RATIO_HEADER", new Double(ratio_th));
			result.put("LOCAL_RATIO_ANCHOR", new Double(ratio_anchor));
			result.put("LOCAL_RATIO_IMAGE", new Double(ratio_img));
			result.put("LOCAL_RATIO_INPUT", new Double(ratio_input));
			result.put("LOCAL_RATIO_SELECT", new Double(ratio_select));
			result.put("LOCAL_RATIO_COLON", new Double(ratio_colon));
			result.put("LOCAL_RATIO_CONTAINS_NUMBER", new Double(ratio_contains_number));
			result.put("LOCAL_RATIO_IS_NUMBER", new Double(ratio_is_number));
			result.put("LOCAL_RATIO_COMMA", new Double(ratio_comma));
			return result;
		}
	}
	

}
