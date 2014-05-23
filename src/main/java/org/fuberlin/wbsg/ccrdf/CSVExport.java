package org.fuberlin.wbsg.ccrdf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

public class CSVExport {
	private static Logger log = Logger.getLogger(CSVExport.class);

	/**
	 * Cache the CSVWriters for each file that we write to
	 * */
	private static Map<File, CSVWriter> writerMap = new HashMap<File, CSVWriter>();
	private static Map<File, Boolean> headerWritten = new HashMap<File, Boolean>();

	public static void writeToFile(Map<String, Object> stats, File statFile) {
		// get and sort keys from the map, they are going to become column names
		// in the CSV file
		String[] keys = {};
		keys = (String[]) stats.keySet().toArray(new String[0]);
		Arrays.sort(keys);

		CSVWriter writer;
		if (!writerMap.containsKey(statFile)) {
			try {
				writer = new CSVWriter(new OutputStreamWriter(
						new GZIPOutputStream(new FileOutputStream(statFile,
								false))), ',');

				writerMap.put(statFile, writer);
				// if the file is empty, add the column name line
				if (!headerWritten.containsKey(statFile)
						|| headerWritten.get(statFile)) {
					writer.writeNext(keys);
					headerWritten.put(statFile, true);
				}
			} catch (IOException e) {
				log.warn("Unable to write to statistics file " + statFile, e);
			}
		}
		writer = writerMap.get(statFile);

		// write the data
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < keys.length; i++) {
			Object value = stats.get(keys[i]);
			if (value != null) {
				values.add(value.toString());
			} else {
				values.add("");
			}
		}
		writer.writeNext((String[]) values.toArray(new String[0]));
	}

	public static void closeWriter(File statFile) {
		CSVWriter writer = writerMap.get(statFile);
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				log.warn(e);
			}
		}
	}

	public static String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1)
				+ (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}
}
