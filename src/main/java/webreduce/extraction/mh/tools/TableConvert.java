package webreduce.extraction.mh.tools;

import java.io.FileInputStream;
import java.io.InputStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.google.common.base.Optional;

public class TableConvert {

	protected int minRows;
	protected int minCols;

	public TableConvert(int minRows, int minCols) {
		this.minRows = minRows;
		this.minCols = minCols;
	}

	// converts a 'table'-DOM-Element to a 2D array of Elements representing
	// the table
	// single cells can then be accessed via result[row][col]
	public Optional<Element[][]> toTable(Element table) {
		if (!table.nodeName().equals("table")) {
			return Optional.absent();
		}

		// this code won't work if a full row or full column
		// has a rowspan or colspan respectively -
		// this would lead to incorrect calculation of tableWidth
		// and tableHeight, because virtual span-generated rows or
		// cols are ignored whereas the computation depends on correct
		// table width and table height when parsing spans

		// this is addressed using the throws declaration

		Elements tableRows = table.getElementsByTag("tr");
		int tableHeight = tableRows.size();

		int tableWidth = 0;
		for (int tr_idx = 0; tr_idx < tableHeight; tr_idx++) {
			Elements tds = tableRows.get(tr_idx).select("td, th");
			int td_size = tds.size();
			if (td_size > tableWidth)
				tableWidth = td_size;
		}

		if (tableHeight < 2 || tableWidth < 2)
			return Optional.absent();

		Element[][] result = new Element[tableHeight][tableWidth];

		try {
			row_computation:
			for (int rowIndex = 0; rowIndex < tableHeight; rowIndex++) {
				Elements colCells = tableRows.get(rowIndex).select("td, th");

				int colIndex = 0;
				column_computation:
				for (Element currentCell : colCells) {

					// skip cells which have been marked in result array
					// (entries in result should always be null unless
					// manipulated beforehand, which means those cells
					// have been reserved for spans)

					// this will throw an IndexOutOfBounds exception if the table
					// is malformed (i.e. invalid spans). Calling methods should
					// catch those errors and ignore these tables
					while(result[rowIndex][colIndex] != null) {
						result[rowIndex][colIndex] = null;
						colIndex++;
					}

					if (rowIndex >= tableHeight) {
						// System.out.println("ERROR: table row index out of bounds, cancelling table");
						break row_computation;
					} else if (colIndex >= tableWidth) {
						// System.out.println("ERROR: table column index out of bounds (invalid colspan?), continue with next row");
						continue column_computation;
					}
					result[rowIndex][colIndex] = currentCell;

					// check for colspan attribute
					int colspan = 1;
					if (currentCell.hasAttr("colspan")) {
						colspan = Integer.valueOf(currentCell.attr("colspan"));
					}

					// if colspan > 1 then insert blank cells into the result array
					// (= skip their position in the result array)
					if (colspan > 1) {
						int emptyCells = colspan - 1;
						colIndex += emptyCells;
					}
					// check for rowspan attribute
					int rowspan = 1;
					if (currentCell.hasAttr("rowspan")) {
						rowspan = Integer.valueOf(currentCell.attr("rowspan"));
					}

					// if rowspan > 1 then mark cells in following rows which are affected
					if (rowspan > 1) {
						for (int i = 1; i < rowspan; i++) {
							if (i >= tableHeight) break; // ignore bad rowspans

							// mark cell
							org.jsoup.parser.Tag invalidTag = org.jsoup.parser.Tag.valueOf("p");
							result[rowIndex+i][colIndex] = new Element(invalidTag, "");
						}
					}

					// switch to next column in the result array
					colIndex++;
				}
			}
		}
		// all irregular tables will lead to Exceptions (i.e. IndexOutOfBounds)
		catch (Exception e) {
			return Optional.absent();
		}

		return Optional.of(result);
	}

	public void printTable(Element[][] table) {
		if (table == null) return;

		System.out.println(table[0][0].text());

		int limiter = table[0].length*4+1;

		for (int i = 0; i < limiter; i++) {
			System.out.print("-");
		}
		System.out.println();

		for (int rowIndex = 0; rowIndex < table.length; rowIndex++) {
			System.out.print("|");
			for (int colIndex = 0; colIndex < table[rowIndex].length; colIndex++) {
				if (table[rowIndex][colIndex] == null) {
					System.out.print("  ");
				} else {
					System.out.print(" X");
				}
				System.out.print(" |");
			}
			System.out.println();
			for (int i = 0; i < limiter; i++) {
				System.out.print("-");
			}
			System.out.println();
		}
	}

	// ONLY FOR TESTING PURPOSES
	// prints out all leaf-tables as 2D arrays at a specified url
	public static void main(String[] args) {
		String url = "/Users/mahe/Desktop/testTable.htm";
		InputStream in;
		TableConvert tableConvert = new TableConvert(2, 2);
		try {
			in = new FileInputStream(url);;
			Document doc = Jsoup.parse(in, null, "");

			for (Element aTable : doc.getElementsByTag("table")) {
				Elements subtables = aTable.getElementsByTag("table");
				// the table tag of the table itself is always included in the subtable
				// query result, so remove it
				subtables.remove(aTable);
				if(subtables.size() == 0) {
					System.out.println("converting table...");
					Optional<Element[][]> result = tableConvert.toTable(aTable);
					if (result.isPresent())
						tableConvert.printTable(result.get());
					else
						System.out.println("Could not convert table.");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}


	}
}
