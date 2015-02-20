package webreduce.extraction.mh.tools;

public class TableStats {
	private int tableWidth;
	public int getTableWidth() {
		return tableWidth;
	}

	public int getTableHeight() {
		return tableHeight;
	}

	private int tableHeight;
	
	public int rowIndex;
	public int colIndex;
	
	public TableStats(int width, int height) {
		this.tableHeight = height;
		this.tableWidth = width;
	}
}
