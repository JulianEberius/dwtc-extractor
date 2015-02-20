package webreduce.extraction.mh.warcTools;

import org.jsoup.nodes.Element;

public class ExtractedTable {
	private Element table;
	private long offset, endOffset;
	private String warcName, baseURL;
	private int tableCount, recordCount;
	
	
	public ExtractedTable(Element table, String warcName, String baseURL, long offset, long endOffset, int tableCount, int recordCount) {
		this.table = table;
		this.warcName = warcName;
		this.baseURL = baseURL;
		this.offset = offset;
		this.endOffset = endOffset;
		this.tableCount = tableCount;
		this.recordCount = recordCount;
	}


	public Element getTable() {
		return table;
	}


	public long getOffset() {
		return offset;
	}


	public long getEndOffset() {
		return endOffset;
	}


	public String getWarcName() {
		return warcName;
	}


	public int getTableCount() {
		return tableCount;
	}


	public int getRecordCount() {
		return recordCount;
	}


	public String getBaseURL() {
		return baseURL;
	}
	
	
	
}
