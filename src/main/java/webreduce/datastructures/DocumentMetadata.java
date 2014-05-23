package webreduce.datastructures;

/* Intermediate product of the extraction process */
public class DocumentMetadata {
	private long start;
	private long end;
	private String s3Link;
	private String url;
	
	public DocumentMetadata(long start, long end, String s3Link, String url) {
		super();
		this.start = start;
		this.end = end;
		this.s3Link = s3Link;
		this.url = url;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public String getS3Link() {
		return s3Link;
	}
	public void setS3Link(String s3Link) {
		this.s3Link = s3Link;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}
