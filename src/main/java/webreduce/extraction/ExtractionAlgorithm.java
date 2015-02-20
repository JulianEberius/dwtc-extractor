package webreduce.extraction;

import java.io.IOException;
import java.util.List;

import org.jsoup.nodes.Document;

import webreduce.data.Dataset;

public interface ExtractionAlgorithm {

	public abstract List<Dataset> extract(Document doc,
			DocumentMetadata metadata) throws IOException, InterruptedException;

	public abstract StatsKeeper getStatsKeeper();

}