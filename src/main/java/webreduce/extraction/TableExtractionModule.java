package webreduce.extraction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.fuberlin.wbsg.ccrdf.ProcessingNode;

import webreduce.extraction.StatsKeeper.HashMapStats;
import webreduce.extraction.basic.BasicExtractionAlgorithm;
import webreduce.extraction.mh.MHExtractionAlgorithm;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class TableExtractionModule extends AbstractModule {

	private Properties properties;

	@Override
	protected void configure() {
		Properties defaults = new Properties();
		defaults.setProperty("extractTopNTerms", "true");
		defaults.setProperty("extractionAlgorithm", "mh");
		defaults.setProperty("phase1ModelPath", "/SimpleCart_P1.mdl");
		defaults.setProperty("phase2ModelPath", "/RandomForest_P2.mdl");

		properties = new Properties(defaults);
		try {
			InputStream pStream = ProcessingNode.class
					.getResourceAsStream("/webreduce.properties");
			properties.load(pStream);
			Names.bindProperties(binder(), properties);
		} catch (FileNotFoundException e) {
			System.out
					.println("The configuration file webreduce.properties can not be found");
		} catch (IOException e) {
			System.out.println("I/O Exception during loading configuration");
		}

		// class bindings
		bind(StatsKeeper.class).to(HashMapStats.class);
		
		String algorithmName = properties.getProperty("extractionAlgorithm");
		if (algorithmName.equals("mh")
				|| algorithmName.equals("MHExtractionAlgorithm"))
			bind(ExtractionAlgorithm.class).to(MHExtractionAlgorithm.class);
		else
			bind(ExtractionAlgorithm.class).to(BasicExtractionAlgorithm.class);
	}
}
