package org.fuberlin.wbsg.ccrdf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;

public interface StatHandler {
	public void addStats(String key, Map<String, String> data);

	public void flush();
}

class LoggingStatHandler implements StatHandler {
	private static Logger log = Logger.getLogger(LoggingStatHandler.class);

	@Override
	public void addStats(String key, Map<String, String> data) {
		log.debug(key + ": " + data);
	}

	@Override
	public void flush() {
	}

}

class AmazonStatHandler implements StatHandler {
	private static Logger log = Logger.getLogger(AmazonStatHandler.class);

	private AmazonSimpleDBClient client;
	private static final int CACHE_SIZE = 24;
	private static final int MAX_TRIES = 20;
	private String domain;

	private Map<String, Map<String, String>> cache = new HashMap<String, Map<String, String>>();

	public AmazonStatHandler(AmazonSimpleDBClient client, String domain) {
		this.client = client;
		this.domain = domain;

		int tries = 0;
		do {
			tries++;
			try {
				ListDomainsResult domainsL = client.listDomains();
				if (!domainsL.getDomainNames().contains(domain)) {
					client.createDomain(new CreateDomainRequest(domain));
				}
				return;
			} catch (Exception ase) {
				log.warn(ase);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}				log.warn(ase);

			}
		} while (tries < MAX_TRIES);
		throw new RuntimeException("Unable to connect to SDB " + domain);

	}

	@Override
	public void addStats(String key, Map<String, String> data) {
		cache.put(key, data);
		if (cache.size() > CACHE_SIZE) {
			flush();
		}
	}

	@Override
	public void flush() {
		if (cache.size() < 1) {
			return;
		}
		BatchPutAttributesRequest req = new BatchPutAttributesRequest();

		List<ReplaceableItem> items = new ArrayList<ReplaceableItem>(CACHE_SIZE);

		for (Map.Entry<String, Map<String, String>> cacheEntry : cache
				.entrySet()) {
			ReplaceableItem entry = new ReplaceableItem(cacheEntry.getKey());
			List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>(
					cacheEntry.getValue().size());
			for (Map.Entry<String, String> dataEntry : cacheEntry.getValue()
					.entrySet()) {
				attributes.add(new ReplaceableAttribute(dataEntry.getKey(),
						dataEntry.getValue(), false));
			}
			entry.setAttributes(attributes);
			items.add(entry);
		}
		req.setDomainName(domain);
		req.setItems(items);

		int tries = 0;
		do {
			tries++;
			try {
				client.batchPutAttributes(req);
				cache.clear();
				return;
			} catch (Exception ase) {
				log.warn(ase);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		} while (tries < MAX_TRIES);
		cache.clear();
		throw new RuntimeException("Unable to connect to SDB " + domain);
	}
}
