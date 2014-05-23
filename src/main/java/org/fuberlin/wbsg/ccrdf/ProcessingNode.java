package org.fuberlin.wbsg.ccrdf;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

public abstract class ProcessingNode {
	private static Logger log = Logger.getLogger(ProcessingNode.class);

	private static final String PFILENAME = "/webreduce.properties";
	public static final String DATA_SUFFIX = ".warc.gz";

	private Properties config = null;

	private AmazonSimpleDBClient sdb = null;

	private RestS3Service s3 = null;

	private AmazonSQS sqs = null;

	String queueUrl = null;

	protected Properties config() {
		if (config == null) {
			config = loadConfig(PFILENAME);
		}
		return config;
	}

	private static Properties loadConfig(String f) {
		Properties p = new Properties();
		InputStream pStream = ProcessingNode.class
				.getResourceAsStream(PFILENAME);
		if (pStream == null) {
			log.warn("Unable to find property file " + PFILENAME);
			return p;
		}
		try {
			p.load(pStream);
		} catch (IOException e) {
			log.warn("Unable to load property file " + PFILENAME);
		}
		return p;
	}

	/**
	 * Returns property value for a certain key.
	 *
	 * @param key
	 * @return property value, empty string if property could not be found
	 * @throws IllegalArgumentException
	 *             if no key is set.
	 */
	public String getOrCry(String key) {
		if (key == null || key.trim().equals("")) {
			throw new IllegalArgumentException(
					"No key given for config lookup!");
		}
		String value = config().getProperty(key);
		if (value == null || value.trim().equals("")) {
			log.warn("Value not found in configuration for key " + key);
			return "";
		}
		return value.trim();
	}

	protected AmazonSimpleDBClient getDbClient() {
		if (sdb == null) {
			sdb = new AmazonSimpleDBClient(getAwsCredentials());
		}
		return sdb;
	}

	public void setDbClient(AmazonSimpleDBClient client) {
		sdb = client;
	}

	protected AWSCredentials getAwsCredentials() {
		return new AWSCredentials() {
			public String getAWSAccessKeyId() {
				return config().getProperty("awsAccessKey");
			}

			public String getAWSSecretKey() {
				return config().getProperty("awsSecretKey");
			}
		};
	}

	protected org.jets3t.service.security.AWSCredentials getJetS3tCredentials() {
		AWSCredentials cred = getAwsCredentials();
		return new org.jets3t.service.security.AWSCredentials(
				cred.getAWSAccessKeyId(), cred.getAWSSecretKey());
	}

	protected AmazonSQS getQueue() {
		if (sqs == null) {
			sqs = new AmazonSQSClient(getAwsCredentials());
			sqs.setEndpoint(config().getProperty("queueEndpoint"));
		}
		return sqs;
	}

	public void setQueue(AmazonSQS q) {
		sqs = q;
	}

	protected RestS3Service getStorage() {
		if (s3 == null) {
			try {
				s3 = new RestS3Service(getJetS3tCredentials());

			} catch (S3ServiceException e1) {
				log.warn("Unable to connect to S3", e1);
			}
		}
		return s3;
	}

	public void setStorage(RestS3Service s) {
		s3 = s;
	}

	protected String getQueueUrl() {
		if (queueUrl == null) {
			String jobQueueName = config().getProperty("jobQueueName");
			if (jobQueueName == null || jobQueueName.trim().equals("")) {
				log.warn("No job queue given");
				return "";
			}
			try {
				GetQueueUrlResult res = getQueue().getQueueUrl(
						new GetQueueUrlRequest(jobQueueName));
				queueUrl = res.getQueueUrl();

				// create the queue if it should be missing for some reason
			} catch (AmazonServiceException e) {
				if (e.getErrorCode().equals(
						"AWS.SimpleQueueService.NonExistentQueue")) {

					CreateQueueRequest req = new CreateQueueRequest();
					req.setQueueName(jobQueueName);

					Map<String, String> qattr = new HashMap<String, String>();
					qattr.put("DelaySeconds", "0");

					// TODO: make these values configurable?
					qattr.put("MessageRetentionPeriod", "1209600");
					qattr.put("VisibilityTimeout", getOrCry("jobTimeLimit"));
					req.setAttributes(qattr);

					CreateQueueResult res = getQueue().createQueue(req);
					queueUrl = res.getQueueUrl();
				}
			}
		}
		return queueUrl;
	}

	public void setQueueUrl(String u) {
		queueUrl = u;
	}

	protected void deleteQueue() {
		getQueue().deleteQueue(new DeleteQueueRequest(getQueueUrl()));
		// set the url to null so the queue can be re-created
		queueUrl = null;
	}
}
