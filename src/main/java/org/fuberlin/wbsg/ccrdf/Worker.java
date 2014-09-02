package org.fuberlin.wbsg.ccrdf;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import webreduce.datastructures.DocumentMetadata;
import webreduce.data.Dataset;
import webreduce.extraction.ExtractionAlgorithm;
import webreduce.extraction.StatsKeeper.WebdataStats;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * Worker node implementation, connects to queue, takes file name to process,
 * loads file, extracts tables, and writes extracted tables back to s3 and
 * statistics to simpleDB. If anything goes wrong, the message is not removed
 * from the queue and another node can have a shot.
 */
public class Worker extends ProcessingNode {
	private static Logger log = Logger.getLogger(Worker.class);

	protected final String dataBucket = getOrCry("dataBucket");
	protected final String resultBucket = getOrCry("resultBucket");
	private final int retryLimit = Integer.parseInt(getOrCry("jobRetryLimit"));

	private StatHandler dataStatHandler = null;
	private StatHandler errorStatHandler = null;

	private static class RecordWithOffsetsAndURL {
		public byte[] bytes;
		public long start;
		public long end;
		public String url;

		public RecordWithOffsetsAndURL(byte[] bytes, long start, long end, String url) {
			super();
			this.bytes = bytes;
			this.start = start;
			this.end = end;
			this.url = url;
		}
	}

	public static class WorkerThread extends Thread {
		private static final String WARC_TARGET_URI = "WARC-Target-URI";
		private Timer timer = new Timer();
		int timeLimit = 0;

		public WorkerThread() {
		}

		public WorkerThread(int timeLimitMsec) {
			this.timeLimit = timeLimitMsec;
		}

		public void run() {
			Worker worker = new Worker();
			if (timeLimit < 1) {
				timeLimit = Integer.parseInt(worker.getOrCry("jobTimeLimit")) * 1000;
			}

			while (true) {
				timer = new Timer();
				final WorkerThread t = this;
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						log.warn("Killing worker thread, timeout expired.");
						t.interrupt();
					}
				}, timeLimit);

				String inputFileKey = "";

				try {
					// receive task message from queue
					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
							worker.getQueueUrl())
							.withAttributeNames("ApproximateReceiveCount");
					receiveMessageRequest.setMaxNumberOfMessages(1);
					ReceiveMessageResult queueRes = worker.getQueue()
							.receiveMessage(receiveMessageRequest);
					if (queueRes.getMessages().size() < 1) {
						log.warn("Queue is empty");
						continue;
					}
					Message jobMessage = queueRes.getMessages().get(0);

					/**
					 * messages which went back to the queue more than the
					 * amount of times defined in the configuration entry
					 * "jobRetryLimit" are discarded, probably contain nasty
					 * data we cannot parse.
					 */

					if (Integer.parseInt(jobMessage.getAttributes().get(
							"ApproximateReceiveCount")) > worker.retryLimit) {
						log.warn("Discarding message " + jobMessage.getBody());
						worker.getQueue().deleteMessage(
								new DeleteMessageRequest(worker.getQueueUrl(),
										jobMessage.getReceiptHandle()));

						// store this information in sdb
						Map<String, String> statData = new HashMap<String, String>();
						statData.put("message", "Message Discarded");

						try {
							statData.put("node", InetAddress.getLocalHost()
									.getHostName());
						} catch (UnknownHostException e1) {
							// ignore
						}
						statData.put("file", jobMessage.getBody());
						statData.put("datetime", Calendar.getInstance()
								.getTime().toString());

						worker.getErrorStatHandler().addStats(
								UUID.randomUUID().toString(), statData);
						worker.getErrorStatHandler().flush();
						continue;
					}

					/**
					 * retrieve data file from s3, and unpack it using gzip
					 */
					inputFileKey = jobMessage.getBody();
					log.info("Now working on " + inputFileKey);

					/**
					 * get file from s3 and process with zipped arc.
					 */
					S3Object inputObject = worker.getStorage().getObject(
							worker.dataBucket, inputFileKey);

					/**
					 * Read all page entries from file and run extractor on them
					 */

					log.info("Extracting data from " + inputFileKey + " ...");
					WarcReader warcReader = WarcReaderFactory
							.getReaderCompressed(inputObject
									.getDataInputStream());

					long pagesTotal = 0;
					long pagesErrors = 0;
					long start = System.currentTimeMillis();

					WebdataStats extractionStats = new WebdataStats();
					ExtractionAlgorithm ea = new ExtractionAlgorithm(
							extractionStats, true);

					// read all entries in the ARC file
					RecordWithOffsetsAndURL item;
					item = getNextResponseRecord(warcReader);
					List<Dataset> result = new ArrayList<>();
					while (item != null) {
						List<Dataset> docResult;
						try {
							Document doc;
							DocumentMetadata dm = new DocumentMetadata(
									item.start, item.end, inputFileKey, item.url);
							
							try {
								// try parsing with charset detected from doc
								doc = Jsoup.parse(new ByteArrayInputStream(
										item.bytes), null, "");
								docResult = ea.extract(doc, dm);
							} catch (IllegalCharsetNameException
									| UnsupportedCharsetException e) {
								try {
									// didnt work, try parsing with utf-8 as
									// charset
									doc = Jsoup.parse(new ByteArrayInputStream(
											item.bytes), "UTF-8", "");
									docResult = ea.extract(doc, dm);
								} catch (IllegalCharsetNameException
										| UnsupportedCharsetException e2) {
									// didnt work either, no result
									docResult = new ArrayList<Dataset>();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							docResult = new ArrayList<>();
						}
						// add per-doc result to per-warc-file result
						if (docResult != null)
							result.addAll(docResult);
						pagesTotal++;
						// next record with one retry
						item = getNextResponseRecord(warcReader);
					}
					warcReader.close();

					// upload result to S3
					upload(worker, inputFileKey, result);

					double duration = (System.currentTimeMillis() - start) / 1000.0;
					double rate = (pagesTotal * 1.0) / duration;

					// create data file statistics
					Map<String, String> dataStats = new HashMap<String, String>();
					for (Map.Entry<String, Integer> entry : extractionStats
							.statsAsMap().entrySet()) {
						dataStats.put(entry.getKey(),
								String.valueOf(entry.getValue()));
					}
					dataStats.put("duration", Double.toString(duration));
					dataStats.put("rate", Double.toString(rate));
					dataStats.put("pagesTotal", Long.toString(pagesTotal));
					dataStats.put("pagesErrors", Long.toString(pagesErrors));

					log.info("Extracted data from " + inputFileKey
							+ " - parsed " + pagesTotal + " pages in "
							+ duration + " seconds, " + rate + " pages/sec");

					/**
					 * Create overall statistics for this data file
					 */
					dataStats.put("size",
							Long.toString(inputObject.getContentLength()));
					worker.getDataStatHandler().addStats(inputFileKey,
							dataStats);

					/**
					 * force statistics being persisted
					 */
					worker.getDataStatHandler().flush();

					/**
					 * remove message from queue. If an Exception is thrown or
					 * the node dies before finishing its task, this does not
					 * occur and the message is re-queued for another node
					 */
					worker.getQueue().deleteMessage(
							new DeleteMessageRequest(worker.getQueueUrl(),
									jobMessage.getReceiptHandle()));

					log.info("Finished processing file " + inputFileKey);

				} catch (Exception e) {
					log.warn("Unable to finish processing ("
							+ e.getClass().getSimpleName() + ": "
							+ e.getMessage() + ")");
					e.printStackTrace();
					log.warn("Stracktrace", e.fillInStackTrace());

					// put error information into sdb for later analyis
					Map<String, String> statData = new HashMap<String, String>();
					statData.put("exception", e.getClass().getSimpleName());
					String message = e.getMessage();
					if (message == null) {
						message = e.getClass().getName();
					}
					statData.put("message", message);
					String st = Worker.getStackTrace(e);
					statData.put("stacktrace",
							st.substring(0, Math.min(1024, st.length())));

					try {
						statData.put("node", InetAddress.getLocalHost()
								.getHostName());
					} catch (UnknownHostException e1) {
						// ignore
					}
					statData.put("file", inputFileKey);
					statData.put("datetime", Calendar.getInstance().getTime()
							.toString());

					worker.getErrorStatHandler().addStats(
							UUID.randomUUID().toString(), statData);
					worker.getErrorStatHandler().flush();

				}

				// on failures sleep a bit
				timer.cancel();
			}
		}

		private RecordWithOffsetsAndURL getNextResponseRecord(WarcReader warcReader)
				throws IOException {
			WarcRecord wr;
			while (true) {
				try {
					wr = warcReader.getNextRecord();
				} catch (IOException e) {
					continue;
				}
				if (wr == null)
					return null;

				long offset = warcReader.getStartOffset();
				String type = wr.getHeader("WARC-Type").value;
				if (type.equals("response")) {
					byte[] rawContent = IOUtils.toByteArray(wr
							.getPayloadContent());
					long endOffset = warcReader.getOffset();
					String url = wr.getHeader(WARC_TARGET_URI).value;
					return new RecordWithOffsetsAndURL(rawContent, offset, endOffset, url);
				}
			}
		}

		private void upload(Worker worker, String inputFileKey,
				Iterable<Dataset> results) throws IOException,
				UnknownHostException, S3ServiceException, NoSuchAlgorithmException {

			long threadId = ThreadGuard.currentThread().getId();
			File tmpFile = new File("/tmp/" + threadId
					+ ".tmp");
			FileOutputStream output = new FileOutputStream(tmpFile);
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(
						new GZIPOutputStream(output), "UTF-8"));
				for (Dataset res : results) {
					writer.append(res.toJson());
					writer.append("\n");
				}
			} finally {
				if (writer != null)
					writer.close();
				output.close();
			}
			if (tmpFile.exists()) {
				S3Object outObject = new S3Object(tmpFile);
				outObject.setKey(makeOutputFileKey(inputFileKey));
				worker.getStorage().putObject(worker.resultBucket, outObject);
				tmpFile.delete();
			}
		}

		private String makeOutputFileKey(String inputFileKey) {
			int idx = inputFileKey.indexOf(".warc");
			String s = inputFileKey.substring(0, idx) + ".json.gz";
			return s;
		}
	}

	public StatHandler getDataStatHandler() {
		if (dataStatHandler == null) {
			dataStatHandler = new AmazonStatHandler(getDbClient(),
					getOrCry("sdbdatadomain"));
		}
		return dataStatHandler;
	}

	public StatHandler getErrorStatHandler() {
		if (errorStatHandler == null) {
			errorStatHandler = new AmazonStatHandler(getDbClient(),
					getOrCry("sdberrordomain"));
		}
		return errorStatHandler;
	}

	private static String getStackTrace(Throwable aThrowable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return result.toString();
	}

	public static class ThreadGuard extends Thread {
		private List<Thread> threads = new ArrayList<Thread>();
		private int threadLimit = Runtime.getRuntime().availableProcessors();
		private int threadSerial = 0;
		private int waitTimeSeconds = 1;

		private Class<? extends Thread> threadClass;

		public ThreadGuard(Class<? extends Thread> threadClass) {
			this.threadClass = threadClass;
		}

		public void run() {
			while (true) {
				List<Thread> threadsCopy = new ArrayList<Thread>(threads);
				for (Thread t : threadsCopy) {
					if (!t.isAlive()) {
						log.warn("Thread " + t.getName() + " died.");
						threads.remove(t);
					}
				}
				while (threads.size() < threadLimit) {
					Thread newThread;
					try {
						newThread = threadClass.newInstance();
						newThread.setName("#" + threadSerial);
						threads.add(newThread);
						newThread.start();
						log.info("Started new WorkerThread, "
								+ newThread.getName());
						threadSerial++;
					} catch (Exception e) {
						log.warn("Failed to start new Thread of class "
								+ threadClass);
					}

				}
				try {
					Thread.sleep(waitTimeSeconds * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		new ThreadGuard(WorkerThread.class).start();
	}

}
