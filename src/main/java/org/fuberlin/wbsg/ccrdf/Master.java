package org.fuberlin.wbsg.ccrdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.AllUsersGrantee;
import org.jets3t.service.model.S3Object;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.UnspecifiedParameterException;

public class Master extends ProcessingNode {
	private static final int BATCH_SIZE = 10;
	private static Logger log = Logger.getLogger(Master.class);

	// command line parameters, different actions
	public static void main(String[] args) throws JSAPException {
		// command line parser
		JSAP jsap = new JSAP();
		UnflaggedOption actionParam = new UnflaggedOption("action")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setGreedy(false);

		actionParam
				.setHelp("Action to perform, can be 'queue', 'clearqueue', 'cleardata', 'crawlstats', 'start', 'shutdown', 'deploy', 'retrievedata', 'retrievestats' and 'monitor'");
		jsap.registerParameter(actionParam);

		JSAPResult config = jsap.parse(args);
		String action = config.getString("action");

		// Prefix path of objects to be queued from bucket
		if ("queue".equals(action)) {
			FlaggedOption prefix = new FlaggedOption("prefix")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("bucket-prefix").setShortFlag('p');

			prefix.setHelp("Prefix path of objects to be queued from bucket");
			jsap.registerParameter(prefix);

			FlaggedOption limit = new FlaggedOption("limit")
					.setStringParser(JSAP.LONG_PARSER).setRequired(false)
					.setLongFlag("file-number-limit").setShortFlag('l');

			limit.setHelp("Limits number of objects to be queued from bucket");

			jsap.registerParameter(limit);

			FlaggedOption prefixFile = new FlaggedOption("prefixFile")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("bucket-prefix-file").setShortFlag('f');

			prefixFile
					.setHelp("File including line based prefix paths of objects to be queued from bucket");
			jsap.registerParameter(prefixFile);

			JSAPResult queueResult = jsap.parse(args);
			// if parsing was not successful print usage of commands and exit
			if (!queueResult.success()) {
				printUsageAndExit(jsap, queueResult);
			}
			Long limitValue = null;
			try {
				limitValue = queueResult.getLong("limit");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}
			String filePath = null;
			try {
				filePath = queueResult.getString("prefixFile");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}

			new Master().queue(queueResult.getString("prefix"), limitValue,
					filePath);
			System.exit(0);
		}

		if ("clearqueue".equals(action)) {
			new Master().clearQueue();
			System.exit(0);
		}

		if ("cleardata".equals(action)) {
			Switch s3Deletion = new Switch("includeS3Storage").setLongFlag(
					"includeS3Storage").setShortFlag('r');
			s3Deletion
					.setHelp("Clear all data including extraction data from s3 storage.");
			jsap.registerParameter(s3Deletion);
			JSAPResult queueResult = jsap.parse(args);
			if (!queueResult.success()) {
				printUsageAndExit(jsap, queueResult);
			}
			boolean includeS3Storage = queueResult
					.getBoolean("includeS3Storage");
			new Master().clearData(includeS3Storage);
			System.exit(0);
		}

		if ("monitor".equals(action)) {
			new Master().monitorQueue();
			System.exit(0);
		}

		if ("crawlstats".equals(action)) {
			FlaggedOption prefixP = new FlaggedOption("prefix")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("bucket-prefix").setShortFlag('p');
			prefixP.setHelp("Prefix path of objects in bucket to calculate statistics for");
			jsap.registerParameter(prefixP);

			FlaggedOption outputP = new FlaggedOption("output")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("output-file").setShortFlag('o');
			outputP.setHelp("Path for CSV output file");
			jsap.registerParameter(outputP);

			JSAPResult statsResult = jsap.parse(args);
			if (!statsResult.success()) {
				printUsageAndExit(jsap, statsResult);
			}

			String prefix = statsResult.getString("prefix");
			String output = statsResult.getString("output");

			System.out
					.println("Calculating object count and size statistics for prefix "
							+ prefix + ", saving results to " + output);
			new Master().crawlStats(prefix, new File(output));
			System.out.println("Done.");
			System.exit(0);
		}

		if ("deploy".equals(action)) {
			FlaggedOption jarfileP = new FlaggedOption("jarfile")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("jarfile").setShortFlag('j');
			jarfileP.setHelp("Jarfile to be executed on the worker instances");
			jsap.registerParameter(jarfileP);

			JSAPResult statsResult = jsap.parse(args);
			if (!statsResult.success()) {
				printUsageAndExit(jsap, statsResult);
			}
			File jarfile = new File(statsResult.getString("jarfile"));
			if (!jarfile.exists() || !jarfile.canRead()) {
				log.warn("Unable to access JAR file at " + jarfile);
				System.exit(-1);
			}

			System.out.println("Deploying JAR file at " + jarfile);
			new Master().deploy(jarfile);
			System.exit(0);
		}

		if ("start".equals(action)) {
			FlaggedOption amountP = new FlaggedOption("amount")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(true)
					.setLongFlag("worker-amount").setShortFlag('a');
			amountP.setHelp("Amount of worker instances to start in EC2");
			jsap.registerParameter(amountP);

			FlaggedOption priceP = new FlaggedOption("pricelimit")
					.setStringParser(JSAP.DOUBLE_PARSER).setRequired(true)
					.setLongFlag("pricelimit").setShortFlag('p');
			priceP.setHelp("Price limit for instances in US$");
			jsap.registerParameter(priceP);

			JSAPResult startParams = jsap.parse(args);
			if (!startParams.success()) {
				printUsageAndExit(jsap, startParams);
			}

			int amount = startParams.getInt("amount");
			new Master().createInstances(amount,
					startParams.getDouble("pricelimit"));
			System.out.println("done.");
			System.exit(0);
		}

		if ("shutdown".equals(action)) {
			System.out
					.print("Cancelling spot request and shutting down all worker instances in EC2...");
			new Master().shutdownInstances();
			System.out.println("done.");
			System.exit(0);
		}

		if ("retrievedata".equals(action)) {
			FlaggedOption destinationDir = new FlaggedOption("destination")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("destination").setShortFlag('d');
			destinationDir.setHelp("Directory to write the extracted data to");
			jsap.registerParameter(destinationDir);

			Switch multiThreadMode = new Switch("multiThreadMode").setLongFlag(
					"multiThreadMode").setShortFlag('m');
			multiThreadMode
					.setHelp("Run data retrieve in multithread mode using thread number equal to available number of cores on the system.");
			jsap.registerParameter(multiThreadMode);

			JSAPResult destinationConfig = jsap.parse(args);
			if (!destinationConfig.success()) {
				printUsageAndExit(jsap, destinationConfig);
			}
			File destinationDirectory = new File(
					destinationConfig.getString("destination"));
			boolean useMultiThread = destinationConfig
					.getBoolean("multiThreadMode");

			System.out
					.println("Getting extracted triples from cloud to local disk...");
			Master m = new Master();
			m.retrieveData(destinationDirectory, 100, useMultiThread);

			System.out.println("done.");
			System.exit(0);
		}

		if ("retrievestats".equals(action)) {

			FlaggedOption destinationDir = new FlaggedOption("destination")
					.setStringParser(JSAP.STRING_PARSER).setRequired(true)
					.setLongFlag("destination").setShortFlag('d');
			destinationDir
					.setHelp("Directory to write the extracted statistics to");
			jsap.registerParameter(destinationDir);

			FlaggedOption mountDestinationDir = new FlaggedOption("mount")
					.setStringParser(JSAP.STRING_PARSER).setRequired(false)
					.setLongFlag("mountdestination");
			mountDestinationDir
					.setHelp("Mounted Directory to write the extracted statistics to after writing them local. Local copy is removed.");
			jsap.registerParameter(mountDestinationDir);

			FlaggedOption modeFlag = new FlaggedOption("mode")
					.setStringParser(JSAP.INTEGER_PARSER).setRequired(false)
					.setLongFlag("mode").setShortFlag('m');
			modeFlag.setHelp("Mode for stats retrievement: empty = all stats, 1 = Failes, 2 = Data, 3 = Pages.");
			jsap.registerParameter(modeFlag);

			JSAPResult destinationConfig = jsap.parse(args);
			if (!destinationConfig.success()) {
				printUsageAndExit(jsap, destinationConfig);
			}
			File destinationDirectory = new File(
					destinationConfig.getString("destination"));
			int mode = 4;
			try {
				mode = destinationConfig.getInt("mode");
			} catch (UnspecifiedParameterException e) {
				// do nothing
			}
			File mountDestinationDirectory = null;
			try {
				mountDestinationDirectory = new File(
						destinationConfig.getString("mount"));
			} catch (NullPointerException e) {
				// do nothing
			}

			if (mode < 1 || mode > 4) {
				System.out
						.println("Could not handle mode. Please use 1, 2, 3 or 4.");
				return;
			}

			System.out
					.println("Getting statistics from cloud to local disk...");
			Master m = new Master();
			m.retrieveStats(destinationDirectory, mountDestinationDirectory,
					100, mode);

			System.out.println("done.");
			System.exit(0);
		}

		printUsageAndExit(jsap, config);
	}

	Map<String, OutputStream> outputWriters = new HashMap<String, OutputStream>();
	Map<String, File> outputFiles = new HashMap<String, File>();

	private OutputStream getOutput(String extractor, File outputDir,
			int sizeLimitMb) throws FileNotFoundException, IOException {
		long sizeLimitBytes = sizeLimitMb * 1024 * 1024;
		if (outputFiles.containsKey(extractor)
				&& outputFiles.get(extractor).length() > sizeLimitBytes) {
			outputWriters.get(extractor).write("\n".getBytes());
			outputWriters.get(extractor).close();
			outputFiles.remove(extractor);
			outputWriters.remove(extractor);
		}

		if (!outputWriters.containsKey(extractor)) {
			int suffix = 0;
			File outputFile;
			do {
				outputFile = new File(outputDir + File.separator + "ccrdf."
						+ extractor + "." + suffix + ".nq.gz");
				suffix++;
			} while (outputFile.exists());

			outputFiles.put(extractor, outputFile);
			OutputStream os = new GZIPOutputStream(new FileOutputStream(
					outputFiles.get(extractor)));
			outputWriters.put(extractor, os);
		}
		return outputWriters.get(extractor);
	}

	public void retrieveStats(File destinationDirectory, File mountedDirectory,
			int sizeLimitMb, int mode) {
		if (!destinationDirectory.exists()) {
			destinationDirectory.mkdirs();
		}
		long sizeLimitBytes = sizeLimitMb * 1024 * 1024;

		if (mode == 1 || mode == 4) {
			File failureStatFile = new File(destinationDirectory
					+ File.separator + "failed.csv.gz");
			domainToCSV(getOrCry("sdberrordomain"), failureStatFile);

		}
		if (mode == 2 || mode == 4) {
			File dataStatFile = new File(destinationDirectory + File.separator
					+ "data.csv.gz");
			domainToCSV(getOrCry("sdbdatadomain"), dataStatFile);
		}
		if (mode == 3 || mode == 4) {
			String resultBucket = getOrCry("resultBucket");
			boolean headerWritten = false;

			try {

				S3Object[] objects = getStorage().listObjects(resultBucket,
						"stats/", null);
				int i = 0;
				int fileCount = 0;

				File pageStatFile = null;
				File pageStatMountFile = null;
				OutputStream statsOut = null;

				for (S3Object object : objects) {
					try {
						i++;
						log.info("Retrieving "
								+ object.getKey()
								+ ", ("
								+ i
								+ "/"
								+ objects.length
								+ ") "
								+ CSVExport.humanReadableByteCount(
										object.getContentLength(), false));

						// check if outputfile already reached its limit
						if (pageStatFile == null
								|| pageStatFile.length() > sizeLimitBytes) {
							// close old output stream if open
							if (statsOut != null) {
								statsOut.write("\n".getBytes());
								statsOut.close();
								// copy to mounted file system if destination is
								// given
								if (mountedDirectory != null) {
									pageStatMountFile = new File(
											mountedDirectory + File.separator
													+ pageStatFile.getName());
									try {
										// copy
										copyFile(pageStatFile,
												pageStatMountFile);
										// remove local file
										pageStatFile.delete();
									} catch (Exception e) {
										log.warn("Could not copy file.", e);
									}
								}
							}
							// create new file
							do {
								pageStatFile = new File(destinationDirectory
										+ File.separator + "pages." + fileCount
										+ ".csv.gz");
								fileCount++;
							} while (pageStatFile.exists());
							// create new outputstream
							statsOut = new GZIPOutputStream(
									new FileOutputStream(pageStatFile));
						}

						// now really download the file
						S3Object dataObject = getStorage().getObject(
								resultBucket, object.getKey());

						boolean headerSkipped = false;

						// data file
						if (object.getKey().endsWith(".csv.gz")) {

							BufferedReader retrievedDataReader = new BufferedReader(
									new InputStreamReader(new GZIPInputStream(
											dataObject.getDataInputStream())));

							String line;
							while ((line = retrievedDataReader.readLine()) != null) {
								if (!headerSkipped) {
									headerSkipped = true;
									if (headerWritten) {
										continue;
									}
									headerWritten = true;
								}
								if (line.split(",").length < 20) {
									continue;
								}
								statsOut.write(line.getBytes());

								statsOut.write("\n".getBytes());
							}
							retrievedDataReader.close();

						}
					} catch (Exception e) {
						log.warn("Error in " + object.getKey(), e);
					}
				}

				statsOut.close();
				// given
				if (mountedDirectory != null) {
					pageStatMountFile = new File(mountedDirectory
							+ File.separator + pageStatFile.getName());
					try {
						// copy
						copyFile(pageStatFile, pageStatMountFile);
						// remove local file
						pageStatFile.delete();
					} catch (Exception e) {
						log.warn("Could not copy file.", e);
					}
				}

			} catch (Exception e) {
				log.warn("Error: ", e);
			}
		}
	}

	private static void copyFile(File sourceFile, File destFile)
			throws IOException {
		if (!destFile.exists()) {
			destFile.createNewFile();
		}

		FileChannel source = null;
		FileChannel destination = null;
		FileInputStream fileInputStream = null;
		FileOutputStream fileOutputStream = null;
		try {
			fileInputStream = new FileInputStream(sourceFile);
			source = fileInputStream.getChannel();
			fileOutputStream = new FileOutputStream(destFile);
			destination = fileOutputStream.getChannel();
			long count = 0;
			long size = source.size();
			while ((count += destination.transferFrom(source, count, size
					- count)) < size)
				;
		} catch (IOException e) {
			log.error("Error while trying to transform data.", e);
		} finally {
			if (source != null) {
				source.close();
			}
			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (fileOutputStream != null) {
				fileOutputStream.close();
			}
			if (destination != null) {
				destination.close();
			}
		}
	}

	private static final int MIN_RESULTS = 5;

	private void domainToCSV(String domainPrefix, File csvFile) {
		log.info("Storing data from SDB domains starting with " + domainPrefix
				+ " to file " + csvFile);
		Set<String> attributes = null;

		List<String> domains = getDbClient().listDomains().getDomainNames();
		int c = 0;
		for (String domainName : domains) {
			if (domainName.startsWith(domainPrefix)) {
				c++;
				log.info("Exporting from " + domainName + " (" + c + "/"
						+ domains.size() + ")");
				long domainCount = getDbClient().domainMetadata(
						new DomainMetadataRequest(domainName)).getItemCount();
				if (domainCount < MIN_RESULTS) {
					log.info("Ignoring " + domainName + ", less than "
							+ MIN_RESULTS + " entries.");
					continue;
				}
				if (attributes == null) {
					attributes = getSdbAttributes(getDbClient(), domainName,
							MIN_RESULTS);
				}
				long total = 0;
				String select = "select * from `" + domainName + "` limit 2500";
				String nextToken = null;
				SelectResult res;
				do {
					res = getDbClient().select(
							new SelectRequest(select).withNextToken(nextToken)
									.withConsistentRead(false));

					for (Item i : res.getItems()) {
						Map<String, Object> csvEntry = new HashMap<String, Object>();
						csvEntry.put("_key", i.getName());
						for (String attr : attributes) {
							csvEntry.put(attr, "");
						}

						for (Attribute a : i.getAttributes()) {
							csvEntry.put(a.getName(), a.getValue());
						}
						CSVExport.writeToFile(csvEntry, csvFile);
					}
					nextToken = res.getNextToken();
					total += res.getItems().size();
					log.info("Exported " + total + " of " + domainCount);
				} while (nextToken != null);
				log.info("Finished exporting from " + domainName);

			}
		}
		CSVExport.closeWriter(csvFile);
	}

	private static Set<String> getSdbAttributes(AmazonSimpleDBClient client,
			String domainName, int sampleSize) {
		if (!client.listDomains().getDomainNames().contains(domainName)) {
			throw new IllegalArgumentException("SimpleDB domain '" + domainName
					+ "' not accessible from given client instance");
		}

		int domainCount = client.domainMetadata(
				new DomainMetadataRequest(domainName)).getItemCount();
		if (domainCount < sampleSize) {
			throw new IllegalArgumentException("SimpleDB domain '" + domainName
					+ "' does not have enough entries for accurate sampling.");
		}

		int avgSkipCount = domainCount / sampleSize;
		int processedCount = 0;
		String nextToken = null;
		Set<String> attributeNames = new HashSet<String>();
		Random r = new Random();
		do {
			int nextSkipCount = r.nextInt(avgSkipCount * 2) + 1;

			SelectResult countResponse = client.select(new SelectRequest(
					"select count(*) from `" + domainName + "` limit "
							+ nextSkipCount).withNextToken(nextToken));

			nextToken = countResponse.getNextToken();

			processedCount += Integer.parseInt(countResponse.getItems().get(0)
					.getAttributes().get(0).getValue());

			SelectResult getResponse = client.select(new SelectRequest(
					"select * from `" + domainName + "` limit 1")
					.withNextToken(nextToken));

			nextToken = getResponse.getNextToken();

			processedCount++;

			if (getResponse.getItems().size() > 0) {
				for (Attribute a : getResponse.getItems().get(0)
						.getAttributes()) {
					attributeNames.add(a.getName());
				}
			}
		} while (domainCount > processedCount);
		return attributeNames;
	}

	public void retrieveData(File dataDir, int sizeLimitMb,
			boolean runInMultiThreadMode) {
		if (!runInMultiThreadMode) {
			// run retrieve with one thread
			dataDir.mkdirs();

			String resultBucket = getOrCry("resultBucket");

			try {
				S3Object[] objects = getStorage().listObjects(resultBucket,
						"data/", null);
				int i = 0;
				for (S3Object object : objects) {
					try {
						i++;
						log.info("Retrieving "
								+ object.getKey()
								+ ", ("
								+ i
								+ "/"
								+ objects.length
								+ ") "
								+ CSVExport.humanReadableByteCount(
										object.getContentLength(), false));

						// now really download the file
						S3Object dataObject = getStorage().getObject(
								resultBucket, object.getKey());

						// data file
						if (object.getKey().endsWith(".nq.gz")) {

							BufferedReader retrievedDataReader = new BufferedReader(
									new InputStreamReader(new GZIPInputStream(
											dataObject.getDataInputStream())));

							String line;
							while ((line = retrievedDataReader.readLine()) != null) {
								Line l = parseLine(line);
								if (l == null) {
									continue;
								}
								OutputStream out = getOutput(l.extractor,
										dataDir, sizeLimitMb);
								out.write(new String(l.quad + "\n").getBytes());
							}
							retrievedDataReader.close();

						}
					} catch (Exception e) {
						log.warn("Error in " + object.getKey(), e);
					}
				}

				for (OutputStream os : outputWriters.values()) {
					if (os != null) {
						os.write("\n".getBytes());
						os.close();
					}
				}

			} catch (Exception e) {
				log.warn("Error: ", e);
			}
		} else {
			// run retrieve with multiple threads
			Thread t = new DataThreadHandler(dataDir, sizeLimitMb);
			t.start();
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private class DataThreadHandler extends Thread implements Observer {
		private File dataDir;
		private int sizeLimitMb;
		private int threads = 0;

		private DataThreadHandler(File dataDir, int sizeLimitMb) {
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
		}

		@Override
		public void run() {
			dataDir.mkdirs();

			String resultBucket = getOrCry("resultBucket");

			int threadLimit = Runtime.getRuntime().availableProcessors();
			try {
				S3Object[] objects = getStorage().listObjects(resultBucket,
						"data/", null);
				int i = 0;

				for (S3Object object : objects) {
					// check if there are already as many threads as cpu cores
					while (threads > threadLimit) {
						Thread.sleep(50);
					}
					i++;
					// create a thread that handles this object
					DataThread dt = new DataThread(object, i, dataDir,
							sizeLimitMb, objects.length, resultBucket);
					dt.addObserver(this);
					Thread t = new Thread(dt);

					t.start();
					threads++;
				}

				// wait till all threads are finished
				while (threads > 0) {
					Thread.sleep(1000);
				}

				for (OutputStream os : outputWriters.values()) {
					if (os != null) {
						os.write("\n".getBytes());
						os.close();
					}
				}

			} catch (Exception e) {
				log.warn("Error: ", e);
			}
		}

		@Override
		public void update(Observable arg0, Object arg1) {
			threads--;
		}
	}

	private class DataThread extends Observable implements Runnable {
		private S3Object object;
		private int i;
		private File dataDir;
		private int sizeLimitMb;
		private int length;
		private String resultBucket;

		private DataThread(S3Object object, int i, File dataDir,
				int sizeLimitMb, int length, String resultBucket) {
			this.object = object;
			this.i = i;
			this.dataDir = dataDir;
			this.sizeLimitMb = sizeLimitMb;
			this.length = length;
			this.resultBucket = resultBucket;
		}

		@Override
		public void run() {
			try {
				log.info("Retrieving "
						+ object.getKey()
						+ ", ("
						+ i
						+ "/"
						+ length
						+ ") "
						+ CSVExport.humanReadableByteCount(
								object.getContentLength(), false));

				// now really download the file
				S3Object dataObject = getStorage().getObject(resultBucket,
						object.getKey());

				// data file
				if (object.getKey().endsWith(".nq.gz")) {

					BufferedReader retrievedDataReader = new BufferedReader(
							new InputStreamReader(new GZIPInputStream(
									dataObject.getDataInputStream())));

					String line;
					while ((line = retrievedDataReader.readLine()) != null) {
						Line l = parseLine(line);
						if (l == null) {
							continue;
						}
						OutputStream out = getOutput(l.extractor, dataDir,
								sizeLimitMb);
						out.write(new String(l.quad + "\n").getBytes());
					}
					retrievedDataReader.close();
				}
			} catch (Exception e) {
				log.warn("Error in " + object.getKey(), e);
			} finally {
				setChanged();
				notifyObservers();
			}
		}

	}

	public static class Line {
		private String quad;
		private String extractor;
	}

	public static final Line parseLine(String line) {
		StringTokenizer t = new StringTokenizer(line, " ");
		// second to last element is extractor name
		// last element is "."

		Line l = new Line();
		while (t.hasMoreTokens()) {
			String entry = t.nextToken();
			if (entry.startsWith("<ex:")) {
				l.extractor = entry.replace("<ex:", "").replace(">", "");
				l.quad = line.replace(entry, "");
				return l;
			}
		}
		log.warn("Unable to parse " + line);
		return null;
	}

	private static void printUsageAndExit(JSAP jsap, JSAPResult result) {
		@SuppressWarnings("rawtypes")
		Iterator it = result.getErrorMessageIterator();
		while (it.hasNext()) {
			System.err.println("Error: " + it.next());
		}

		System.err.println("Usage: " + Master.class.getName() + " "
				+ jsap.getUsage());
		System.err.println(jsap.getHelp());
		System.err
				.println("General Usage: \n1) Create a CC extractor JAR file (mvn install)\n2) Use 'deploy' command to upload the JAR to S3\n3) Use 'queue' command to fill the extraction queue with CC file names\n4) Use 'start' command to launch EC2 extraction instances\n5) Wait until everything is finished using the 'monitor' command\n6) Use 'shutdown' command to kill worker nodes\n7) Collect result data and statistics with the 'retrievedata' and 'retrievestats' commands");

		System.exit(1);
	}

	public void crawlStats(String prefix, File statFile) {
		String dataBucket = getOrCry("dataBucket");

		Map<String, Long> fileCount = new HashMap<String, Long>();
		Map<String, Long> fileSize = new HashMap<String, Long>();

		try {

			for (S3Object object : getStorage().listObjects(dataBucket, prefix,
					null)) {
				if (!object.getKey().endsWith(DATA_SUFFIX)) {
					continue;
				}
				List<Permission> permissions = object.getAcl()
						.getPermissionsForGrantee(new AllUsersGrantee());

				if (permissions == null
						|| !permissions.contains(Permission.PERMISSION_READ)) {
					log.warn("Unable to access " + object.getKey());
				}
				Long size = Long.parseLong((String) object
						.getMetadata(S3Object.METADATA_HEADER_CONTENT_LENGTH));
				StringTokenizer st = new StringTokenizer(object.getKey(), "/");
				String statKey = "";
				while (st.hasMoreTokens()) {
					statKey += st.nextToken() + "/";
					if (!st.hasMoreTokens()) {
						// we are at the last token, this is the filename, we no
						// want to see this in our statistics
						break;
					}
					Long oldCount = 0L;
					Long oldSize = 0L;

					if (fileCount.containsKey(statKey)) {
						oldCount = fileCount.get(statKey);
					}
					if (fileSize.containsKey(statKey)) {
						oldSize = fileSize.get(statKey);
					}

					fileCount.put(statKey, oldCount + 1);
					fileSize.put(statKey, oldSize + size);
				}
			}

		} catch (S3ServiceException e) {
			log.warn(e);
		}
		List<String> keys = new ArrayList<String>(fileCount.keySet());
		Collections.sort(keys);
		for (String key : keys) {

			Map<String, Object> statEntry = new HashMap<String, Object>();
			statEntry.put("bucket", dataBucket);
			statEntry.put("key", key);
			statEntry.put("files", fileCount.get(key));
			statEntry.put("size", fileSize.get(key));
			statEntry.put("sizep",
					CSVExport.humanReadableByteCount(fileSize.get(key), false));
			log.info(statEntry);
			CSVExport.writeToFile(statEntry, statFile);
		}
		CSVExport.closeWriter(statFile);

	}

	public void monitorQueue() {
		System.out
				.println("Monitoring job queue, extraction rate and running instances.");
		System.out.println();

		List<DateSizeRecord> sizeLog = new ArrayList<DateSizeRecord>();
		DecimalFormat twoDForm = new DecimalFormat("#.##");

		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		while (true) {
			try {
				DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
				DescribeSpotInstanceRequestsResult describeResult = ec2
						.describeSpotInstanceRequests(describeRequest);
				List<SpotInstanceRequest> describeResponses = describeResult
						.getSpotInstanceRequests();

				int requestedInstances = 0;
				int runningInstances = 0;
				for (SpotInstanceRequest describeResponse : describeResponses) {
					if ("active".equals(describeResponse.getState())) {
						runningInstances++;
						requestedInstances++;
					}
					if ("open".equals(describeResponse.getState())) {
						requestedInstances++;
					}
				}

				// get queue attributes
				GetQueueAttributesResult res = getQueue().getQueueAttributes(
						new GetQueueAttributesRequest(getQueueUrl())
								.withAttributeNames("All"));
				Long queueSize = Long.parseLong(res.getAttributes().get(
						"ApproximateNumberOfMessages"));
				Long inflightSize = Long.parseLong(res.getAttributes().get(
						"ApproximateNumberOfMessagesNotVisible"));

				// add the new value to the tail, now remove too old stuff from
				// the
				// head
				DateSizeRecord nowRecord = new DateSizeRecord(Calendar
						.getInstance().getTime(), queueSize + inflightSize);
				sizeLog.add(nowRecord);

				int windowSizeSec = 120;

				// remove outdated entries
				for (DateSizeRecord rec : new ArrayList<DateSizeRecord>(sizeLog)) {
					if (nowRecord.recordTime.getTime()
							- rec.recordTime.getTime() > windowSizeSec * 1000) {
						sizeLog.remove(rec);
					}
				}
				// now the first entry is the first data point, and the entry
				// just
				// added the last;
				DateSizeRecord compareRecord = sizeLog.get(0);
				double timeDiffSec = (nowRecord.recordTime.getTime() - compareRecord.recordTime
						.getTime()) / 1000;
				long sizeDiff = compareRecord.queueSize - nowRecord.queueSize;

				double rate = sizeDiff / timeDiffSec;

				System.out.print('\r');

				if (rate > 0) {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), R: " + twoDForm.format(rate * 60)
							+ " m/min, ETA: "
							+ twoDForm.format((queueSize / rate) / 3600)
							+ " h, N: " + runningInstances + "/"
							+ requestedInstances + "          ");
				} else {
					System.out.print("Q: " + queueSize + " (" + inflightSize
							+ "), N: " + runningInstances + "/"
							+ requestedInstances
							+ "                          	");
				}

			} catch (AmazonServiceException e) {
				System.out.print("\r! // ");
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// who cares if we get interrupted here
			}
		}
	}

	private String getJarUrl() {
		return "http://s3.amazonaws.com/" + getOrCry("deployBucket") + "/"
				+ getOrCry("deployFilename");
	}

	/**
	 * NOTICE: This is a startup shell script for EC2 instances. It installs
	 * Java, downloads the Extractor JAR from S3 and launches it.
	 *
	 * This is designed to work on the Ubuntu AMI
	 * "Ubuntu 11.10 Oneiric instance-store" from www.alestic.com see
	 * http://alestic.com/2009/06/ec2-user-data-scripts
	 * */

	private final String startupScript = "#!/bin/bash \n echo 1 > /proc/sys/vm/overcommit_memory \n aptitude update \n aptitude -y install openjdk-7-jre-headless htop \n wget -O /tmp/start.jar \""
			+ getJarUrl()
			+ "\" \n java -Xmx"
			+ getOrCry("javamemory").trim()
			+ " -jar /tmp/start.jar > /tmp/start.log & \n";

	public void createInstances(int count, double priceLimitDollars) {
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		log.info("Requesting " + count + " instances of type "
				+ getOrCry("ec2instancetype") + " with price limit of "
				+ priceLimitDollars + " US$");
		log.debug(startupScript);

		try {
			// our bid
			RequestSpotInstancesRequest runInstancesRequest = new RequestSpotInstancesRequest()
					.withSpotPrice(Double.toString(priceLimitDollars))
					.withInstanceCount(count).withType("persistent");

			// increase volume size
			// BlockDeviceMapping mapping = new BlockDeviceMapping()
			// .withDeviceName("/dev/sda1").withEbs(
			// new EbsBlockDevice().withVolumeSize(Integer
			// .parseInt(getOrCry("ec2disksize"))));

			// what we want
			LaunchSpecification workerSpec = new LaunchSpecification()
					.withInstanceType(getOrCry("ec2instancetype"))
					.withImageId(getOrCry("ec2ami"))
					.withKeyName(getOrCry("ec2keypair"))
					// .withBlockDeviceMappings(mapping)
					.withUserData(
							new String(Base64.encodeBase64(startupScript
									.getBytes())));

			runInstancesRequest.setLaunchSpecification(workerSpec);

			// place the request
			ec2.requestSpotInstances(runInstancesRequest);
			log.info("Request placed, now use 'monitor' to check how many instances are running. Use 'shutdown' to cancel the request and terminate the corresponding instances.");
		} catch (Exception e) {
			log.warn("Failed to start instances - ", e);
		}
	}

	public void monitorCPUUsage() {
		AmazonCloudWatchClient cloudClient = new AmazonCloudWatchClient(
				getAwsCredentials());
		GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
		Calendar cal = Calendar.getInstance();
		request.setEndTime(cal.getTime());
		cal.add(Calendar.MINUTE, -5);
		request.setStartTime(cal.getTime());
		request.setNamespace("AWS/EC2");
		List<String> statistics = new ArrayList<String>();
		statistics.add("Maximium");
		statistics.add("Average");
		request.setStatistics(statistics);
		request.setMetricName("CPUUtilization");
		request.setPeriod(300);
		Dimension dimension = new Dimension();
		dimension.setName("InstanceId");
		dimension.setValue("i-d93fa2a4");
		List<Dimension> dimensions = new ArrayList<Dimension>();
		dimensions.add(dimension);
		request.setDimensions(dimensions);
		GetMetricStatisticsResult result = cloudClient
				.getMetricStatistics(request);
		List<Datapoint> dataPoints = result.getDatapoints();
		for (Datapoint dataPoint : dataPoints) {
			System.out.println(dataPoint.getAverage());
		}

	}

	public void shutdownInstances() {
		AmazonEC2 ec2 = new AmazonEC2Client(getAwsCredentials());
		ec2.setEndpoint(getOrCry("ec2endpoint"));

		try {
			// cancel spot request, so no new instances will be launched
			DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest();
			DescribeSpotInstanceRequestsResult describeResult = ec2
					.describeSpotInstanceRequests(describeRequest);
			List<SpotInstanceRequest> describeResponses = describeResult
					.getSpotInstanceRequests();
			List<String> spotRequestIds = new ArrayList<String>();
			List<String> instanceIds = new ArrayList<String>();

			for (SpotInstanceRequest describeResponse : describeResponses) {
				spotRequestIds.add(describeResponse.getSpotInstanceRequestId());
				if ("active".equals(describeResponse.getState())) {
					instanceIds.add(describeResponse.getInstanceId());
				}
			}
			ec2.cancelSpotInstanceRequests(new CancelSpotInstanceRequestsRequest()
					.withSpotInstanceRequestIds(spotRequestIds));
			log.info("Cancelled spot request");

			if (instanceIds.size() > 0) {
				ec2.terminateInstances(new TerminateInstancesRequest(
						instanceIds));
				log.info("Shut down " + instanceIds.size() + " instances");
			}

		} catch (Exception e) {
			log.warn("Failed to shutdown instances - ", e);
		}
	}

	public void deploy(File jarFile) {
		String deployBucket = getOrCry("deployBucket");
		String deployFilename = getOrCry("deployFilename");

		try {
			getStorage().getOrCreateBucket(deployBucket);
			AccessControlList bucketAcl = getStorage().getBucketAcl(
					deployBucket);
			bucketAcl.grantPermission(GroupGrantee.ALL_USERS,
					Permission.PERMISSION_READ);

			S3Object statFileObject = new S3Object(jarFile);
			statFileObject.setKey(deployFilename);
			statFileObject.setAcl(bucketAcl);

			getStorage().putObject(deployBucket, statFileObject);

			log.info("File " + jarFile + " now accessible at " + getJarUrl());
		} catch (Exception e) {
			log.warn("Failed to deploy or set permissions in bucket  "
					+ deployBucket + ", key " + deployFilename, e);
		}
	}

	private class DateSizeRecord {
		Date recordTime;
		Long queueSize;

		public DateSizeRecord(Date time, Long size) {
			this.recordTime = time;
			this.queueSize = size;
		}
	}

	public void clearQueue() {
		deleteQueue();
		log.info("Deleted job queue");
	}

	public void clearData(boolean includeS3Storage) {
		ExecutorService ex = Executors.newFixedThreadPool(100);
		final AmazonSimpleDBClient client = getDbClient();
		String nextToken = null;
		long domainCount = 0;
		List<String> domains = new ArrayList<String>();
		do {
			ListDomainsResult res = client.listDomains(new ListDomainsRequest()
					.withNextToken(nextToken));
			nextToken = res.getNextToken();
			domains.addAll(res.getDomainNames());
			domainCount += domains.size();

		} while (nextToken != null);

		log.info(domainCount + " domains");

		for (final String domain : domains) {
			ex.submit(new Thread() {
				public void run() {
					client.deleteDomain(new DeleteDomainRequest(domain));
					log.info("Deleted " + domain);
				}
			});

		}

		ex.shutdown();
		try {
			ex.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (includeS3Storage) {
			log.info("Cleaning all data including s3 storage.");
			String resultBucket = getOrCry("resultBucket");
			try {
				for (S3Object object : getStorage().listObjects(resultBucket)) {
					if (object.getKey() != null
							&& (object.getKey().startsWith("data/") || object
									.getKey().startsWith("stats/"))) {
						log.info("Removing s3 object: " + object.getKey()
								+ " in bucket " + object.getBucketName());
						getStorage()
								.deleteObject(resultBucket, object.getKey());
					}
				}
			} catch (S3ServiceException e) {
				log.warn(e, e.fillInStackTrace());
			} catch (ServiceException e) {
				log.warn(e, e.fillInStackTrace());
			}
		}

		/*
		 * getDbClient().deleteDomain( new
		 * DeleteDomainRequest(getOrCry("sdbdatadomain")));
		 *
		 * getDbClient().deleteDomain( new
		 * DeleteDomainRequest(getOrCry("sdberrordomain")));
		 *
		 * String resultBucket = getOrCry("resultBucket");
		 *
		 * try { for (S3Object object : getStorage().listObjects(resultBucket,
		 * "", null)) { getStorage().deleteObject(resultBucket,
		 * object.getKey()); } } catch (Exception e) {
		 * log.warn("Unable to clear data files", e); }
		 */
		log.info("Deleted statistics and intermediate data");
	}

	public void queue(String singlePrefix, Long limit, String filePath) {

		String dataBucket = getOrCry("dataBucket");

		Set<String> prefixes = new HashSet<String>();
		if (filePath == null) {
			if (singlePrefix == null || singlePrefix.trim().equals("")) {
				log.warn("No prefix given");
				return;
			}
			prefixes.add(singlePrefix);
			log.info("Queuing all keys from bucket " + dataBucket
					+ " with prefix " + singlePrefix);
		} else {

			try {
				FileReader fis = new FileReader(new File(filePath));
				BufferedReader br = new BufferedReader(fis);
				while (br.ready()) {
					String line = br.readLine();
					if (line != null && line.trim().length() > 0) {
						prefixes.add(line.trim());
					}
				}
				br.close();
			} catch (FileNotFoundException e) {
				log.warn("Could not find file.");
				log.debug(e);
			} catch (IOException e) {
				log.warn("Could not access file.");
				log.debug(e);
			}

			log.info("Queuing all keys from bucket " + dataBucket
					+ " with prefixes included in " + filePath);
		}
		if (prefixes.size() < 1) {
			log.warn("No prefixes included");
			return;
		}
		if (limit != null) {
			log.info("Setting limit of files to: " + limit);
		} else {
			log.info("Selecting all included files.");
		}
		long globalQueued = 0;
		for (String prefix : prefixes) {
			try {
				prefix = getOrCry("dataPrefix") + "/" + prefix;
				long objectsQueued = 0;
				SendMessageBatchRequest smbr = new SendMessageBatchRequest(
						getQueueUrl());
				smbr.setEntries(new ArrayList<SendMessageBatchRequestEntry>());

				for (S3Object object : getStorage().listObjects(dataBucket,
						prefix, null)) {
					// if limit is set and number of queued objects reached
					// limit,
					// stop queuing
					if (limit != null && globalQueued >= limit) {
						break;
					}
					if (!object.getKey().endsWith(DATA_SUFFIX)) {
						continue;
					}
					SendMessageBatchRequestEntry smbre = new SendMessageBatchRequestEntry();
					smbre.setMessageBody(object.getKey());
					smbre.setId("task_" + objectsQueued);
					smbr.getEntries().add(smbre);
					if (smbr.getEntries().size() >= BATCH_SIZE) {
						getQueue().sendMessageBatch(smbr);
						// having send into queue - reset entries.
						smbr.setEntries(new ArrayList<SendMessageBatchRequestEntry>());
					}
					objectsQueued++;
					globalQueued++;
				}
				// send the rest
				if (smbr.getEntries().size() > 0) {
					getQueue().sendMessageBatch(smbr);
				}
				log.info("Queued " + objectsQueued + " objects for prefix "
						+ prefix);
			} catch (Exception e) {
				log.warn("Failed to queue objects in bucket " + dataBucket
						+ " with prefix " + prefix, e);
			}
		}
		log.info("Queued " + globalQueued + " objects for all given prefixes.");
	}
}
