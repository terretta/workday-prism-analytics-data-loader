/*
 * =========================================================================================
 * Copyright (c) 2018 Workday, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Contributors:
 * Each Contributor (“You”) represents that such You are legally entitled to submit any 
 * Contributions in accordance with these terms and by posting a Contribution, you represent
 * that each of Your Contribution is Your original creation.   
 *
 * You are not expected to provide support for Your Contributions, except to the extent You 
 * desire to provide support. You may provide support for free, for a fee, or not at all. 
 * Unless required by applicable law or agreed to in writing, You provide Your Contributions 
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied, including, without limitation, any warranties or conditions of TITLE, 
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * =========================================================================================
 */
package com.wday.prism.dataset.file.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.client.ClientProtocolException;

import com.wday.prism.dataset.api.DataAPIConsumer;
import com.wday.prism.dataset.api.types.DatasetType;
import com.wday.prism.dataset.api.types.GetBucketResponseType;
import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.monitor.ThreadContext;
import com.wday.prism.dataset.util.CSVReader;
import com.wday.prism.dataset.util.CharsetChecker;

/**
 * The Class DatasetLoader.
 */
public class DatasetLoader {

	private static final long maxWaitTime = 5 * 60 * 1000L;

	/** The Constant nf. */
	public static final NumberFormat nf = NumberFormat.getIntegerInstance();

	/** The Constant logformat. */
	static final SimpleDateFormat logformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS zzz");
	static {
		logformat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	/**
	 * @param tenantURL
	 * @param apiVersion
	 * @param tenantName
	 * @param accessToken
	 * @param inputFileString
	 * @param schemaFileString
	 * @param uploadFormat
	 * @param codingErrorAction
	 * @param inputFileCharset
	 * @param datasetAlias
	 * @param datasetLabel
	 * @param operation
	 * @param logger
	 * @param createDataset
	 * @throws DatasetLoaderException
	 */
	public static boolean uploadDataset(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String inputFileString, String schemaFileString, String uploadFormat, CodingErrorAction codingErrorAction,
			Charset inputFileCharset, String datasetAlias, String datasetLabel, String operation, PrintStream logger,
			boolean createDataset) throws DatasetLoaderException {
		File archiveDir = null;
		File datasetArchiveDir = null;
		File bucketDir = null;
		File inputFile = null;
		File schemaFile = null;
		boolean status = true;
		long uploadTime = 0L;

		ThreadContext tx = ThreadContext.get();
		Session session = tx.getSession();

		// we only want a small capacity otherwise the reader thread will run away
		BlockingQueue<List<String>> q = new LinkedBlockingQueue<List<String>>(10);

		if (uploadFormat == null || uploadFormat.trim().isEmpty())
			uploadFormat = "binary";

		if (codingErrorAction == null)
			codingErrorAction = CodingErrorAction.REPORT;

		if (logger == null)
			logger = System.out;

		if (inputFileCharset == null) {
			Charset tmp = null;
			try {
				inputFile = new File(inputFileString);
				if (inputFile.exists() && inputFile.length() > 0) {
					tmp = CharsetChecker.detectCharset(inputFile, logger);
				}
			} catch (Exception e) {
			}

			if (tmp != null) {
				inputFileCharset = tmp;
			} else {
				inputFileCharset = Charset.forName("UTF-8");
			}
		}

		if (operation == null) {
			operation = "Replace";
		}

		if (datasetLabel == null || datasetLabel.trim().isEmpty()) {
			datasetLabel = datasetAlias;
		}

		Runtime rt = Runtime.getRuntime();
		long mb = 1024 * 1024;

		logger.println("\n*******************************************************************************");
		logger.println("Start Timestamp: " + logformat.format(new Date()));
		logger.println("inputFile: " + inputFileString);
		logger.println("schemaFile: " + schemaFileString);
		logger.println("inputFileCharset: " + inputFileCharset);
		logger.println("dataset: " + datasetAlias);
		logger.println("datasetLabel: " + datasetLabel);
		logger.println("operation: " + operation);
		logger.println("uploadFormat: " + uploadFormat);
		logger.println("JVM Max memory: " + nf.format(rt.maxMemory() / mb));
		logger.println("JVM Total memory: " + nf.format(rt.totalMemory() / mb));
		logger.println("JVM Free memory: " + nf.format(rt.freeMemory() / mb));
		logger.println("*******************************************************************************\n");

		try {

			inputFile = new File(inputFileString);
			if (!inputFile.exists()) {
				logger.println("Error: File {" + inputFile.getAbsolutePath() + "} not found");
				throw new DatasetLoaderException("File {" + inputFile.getAbsolutePath() + "} not found");
			}

			if (inputFile.length() == 0) {
				logger.println("Error: File {" + inputFile.getAbsolutePath() + "} is empty");
				throw new DatasetLoaderException("Error: File {" + inputFile.getAbsolutePath() + "} is empty");
			}

			if (inputFile.length() > Constants.MAX_UNCOMPRESSED_FILE_LENGTH) {
				logger.println("Error: File {" + inputFile.getAbsolutePath()
						+ "} size is greater than the max supported size: "
						+ Constants.MAX_UNCOMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
				throw new DatasetLoaderException("Error: File {" + inputFile.getAbsolutePath()
						+ "} size is greater than the max supported size: "
						+ Constants.MAX_UNCOMPRESSED_FILE_LENGTH / FileUtils.ONE_GB + "GB");
			}

			if (schemaFileString != null) {
				schemaFile = new File(schemaFileString);
				if (!schemaFile.exists()) {
					logger.println("Error: File {" + schemaFile.getAbsolutePath() + "} not found");
					throw new DatasetLoaderException("File {" + schemaFile.getAbsolutePath() + "} not found");
				}

				if (schemaFile.length() == 0) {
					logger.println("Error: File {" + schemaFile.getAbsolutePath() + "} is empty");
					throw new DatasetLoaderException("Error: File {" + schemaFile.getAbsolutePath() + "} is empty");
				}
			}

			if (datasetAlias == null || datasetAlias.trim().isEmpty()) {
				throw new DatasetLoaderException("datasetAlias cannot be null");
			}

			String santizedDatasetName = FileSchema.createDevName(datasetAlias, "Dataset", 1, false);
			if (!datasetAlias.equals(santizedDatasetName)) {
				logger.println(
						"\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
				logger.println("\n changing dataset name to: {" + santizedDatasetName + "}");
				datasetAlias = santizedDatasetName;
			}

			if (datasetAlias.length() > 255)
				throw new DatasetLoaderException(
						"datasetName {" + datasetAlias + "} should be less than 255 characters");

			String datasetId = DatasetLoader.checkAPIAccess(tenantURL, apiVersion, tenantName, accessToken,
					datasetAlias, logger);

			if (datasetId == null && createDataset == true) {
				datasetId = DataAPIConsumer.createDataset(tenantURL, apiVersion, tenantName, accessToken, datasetAlias,
						logger);
			}

			// Validate access to the API before going any further
			if (datasetId == null) {
				logger.println("Error: you do not have access to Prism Data API. Please contact your Workday admin");
				throw new DatasetLoaderException(
						"Error: you do not have access to Prism Data API. Please contact your Workday admin");
			}

			if (session == null) {
				session = Session.getCurrentSession(tenantName, datasetAlias, false);
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			if (schemaFile == null)
				schemaFile = FileSchema.getSchemaFile(inputFile, logger);

			FileSchema schema = null;

			String fileExt = FilenameUtils.getExtension(inputFile.getName());
			boolean isParsable = false;
			if (fileExt != null && (fileExt.equalsIgnoreCase("csv") || fileExt.equalsIgnoreCase("txt"))) {
				isParsable = true;
			}

			if (!isParsable)
				throw new DatasetLoaderException("Error: Input file should be .csv type");

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			if (schema == null) {
				logger.println("\n*******************************************************************************");
				if (isParsable) {
					if (schemaFile != null && schemaFile.exists() && schemaFile.length() > 0)
						session.setStatus("LOADING SCHEMA");
					else
						session.setStatus("DETECTING SCHEMA");

					schema = FileSchema.init(inputFile, schemaFile, inputFileCharset, logger);
					if (schema == null) {
						logger.println(
								"Failed to parse schema file {" + FileSchema.getSchemaFile(inputFile, logger) + "}");
						throw new DatasetLoaderException(
								"Failed to parse schema file {" + FileSchema.getSchemaFile(inputFile, logger) + "}");
					}
				}
				logger.println("*******************************************************************************\n");
			}

			if (schema != null) {
				if ((operation.equalsIgnoreCase("Upsert") || operation.equalsIgnoreCase("Delete"))
						&& !FileSchema.hasUniqueID(schema)) {
					throw new DatasetLoaderException("Schema File {" + FileSchema.getSchemaFile(inputFile, logger)
							+ "} must have uniqueId set for atleast one field");
				}

				if (operation.equalsIgnoreCase("Append") && FileSchema.hasUniqueID(schema)) {
					throw new DatasetLoaderException("Schema File {" + FileSchema.getSchemaFile(inputFile, logger)
							+ "} has a uniqueId set. Choose 'Upsert' operation instead");
				}
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			archiveDir = new File(inputFile.getParent(), "archive");
			try {
				FileUtils.forceMkdir(archiveDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}

			datasetArchiveDir = new File(archiveDir, datasetAlias);
			try {
				FileUtils.forceMkdir(datasetArchiveDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}

			FileSchema altSchema = schema;

			String hdrId = insertFileHdr(tenantURL, apiVersion, tenantName, accessToken, datasetAlias, datasetId,
					datasetLabel, altSchema, operation, logger);

			if (hdrId == null || hdrId.isEmpty()) {
				throw new DatasetLoaderException("Error: failed to Bucket for Dataset: " + datasetAlias);
			}

			session.setParam(Constants.hdrIdParam, hdrId);

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			bucketDir = new File(datasetArchiveDir, hdrId);
			try {
				FileUtils.forceMkdir(bucketDir);
			} catch (Throwable t) {
				t.printStackTrace();
			}

			readInputFile(inputFile, bucketDir, hdrId, schema, logger, inputFileCharset, session, q);

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

			long startTime = System.currentTimeMillis();
			DataAPIConsumer.uploadDirToBucket(tenantURL, apiVersion, tenantName, accessToken, hdrId, bucketDir, logger);
			long endTime = System.currentTimeMillis();
			uploadTime = endTime - startTime;

			status = DataAPIConsumer.completeBucket(tenantURL, apiVersion, tenantName, accessToken, hdrId, logger);
			startTime = System.currentTimeMillis();
			while (status) {
				GetBucketResponseType serverStatus = DataAPIConsumer.getBucket(tenantURL, apiVersion, tenantName,
						accessToken, hdrId, logger);
				if (serverStatus != null) {
					session.setParam(Constants.serverStatusParam, serverStatus.getBucketState().toUpperCase());
					// Bucket state can be: New, Processing, Loading, Success, Failed
					if (serverStatus.getBucketState().equalsIgnoreCase("Success")) {
						break;
					} else if (serverStatus.getBucketState().equalsIgnoreCase("Processing")) {
						if (System.currentTimeMillis() - startTime > maxWaitTime) {
							status = false;
							throw new DatasetLoaderException(
									"Bucket {" + hdrId + "} did not finish processing in time. Giving up...");
						}
						Thread.sleep(3000);
						continue;
					} else // status can be (New, Loading, Failed) all are invalid states at this point
					{
						status = false;
						if (serverStatus.getErrorMessage() != null)
							throw new DatasetLoaderException(serverStatus.getErrorMessage());
						break;
					}

				}
			}

			if (session.isDone()) {
				throw new DatasetLoaderException("operation terminated on user request");
			}

		} catch (MalformedInputException mie) {
			logger.println("\n*******************************************************************************");
			logger.println("The input file is not valid utf8 encoded. Please save it as UTF8 file first");
			mie.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException("The input file is not utf8 encoded");
		} catch (Throwable t) {
			logger.println("\n*******************************************************************************");
			t.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException(t.getMessage());
		} finally {
			if (schemaFile != null && schemaFile.exists() && schemaFile.length() > 0)
				session.setParam(Constants.metadataJsonParam, schemaFile.getAbsolutePath());

			logger.println("\n*******************************************************************************");
			if (status)
				logger.println("Successfully uploaded {" + inputFile + "} to Dataset {" + datasetAlias
						+ "} Processing Time {" + nf.format(uploadTime) + "} msecs");
			else
				logger.println("Failed to load {" + inputFile + "} to Dataset {" + datasetAlias + "}");
			logger.println("*******************************************************************************\n");

			logger.println("\n*******************************************************************************");
			logger.println("End Timestamp: " + logformat.format(new Date()));
			logger.println("JVM Max memory: " + nf.format(rt.maxMemory() / mb));
			logger.println("JVM Total memory: " + nf.format(rt.totalMemory() / mb));
			logger.println("JVM Free memory: " + nf.format(rt.freeMemory() / mb));
			logger.println("*******************************************************************************\n");
		}
		return status;
	}

	/**
	 * Insert file hdr.
	 *
	 * @param partnerConnection
	 *            the partner connection
	 * @param datasetAlias
	 *            the dataset alias
	 * @param datasetContainer
	 *            the dataset container
	 * @param datasetLabel
	 *            the dataset label
	 * @param metadataJson
	 *            the metadata json
	 * @param dataFormat
	 *            the data format
	 * @param operation
	 *            the operation
	 * @param logger
	 *            the logger
	 * @param schema
	 * @return the string
	 * @throws DatasetLoaderException
	 *             the dataset loader exception
	 */
	private static String insertFileHdr(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetAlias, String datasetId, String datasetLabel, FileSchema schema, String operation,
			PrintStream logger) throws DatasetLoaderException {
		String rowId = null;
		long startTime = System.currentTimeMillis();
		try {

			rowId = DataAPIConsumer.createBucket(tenantURL, apiVersion, tenantName, accessToken, datasetAlias,
					datasetId, schema, operation, logger);

			long endTime = System.currentTimeMillis();
			if (rowId != null) {
				logger.println(
						"Created Bucket {" + rowId + "}, upload time {" + nf.format(endTime - startTime) + "} msec");
			}
		} catch (Throwable e) {
			e.printStackTrace(logger);
			throw new DatasetLoaderException("Failed to create Bucket: " + e.getMessage());
		}
		if (rowId == null)
			throw new DatasetLoaderException("Failed to create Bucket: " + datasetAlias);

		return rowId;
	}

	/**
	 * Check api access.
	 *
	 * @param accessToken
	 *            the partner connection
	 * @param logger
	 *            the logger
	 * @return true, if successful
	 * @throws DatasetLoaderException
	 */
	private static String checkAPIAccess(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String datasetAlias, PrintStream logger) throws DatasetLoaderException {
		try {

			List<DatasetType> datasetList = DataAPIConsumer.listDatasets(tenantURL, apiVersion, tenantName, accessToken,
					null, logger);
			if (datasetList == null || datasetList.size() == 0) {
				return null;
			} else {
				for (DatasetType dataset : datasetList) {
					if (datasetAlias.equalsIgnoreCase(dataset.getName())) {
						return dataset.getId();
					}
				}
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void readInputFile(File inputFile, File bucketDir, String hdrId, FileSchema schema,
			PrintStream logger, Charset inputFileCharset, Session session, BlockingQueue<List<String>> q)
			throws DatasetLoaderException, IOException {
		CSVReader reader = null;
		ErrorWriter errorWriter = null;
		WriterThread writer = null;
		// boolean status = false;
		long totalRowCount = 0;
		long successRowCount = 0;
		long errorRowCount = 0;
		long startTime = System.currentTimeMillis();

		try {

			try {

				errorWriter = new ErrorWriter(inputFile, schema.getParseOptions().getFieldsDelimitedBy().charAt(0),
						inputFileCharset);

				if (session != null) {
					session.setParam(Constants.errorCsvParam, errorWriter.getErrorFile().getAbsolutePath());
				}

				reader = new CSVReader(new FileInputStream(inputFile), inputFileCharset.name(),
						new char[] { schema.getParseOptions().getFieldsDelimitedBy().charAt(0) });
				writer = new WriterThread(q, inputFile, bucketDir, schema.getFields(), errorWriter, logger, session);
				Thread th = new Thread(writer, "Writer-Thread");
				th.setDaemon(true);
				th.start();

				boolean hasmore = true;
				logger.println("\n*******************************************************************************");
				logger.println(
						"File: " + inputFile.getName() + ", being digested to file: " + inputFile.getName() + ".gz");
				logger.println("*******************************************************************************\n");
				if (session != null)
					session.setStatus("DIGESTING");
				List<String> row = null;
				while (hasmore) {
					if (session != null && session.isDone()) {
						throw new DatasetLoaderException("operation terminated on user request");
					}
					try {
						totalRowCount++;
						row = reader.nextRecord();
						if (row != null && !writer.isDone() && !writer.isAborted()) {
							if (totalRowCount == 1)
								continue;
							if (row.size() != 0) {
								q.put(row);
							} else {
								errorRowCount++;
							}
						} else {
							totalRowCount--;
							hasmore = false;
						}
					} catch (Exception t) {
						errorRowCount++;
						logger.println("Line {" + (totalRowCount) + "} has error {" + t + "}");

						if (t instanceof MalformedInputException || errorRowCount >= Constants.max_error_threshhold) {
							// status = false;
							hasmore = false;
							int retryCount = 0;
							while (!writer.isDone()) {
								retryCount++;
								try {
									Thread.sleep(1000);
									if (retryCount % 10 == 0) {
										q.put(new ArrayList<String>(0));
										logger.println("Waiting for writer to finish");
									}
								} catch (InterruptedException in) {
									in.printStackTrace();
								}
							}

							if (errorRowCount >= Constants.max_error_threshhold) {
								logger.println(
										"\n*******************************************************************************");
								logger.println("Max error threshold reached. Aborting processing");
								logger.println(
										"*******************************************************************************\n");
								throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
							} else if (t instanceof MalformedInputException) {
								logger.println(
										"\n*******************************************************************************");
								logger.println("The input file is not utf8 encoded. Please save it as UTF8 file first");
								logger.println(
										"*******************************************************************************\n");
								throw new DatasetLoaderException("The input file is not utf8 encoded");
							}

						}
					} finally {
						if (session != null) {
							session.setSourceTotalRowCount(totalRowCount);
							session.setSourceErrorRowCount(errorRowCount);
						}
					}
				} // end while
				int retryCount = 0;
				while (!writer.isDone()) {
					try {
						if (retryCount % 10 == 0) {
							q.put(new ArrayList<String>(0));
							logger.println("Waiting for writer to finish");
						}
						Thread.sleep(1000);
					} catch (InterruptedException in) {
						in.printStackTrace();
					}
					retryCount++;
				}
				successRowCount = writer.getSuccessRowCount();
				errorRowCount = writer.getErrorRowCount();
			} finally {
				if (reader != null)
					reader.finalise();
			}
			long endTime = System.currentTimeMillis();
			long digestTime = endTime - startTime;

			if (writer.isAborted()) {
				throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
			}

			if (successRowCount < 1) {
				logger.println("\n*******************************************************************************");
				logger.println("All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
				logger.println("*******************************************************************************\n");
				throw new DatasetLoaderException(
						"All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
			}
			if (errorRowCount > 1) {
				logger.println("\n*******************************************************************************");
				logger.println(nf.format(errorRowCount) + " Rows failed. Please check {"
						+ errorWriter.getErrorFile().getName() + "} for error rows");
				logger.println("*******************************************************************************\n");
			}

			if (writer.getTotalFileSize() == 0L) {
				logger.println("Error: File {" + bucketDir.getAbsolutePath() + "} not found or is zero bytes");
				throw new DatasetLoaderException(
						"Error: Output File {" + bucketDir.getAbsolutePath() + "} not found or is zero bytes");
			}

			logger.println("\n*******************************************************************************");
			logger.println("Total Rows: " + nf.format(totalRowCount - 1) + ", Success Rows: "
					+ nf.format(successRowCount) + ", Error Rows: " + nf.format(errorRowCount) + ", % Compression: "
					+ (inputFile.length() / writer.getTotalFileSize()) * 100 + "%" + ", Digest Time {"
					+ nf.format(digestTime) + "} msecs");
			logger.println("*******************************************************************************\n");
		} finally {
		}
	}

}
