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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FilenameUtils;

import com.wday.prism.dataset.monitor.Session;
import com.wday.prism.dataset.monitor.ThreadContext;

public class UploadWorker implements Runnable {

	private final File logFile;
	private final Session session;

	private Charset inputFileCharset = null;
	private final PrintStream logger;
	private String csvFile = null;
	private String schemaFile = null;
	String datasetName = null;
	private String datasetLabel = null;
	private String operation = "Overwrite";

	private String tenantURL = null;
	private String apiVersion = null;
	private String tenantName = null;
	private String accessToken = null;
	private String uploadFormat = null;
	private CodingErrorAction codingErrorAction;
	private boolean createDataset = true;

	private volatile AtomicBoolean uploadStatus = new AtomicBoolean(false);
	private volatile AtomicBoolean isDone = new AtomicBoolean(false);

	public UploadWorker(String tenantURL, String apiVersion, String tenantName, String accessToken, String csvFile,
			String schemaFile, String uploadFormat, CodingErrorAction codingErrorAction, Charset inputFileCharset,
			String datasetAlias, String datasetLabel, String Operation, boolean createDataset, Session session)
			throws IOException {
		this.session = session;
		this.tenantURL = tenantURL;
		this.tenantName = tenantName;
		this.apiVersion = apiVersion;
		this.accessToken = accessToken;
		this.uploadFormat = uploadFormat;
		this.codingErrorAction = codingErrorAction;
		this.createDataset = createDataset;

		this.inputFileCharset = inputFileCharset;
		this.datasetName = datasetAlias;
		this.datasetLabel = datasetLabel;
		this.operation = Operation;

		this.csvFile = csvFile;
		this.schemaFile = schemaFile;

		File tmp = new File(csvFile);
		FilenameUtils.getBaseName(tmp.getName());

		this.logFile = new File(tmp.getAbsoluteFile().getParentFile(),
				FilenameUtils.getBaseName(tmp.getName()) + ".log");

		this.logger = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
	}

	@Override
	public void run() {
		boolean status = false;
		try {
			ThreadContext threadContext = ThreadContext.get();
			threadContext.setSession(session);
			session.start();
			try {
				System.out.println("START Uploading file {" + this.csvFile + "} to dataset: " + datasetName);
				status = com.wday.prism.dataset.file.loader.DatasetLoader.uploadDataset(tenantURL, apiVersion,
						tenantName, accessToken, csvFile, schemaFile, uploadFormat, codingErrorAction, inputFileCharset,
						datasetName, datasetLabel, operation, logger, createDataset);
				if (status)
					session.end();
				else
					session.fail("Check sessionLog for details");
			} catch (Throwable e) {
				e.printStackTrace(logger);
				session.fail(e.getMessage());
			} finally {
				System.out.println("END Uploading file {" + this.csvFile + "} to dataset: " + datasetName + " Status: "
						+ (status ? "SUCCCESS" : "FAILED"));
			}
		} finally {
			uploadStatus.set(status);
			isDone.set(true);
		}
	}

	public boolean isDone() {
		return isDone.get();
	}

	public boolean isUploadStatus() {
		return uploadStatus.get();
	}

	public File getLogFile() {
		return logFile;
	}

}
