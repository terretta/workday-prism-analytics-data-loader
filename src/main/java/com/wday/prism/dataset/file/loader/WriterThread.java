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
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FieldType;
import com.wday.prism.dataset.monitor.Session;

public class WriterThread implements Runnable {

	private final BlockingQueue<List<String>> queue;
	private final File outputDir;
	private final String uploadFilePrefix;
	private final List<FieldType> fields;
	private final ErrorWriter errorwriter;
	private final PrintStream logger;

	private volatile AtomicBoolean done = new AtomicBoolean(false);
	private volatile AtomicBoolean aborted = new AtomicBoolean(false);
	private volatile AtomicInteger successRowCount = new AtomicInteger(0);
	private volatile AtomicInteger errorRowCount = new AtomicInteger(0);
	private volatile AtomicInteger totalRowCount = new AtomicInteger(0);
	private volatile AtomicLong totalFileSize = new AtomicLong(0L);
	Session session = null;
	private int headerLinesToIgnore;

	WriterThread(BlockingQueue<List<String>> q, String uploadFilePrefix, File outputDir, List<FieldType> fields, ErrorWriter ew,
			PrintStream logger, Session session, int headerLinesToIgnore) {
		if (q == null || uploadFilePrefix == null || outputDir == null || ew == null || session == null) {
			throw new IllegalArgumentException("Constructor input cannot be null");
		}
		queue = q;
		this.uploadFilePrefix = uploadFilePrefix;
		this.outputDir = outputDir;
		this.fields = fields;
		this.errorwriter = ew;
		this.logger = logger;
		this.session = session;
		this.headerLinesToIgnore = headerLinesToIgnore;
	}

	@Override
	public void run() {
		logger.println("Start: " + Thread.currentThread().getName());
		CsvFormatWriter csvFormatWriter = null;
		try {
			List<String> row = queue.take();
			while (row != null && row.size() != 0) {
				if (session.isDone()) {
					throw new DatasetLoaderException("operation terminated on user request");
				}

				try {
					totalRowCount.getAndIncrement();
					if (csvFormatWriter == null) {
						csvFormatWriter = new CsvFormatWriter(outputDir, uploadFilePrefix, fields, logger, headerLinesToIgnore);
					}
					csvFormatWriter.addrow(row);
				} catch (Throwable t) {
					if (errorRowCount.get() == 0) {
						logger.println();
					}
					logger.println("Row {" + totalRowCount + "} has error {" + t + "}");
					if (row != null) {
						errorRowCount.getAndIncrement();
						if (Constants.debug)
							t.printStackTrace();
						errorwriter.addError(row, t.getMessage() != null ? t.getMessage() : t.toString());
						if (errorRowCount.get() >= Constants.max_error_threshhold
								&& ((errorRowCount.get() / totalRowCount.get()) > .5)) {
							logger.println("Max error threshold reached. Aborting processing");
							aborted.set(true);
							queue.clear();
							break;
						}
					}
				} finally {
					session.setTargetTotalRowCount(totalRowCount.get());
					session.setTargetErrorCount(errorRowCount.get());
				}
				row = queue.take();
			}
		} catch (Throwable t) {
			logger.println(Thread.currentThread().getName() + " " + t.toString());
			aborted.set(true);
			queue.clear();
		} finally {
			try {
				csvFormatWriter.finish();
				this.totalFileSize.set(csvFormatWriter.getTotalOutputFileSize());
				this.setSuccessRowCount(csvFormatWriter.getSuccessRowCount());
			} catch (Throwable e) {
				e.printStackTrace(logger);
			}
			try {
				errorwriter.finish();
			} catch (Throwable e) {
				e.printStackTrace(logger);
			}
		}
		logger.println("END: " + Thread.currentThread().getName());
		done.set(true);
		queue.clear();
	}

	public boolean isDone() {
		return done.get();
	}

	public boolean isAborted() {
		return aborted.get();
	}

	public int getErrorRowCount() {
		return errorRowCount.get();
	}

	public int getTotalRowCount() {
		return totalRowCount.get();
	}

	public long getTotalFileSize() {
		return totalFileSize.get();
	}

	public int getSuccessRowCount() {
		return successRowCount.get();
	}

	public void setSuccessRowCount(int successRowCount) {
		this.successRowCount.set(successRowCount);
	}

}