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
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.schema.FileSchema;
import com.wday.prism.dataset.monitor.Session;

public class FolderUploader {

	private static final int MAX_THREAD_POOL = 100;
	private static final ThreadPoolExecutor executorPool = new ThreadPoolExecutor(Constants.MAX_CONCURRENT_LOADS,
			Constants.MAX_CONCURRENT_LOADS, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(MAX_THREAD_POOL),
			Executors.defaultThreadFactory());

	public static boolean uploadFolder(String tenantURL, String apiVersion, String tenantName, String accessToken,
			String csvFile, String schemaFile, String uploadFormat, CodingErrorAction codingErrorAction,
			Charset inputFileCharset, String datasetAlias, String datasetLabel, String Operation, PrintStream logger,
			boolean createDataset) throws DatasetLoaderException {

		System.out.println("Starting FolderUploader for Folder {" + csvFile + "} ");
		LinkedHashMap<UploadWorker, Session> workers = new LinkedHashMap<UploadWorker, Session>();

		try {

			try {
				IOFileFilter suffixFileFilter1 = FileFilterUtils.suffixFileFilter(".csv.gz", IOCase.INSENSITIVE);
				IOFileFilter suffixFileFilter2 = FileFilterUtils.suffixFileFilter(".csv", IOCase.INSENSITIVE);
				IOFileFilter orFileFilter = FileFilterUtils.or(suffixFileFilter1, suffixFileFilter2);

				File[] files = getFiles(new File(csvFile), orFileFilter);
				if (files == null) {
					return false;
				}

				for (File file : files) {
					if (file.getName().toLowerCase().endsWith(ErrorWriter.errorFileSuffix))
						continue;

					Session session = null;
					try {

						while (executorPool.getQueue().size() >= MAX_THREAD_POOL) {
							System.out.println("There are too many jobs in the queue, will wait before trying again");
							Thread.sleep(3000);
						}

						String newDataSetAlias = FileSchema.createDevName(FilenameUtils.getBaseName(file.getName()),
								"Dataset", 1, true);
						session = new Session(tenantName, newDataSetAlias);
						UploadWorker worker = new UploadWorker(tenantURL, apiVersion, tenantName, accessToken,
								file.getAbsolutePath(), schemaFile, uploadFormat, codingErrorAction, inputFileCharset,
								newDataSetAlias, datasetLabel, Operation, createDataset, session);

						try {
							executorPool.execute(worker);
							workers.put(worker, session);
						} catch (Throwable t) {
							Session.removeCurrentSession(session);
						}

					} catch (Throwable e) {
						System.out.println();
						e.printStackTrace(System.out);
						if (session != null)
							Session.removeCurrentSession(session);
					} finally {
						session = null;
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
			waitforAllWorkersToBeDone(workers);
			executorPool.shutdownNow();
		} catch (Throwable t) {
			System.out.println(Thread.currentThread().getName() + " " + t.getMessage());
		}
		System.out.println("Stopping FolderLoader for Folder {" + csvFile + "} ");
		return true;
	}

	private static void waitforAllWorkersToBeDone(LinkedHashMap<UploadWorker, Session> workers)
			throws InterruptedException {
		boolean working = true;
		while (working) {
			working = false;
			int sleepCount = 0;
			for (UploadWorker work : workers.keySet()) {

				if (!work.isDone()) {
					working = true;
					if (sleepCount % 10 == 0)
						System.out.println("Waiting for upload worker {" + work.datasetName + "} to finish");
					Thread.sleep(3000);
					sleepCount++;
				} else {
					Session session = workers.get(work);
					if (session != null)
						Session.removeCurrentSession(session);

				}
			}
		}
	}

	public static File[] getFiles(File directory, IOFileFilter fileFilter) {
		if (directory == null)
			directory = new File("").getAbsoluteFile();
		FileUtils.listFiles(directory, fileFilter, null);
		Collection<File> list = FileUtils.listFiles(directory, fileFilter, null);

		File[] files = list.toArray(new File[0]);

		if (files != null && files.length > 0) {
			Arrays.sort(files, new Comparator<File>() {
				@Override
				public int compare(File a, File b) {
					long diff = (a.lastModified() - b.lastModified());
					if (diff > 0L)
						return 1;
					else if (diff < 0L)
						return -1;
					else
						return 0;
				}
			});

			return files;
		} else {
			return null;
		}
	}
}