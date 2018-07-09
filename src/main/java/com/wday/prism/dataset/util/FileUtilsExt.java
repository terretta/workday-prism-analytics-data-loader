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
package com.wday.prism.dataset.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

/**
 * The Class FileUtilsExt.
 */
public class FileUtilsExt {

	/** The Constant p. */
	public static final Pattern p = Pattern.compile("copy\\d+$");

	/**
	 * Gets the unique file.
	 *
	 * @param file
	 *            the file
	 * @return the unique file
	 */
	public static File getUniqueFile(File file) {
		if (file == null)
			return file;

		if (file.exists()) {
			int ver = -1;
			while (file.exists()) {
				String name = FilenameUtils.getBaseName(file.getName());
				if (ver == -1) {
					ver = 0;
					String version = null;
					if (name != null) {
						Matcher m = p.matcher(name);
						if (m.find()) {
							version = m.group();
						}
						if (version != null && !version.trim().isEmpty()) {
							BigDecimal bd = new BigDecimal(version.replace("copy", ""));
							if (bd != null) {
								ver = bd.intValue();
								if (ver > 99) {
									ver = 0;
								}
							}
							if (name.endsWith(version)) {
								name = name.replace(version, "");
							}
						}
					} else {
						name = "";
					}
				}
				if (name.endsWith("copy" + ver)) {
					name = name.replace("copy" + ver, "");
				}
				ver++;
				if (FilenameUtils.getExtension(file.getName()) == null
						|| FilenameUtils.getExtension(file.getName()).trim().isEmpty()) {
					name = name + "copy" + ver;
				} else {
					name = name + "copy" + ver + "." + FilenameUtils.getExtension(file.getName());
				}
				if (file.getAbsoluteFile().getParentFile() == null) {
					file = new File(name);
				} else {
					file = new File(file.getAbsoluteFile().getParentFile(), name);
				}
			}
		}
		return file;
	}

	/**
	 * Delete quietly.
	 *
	 * @param file
	 *            the file
	 * @return true, if successful
	 */
	public static boolean deleteQuietly(File file) {
		if (file != null) {
			Path path = Paths.get(file.getAbsolutePath());
			try {
				return Files.deleteIfExists(path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	/**
	 * Gets the files.
	 *
	 * @param directory
	 *            the directory
	 * @param fileFilter
	 *            the file filter
	 * @return the files
	 */
	public static File[] getFiles(File directory, IOFileFilter fileFilter) {
		Collection<File> list = FileUtils.listFiles(directory, fileFilter, TrueFileFilter.INSTANCE);
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

	public static File[] getFiles(File directory, IOFileFilter fileFilter, boolean includeSubDirectories) {
		Collection<File> list = null;

		if (includeSubDirectories)
			list = FileUtils.listFiles(directory, fileFilter, TrueFileFilter.INSTANCE);
		else
			list = FileUtils.listFiles(directory, fileFilter, null);

		if (list == null)
			return null;

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

	public static boolean isGzipFile(File input) throws IOException {
		FileInputStream fios = new FileInputStream(input);
		byte[] signature = new byte[2];
		int len = fios.read(signature); // read the signature
		fios.close();
		if (len == 2 && signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) // check if matches standard gzip
																					// magic number
			return true;
		else
			return false;
	}

}
