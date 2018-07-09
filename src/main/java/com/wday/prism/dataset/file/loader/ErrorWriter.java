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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.util.CSVReader;
import com.wday.prism.dataset.util.CSVWriter;
import com.wday.prism.dataset.util.FileUtilsExt;
import com.wday.prism.dataset.util.Logger;

/**
 * The Class ErrorWriter.
 */
public class ErrorWriter {

	/** The delimiter. */
	private char delimiter = ',';

	/** The Header line. */
	private String HeaderLine = "";

	/** The header columns. */
	private ArrayList<String> headerColumns = null;

	/** The Header suffix. */
	private String HeaderSuffix = "Error";

	/** The error file suffix. */
	public static String errorFileSuffix = "_err.";

	/** The error csv. */
	private File errorCsv;

	/** The writer. */
	private BufferedWriter fWriter = null;

	/** The Constant LF. */
	public static final char LF = '\n';

	/** The Constant CR. */
	public static final char CR = '\r';

	/** The Constant QUOTE. */
	public static final char QUOTE = '"';

	/** The Constant COMMA. */
	public static final char COMMA = ',';

	/**
	 * Instantiates a new error writer.
	 *
	 * @param inputCsv
	 *            the input csv
	 * @param delimiter
	 *            the delimiter
	 * @param inputFileCharset
	 *            the input File Charset
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public ErrorWriter(File inputCsv, char delimiter, Charset inputFileCharset) throws IOException {
		if (inputCsv == null || !inputCsv.exists()) {
			throw new IOException("inputCsv file {" + inputCsv + "} does not exist");
		}

		this.delimiter = delimiter;

		CSVReader reader = new CSVReader(new FileInputStream(inputCsv), inputFileCharset.name(), delimiter);
		headerColumns = reader.nextRecord();
		reader.finalise();

		StringBuffer hdrLine = new StringBuffer();
		hdrLine.append(HeaderSuffix);
		int cnt = 0;
		for (String hdr : headerColumns) {
			cnt++;
			hdrLine.append(this.delimiter);
			hdrLine.append(hdr);
			if (hdrLine.length() > 400000)
				break;
			if (cnt > 5000)
				break;
		}
		this.HeaderLine = hdrLine.toString();

		this.errorCsv = new File(inputCsv.getParent(), FilenameUtils.getBaseName(inputCsv.getName()) + errorFileSuffix
				+ FilenameUtils.getExtension((inputCsv.getName())));
		if (this.errorCsv.exists()) {
			try {
				File archiveDir = new File(inputCsv.getParent(), "archive");
				FileUtils.moveFile(this.errorCsv,
						new File(archiveDir, this.errorCsv.getName() + "." + this.errorCsv.lastModified()));
			} catch (Throwable t) {
				FileUtilsExt.deleteQuietly(this.errorCsv);
			}
		}
	}

	public void addError(List<String> values, String error) {
		try {
			if (fWriter == null) {
				fWriter = new BufferedWriter(new FileWriter(this.errorCsv), Constants.DEFAULT_BUFFER_SIZE);
				if (this.HeaderLine != null)
					fWriter.write(this.HeaderLine + "\n");
			}
			if (error == null)
				error = "null";
			fWriter.write(getCSVFriendlyString(error));
			if (values != null) {
				for (String val : values) {
					fWriter.write(this.delimiter);
					if (val != null)
						fWriter.write(CSVWriter.encode(val, this.delimiter, QUOTE));
				}
			}
			fWriter.write("\n");
		} catch (Throwable t) {
			t.printStackTrace(Logger.out);
		}
	}

	/**
	 * Gets the error file.
	 *
	 * @return the error file
	 */
	public File getErrorFile() {
		return this.errorCsv;
	}

	/**
	 * Finish.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void finish() throws IOException {
		if (fWriter != null) {
			fWriter.flush();
			IOUtils.closeQuietly(fWriter);
		}
		fWriter = null;
	}

	/**
	 * Gets the CSV friendly string.
	 *
	 * @param content
	 *            the content
	 * @return the CSV friendly string
	 */
	public static String getCSVFriendlyString(String content) {
		if (content != null && !content.isEmpty()) {
			content = replaceString(content, "" + COMMA, "");
			content = replaceString(content, "" + CR, "");
			content = replaceString(content, "" + LF, "");
			content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}

	/**
	 * Replace string.
	 *
	 * @param original
	 *            the original
	 * @param pattern
	 *            the pattern
	 * @param replace
	 *            the replace
	 * @return the string
	 */
	public static String replaceString(String original, String pattern, String replace) {
		if (original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace != null) {
			final int len = pattern.length();
			int found = original.indexOf(pattern);

			if (found > -1) {
				StringBuffer sb = new StringBuffer();
				int start = 0;

				while (found != -1) {
					sb.append(original.substring(start, found));
					sb.append(replace);
					start = found + len;
					found = original.indexOf(pattern, start);
				}

				sb.append(original.substring(start));

				return sb.toString();
			} else {
				return original;
			}
		} else
			return original;
	}
}
