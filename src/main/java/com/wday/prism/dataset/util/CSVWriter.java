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

import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;

public class CSVWriter {
	private PrintWriter writer;
	private char delimiter = ',';
	private char quoteChar = '\"';

	public CSVWriter(Writer w, char delimiter, char quoteChar) {
		writer = new PrintWriter(w, true);
		if (delimiter != 0) {
			this.delimiter = delimiter;
		}

		if (quoteChar != 0) {
			this.quoteChar = quoteChar;
		}
	}

	public void writeRecord(String[] values) {
		if (values != null && values.length != 0) {
			writeFirstField(values[0]);
			for (int i = 1; i < values.length; i++) {
				writeField(values[i]);
			}
			endRecord();
		}
	}

	public void writeRecord(List<String> values) {
		if (values != null && !values.isEmpty()) {
			writeFirstField(values.get(0));
			for (int i = 1; i < values.size(); i++) {
				writeField(values.get(i));
			}
			endRecord();
		}
	}

	public void close() {
		writer.close();
	}

	public void endRecord() {
		writer.println();
	}

	public void writeField(String value) {
		writer.print(this.delimiter);
		if (value != null && !value.isEmpty()) {
			writeFirstField(value);
		}
	}

	public void writeFirstField(String value) {
		if (value != null && !value.isEmpty()) {
			writer.print(encode(value, delimiter, quoteChar));
		}
	}

	public static String encode(final String input, char delimiter, char quote) {

		final StringBuilder currentColumn = new StringBuilder();
		final String eolSymbols = "\n";
		final int lastCharIndex = input.length() - 1;

		boolean quotesRequiredForSpecialChar = false;

		boolean skipNewline = false;

		for (int i = 0; i <= lastCharIndex; i++) {

			final char c = input.charAt(i);

			if (skipNewline) {
				skipNewline = false;
				if (c == '\n') {
					continue; // newline following a carriage return is skipped
				}
			}

			if (c == delimiter) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(c);
			} else if (c == quote) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(quote);
				currentColumn.append(quote);
			} else if (c == '\r') {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
				skipNewline = true;
			} else if (c == '\n') {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
			} else {
				currentColumn.append(c);
			}
		}

		final boolean quotesRequiredForMode = false;
		final boolean quotesRequiredForSurroundingSpaces = true && input.length() > 0
				&& (input.charAt(0) == ' ' || input.charAt(input.length() - 1) == ' ');

		if (quotesRequiredForSpecialChar || quotesRequiredForMode || quotesRequiredForSurroundingSpaces) {
			currentColumn.insert(0, quote).append(quote);
		}

		return currentColumn.toString();
	}
}
