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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeparatorGuesser {

	static public class Separator {
		char separator;
		int totalCount = 0;
		int totalOfSquaredCount = 0;
		int currentLineCount = 0;

		double averagePerLine;
		double stddev;
	}

	static public char guessSeparator(File file, Charset inputFileCharset, boolean handleQuotes) {
		try {
			InputStream is = new FileInputStream(file);
			Reader reader = inputFileCharset != null ? new InputStreamReader(is, inputFileCharset)
					: new InputStreamReader(is);
			LineNumberReader lineNumberReader = new LineNumberReader(reader);

			try {
				List<Separator> separators = new ArrayList<SeparatorGuesser.Separator>();
				Map<Character, Separator> separatorMap = new HashMap<Character, SeparatorGuesser.Separator>();

				int totalChars = 0;
				int lineCount = 0;
				boolean inQuote = false;
				String s;
				while (totalChars < 64 * 1024 && lineCount < 100 && (s = lineNumberReader.readLine()) != null) {

					totalChars += s.length() + 1; // count the new line character
					if (s.length() == 0) {
						continue;
					}
					if (!inQuote) {
						lineCount++;
					}

					for (int i = 0; i < s.length(); i++) {
						char c = s.charAt(i);
						if ('"' == c) {
							inQuote = !inQuote;
						}
						if (!Character.isLetterOrDigit(c) && !"\"' .-".contains(s.subSequence(i, i + 1))
								&& (!handleQuotes || !inQuote)) {
							Separator separator = separatorMap.get(c);
							if (separator == null) {
								separator = new Separator();
								separator.separator = c;

								separatorMap.put(c, separator);
								separators.add(separator);
							}
							separator.currentLineCount++;
						}
					}

					if (!inQuote) {
						for (Separator separator : separators) {
							separator.totalCount += separator.currentLineCount;
							separator.totalOfSquaredCount += separator.currentLineCount * separator.currentLineCount;
							separator.currentLineCount = 0;
						}
					}
				}

				if (separators.size() > 0) {
					for (Separator separator : separators) {
						separator.averagePerLine = separator.totalCount / (double) lineCount;
						separator.stddev = Math.sqrt((((double) lineCount * separator.totalOfSquaredCount)
								- (separator.totalCount * separator.totalCount))
								/ ((double) lineCount * (lineCount - 1)));
					}

					Collections.sort(separators, new Comparator<Separator>() {
						@Override
						public int compare(Separator sep0, Separator sep1) {
							if (sep0.stddev == 0 && sep1.stddev == 0)
								return -1 * Double.compare(sep0.averagePerLine, sep1.averagePerLine);
							else
								return Double.compare(sep0.stddev / sep0.averagePerLine,
										sep1.stddev / sep1.averagePerLine);
						}
					});

					for (Separator separator : separators) {
						// Separator separator = separators.get(0);
						if (separator.averagePerLine >= 1.0 && (separator.stddev / separator.averagePerLine < 0.1)) {
							return separator.separator;
						}
					}

				}
			} finally {
				lineNumberReader.close();
				reader.close();
				is.close();
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return 0;
	}

}
