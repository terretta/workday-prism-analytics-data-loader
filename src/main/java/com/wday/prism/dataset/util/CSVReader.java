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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.input.BOMInputStream;

import com.wday.prism.dataset.constants.Constants;

/**
 * Parse a CSV file into lines of fields.
 */
public class CSVReader {

	private static final int bufferSize = 65536;

	private final BufferedReader reader;
	private StreamTokenizer parser;
	private char[] separators;
	private boolean ignoreBlankRecords = true;
	private int maxSizeOfIndividualCell = 32000;
	private int maxColumnsPerRow = 1000;
	private int maxRowSizeInCharacters = 500000; // 500K of characters in a row..

	private long maxFileSizeInCharacters = Long.MAX_VALUE;
	private long maxRowsInFile = Long.MAX_VALUE;

	private long fileSizeInCharacters = 0;
	private long rowsInFile = 0;
	private int maxFieldCount;

	private int avgRowLength = 0;
	private int lastRowLength = 0;

	boolean atEOF;

	private CSVReader(Reader input) {
		this(new BufferedReader(input));
	}

	private CSVReader(Reader input, char customizedSeparator) {
		this(new BufferedReader(input), customizedSeparator);
	}

	private CSVReader(Reader input, char[] customizedSeparators) {
		this(new BufferedReader(input), customizedSeparators);
	}

	public CSVReader(InputStream input) {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction)));
	}

	public CSVReader(InputStream input, char customizedSeparator) throws UnsupportedEncodingException {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction)), customizedSeparator);
	}

	public CSVReader(InputStream input, char[] customizedSeparators) throws UnsupportedEncodingException {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction)), customizedSeparators);
	}

	public CSVReader(InputStream input, String enc) throws UnsupportedEncodingException {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction, enc)));
	}

	public CSVReader(InputStream input, String enc, char customizedSeparator) throws UnsupportedEncodingException {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction, enc)), customizedSeparator);
	}

	public CSVReader(InputStream input, String enc, char[] customizedSeparators) throws UnsupportedEncodingException {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				StringUtilsExt.utf8Decoder(Constants.codingErrorAction, enc)), customizedSeparators);
	}

	private CSVReader(BufferedReader input) {
		this(input, ',');
	}

	private CSVReader(BufferedReader input, char customizedSeparator) {
		this(input, new char[] { customizedSeparator });
	}

	private CSVReader(BufferedReader input, char[] customizedSeparators) {
		this.reader = input;
		Arrays.sort(customizedSeparators);
		this.separators = customizedSeparators;

		parser = new StreamTokenizer(input);
		parser.ordinaryChars(0, 255);
		parser.wordChars(0, 255);
		parser.ordinaryChar('\"');
		for (char customizedSeparator : customizedSeparators) {
			parser.ordinaryChar(customizedSeparator);
		}

		// Need to do set EOL significance after setting ordinary and word
		// chars, and need to explicitly set \n and \r as whitespace chars
		// for EOL detection to work
		parser.eolIsSignificant(true);
		parser.whitespaceChars('\n', '\n');
		parser.whitespaceChars('\r', '\r');
		atEOF = false;
	}

	public void finalise() throws IOException {
		this.reader.close();
	}

	private void checkRecordExceptions(List<String> line) throws IOException {
		int rowSizeInCharacters = 0;
		if (line != null) {
			for (String value : line) {
				if (value != null) {
					rowSizeInCharacters += value.length();
				}
			}
			this.setLastRowLength(rowSizeInCharacters);

			if (rowSizeInCharacters > maxRowSizeInCharacters) {
				throw new CSVParseException("Exceeded max length for one record: " + rowSizeInCharacters
						+ ". Max length for one record should be less than or equal to " + maxRowSizeInCharacters,
						parser.lineno());
			}

			fileSizeInCharacters += rowSizeInCharacters;

			if (fileSizeInCharacters > maxFileSizeInCharacters) {
				throw new CSVParseException("Exceeded max file size: " + fileSizeInCharacters
						+ ". Max file size in characters should be less than or equal to " + maxFileSizeInCharacters,
						parser.lineno());
			}

			rowsInFile++;

			if (rowsInFile > maxRowsInFile) {
				throw new CSVParseException(
						"Exceeded number of records : " + rowsInFile
								+ ". Number of records should be less than or equal to " + maxRowsInFile,
						parser.lineno());
			}
		}
	}

	public ArrayList<String> nextRecord() throws IOException {
		ArrayList<String> record = nextRecordLocal();

		if (ignoreBlankRecords) {
			while (record != null) {
				boolean emptyLine = false;

				if (record.size() == 0) {
					emptyLine = true;
				} else if (record.size() == 1) {
					String val = record.get(0);
					if (val == null || val.length() == 0) {
						emptyLine = true;
					}
				} else {
					boolean found = false;
					for (String value : record) {
						if (value != null && value.length() > 0) {
							found = true;
							break;
						}
					}
					emptyLine = !found;
				}

				if (emptyLine) {
					record = nextRecordLocal();
				} else {
					break;
				}
			}
		}

		checkRecordExceptions(record);
		return record;
	}

	private ArrayList<String> nextRecordLocal() throws IOException {
		if (atEOF) {
			return null;
		}

		ArrayList<String> record = new ArrayList<String>(maxFieldCount);

		StringBuilder fieldValue = null;

		while (true) {
			int token = parser.nextToken();

			if (token == StreamTokenizer.TT_EOF) {
				addField(record, fieldValue);
				atEOF = true;
				break;
			}

			if (token == StreamTokenizer.TT_EOL) {
				addField(record, fieldValue);
				break;
			}

			if (token == StreamTokenizer.TT_WORD) {
				if (fieldValue != null) {
					throw new CSVParseException(String.format("Unknown error, near %s", fieldValue), parser.lineno());
				}

				fieldValue = new StringBuilder(parser.sval);
				continue;
			}

			if (Arrays.binarySearch(separators, (char) token) >= 0) {
				addField(record, fieldValue);
				fieldValue = null;
				continue;
			}

			if (token == '"') {
				if (fieldValue != null) {
					throw new CSVParseException("Found unescaped quote. A value with quote should be within a quote",
							parser.lineno());
				}

				while (true) {
					token = parser.nextToken();

					if (token == StreamTokenizer.TT_EOF) {
						atEOF = true;
						throw new CSVParseException("EOF reached before closing an opened quote", parser.lineno());
					}

					if (token == StreamTokenizer.TT_EOL) {
						fieldValue = appendFieldValue(fieldValue, "\n");
						continue;
					}

					if (token == StreamTokenizer.TT_WORD) {
						fieldValue = appendFieldValue(fieldValue, parser.sval);
						continue;
					}

					if (Arrays.binarySearch(separators, (char) token) >= 0) {
						fieldValue = appendFieldValue(fieldValue, token);
						continue;
					}

					if (token == '"') {
						int nextToken = parser.nextToken();

						if (nextToken == '"') {
							// escaped quote
							fieldValue = appendFieldValue(fieldValue, nextToken);
							continue;
						}

						if (nextToken == StreamTokenizer.TT_WORD) {
							throw new CSVParseException("Not expecting more text after end quote", parser.lineno());
						} else {
							parser.pushBack();
							break;
						}
					}
				}
			}
		}

		if (record.size() > maxFieldCount) {
			maxFieldCount = record.size();
		}

		return record;
	}

	private StringBuilder appendFieldValue(StringBuilder fieldValue, int token) throws CSVParseException {
		return appendFieldValue(fieldValue, "" + (char) token);
	}

	private StringBuilder appendFieldValue(StringBuilder fieldValue, String token) throws CSVParseException {
		if (fieldValue == null) {
			fieldValue = new StringBuilder();
		}

		fieldValue.append(token);

		if (token.length() > maxSizeOfIndividualCell) {
			throw new CSVParseException("Exceeded max field size: " + token.length(), parser.lineno());
		}

		return fieldValue;
	}

	public int getAvgRowLength() {
		return this.avgRowLength;
	}

	private void addField(ArrayList<String> record, StringBuilder fieldValue) throws CSVParseException {

		record.add(fieldValue == null ? null : fieldValue.toString());

		if (fieldValue != null) {
			if (fieldValue.length() > maxSizeOfIndividualCell) {
				throw new CSVParseException("Exceeded max field size: " + fieldValue.length(), parser.lineno());
			}
		}

		if (record.size() > maxColumnsPerRow) {
			throw new CSVParseException("Exceeded max number of columns per record : " + maxColumnsPerRow,
					parser.lineno());
		}
	}

	public long getMaxRowsInFile() {
		return this.maxRowsInFile;
	}

	public void setMaxRowsInFile(long newMax) {
		this.maxRowsInFile = newMax;
	}

	public void setMaxCharsInFile(long newMax) {
		this.maxFileSizeInCharacters = newMax;
	}

	public long getFileSizeInCharacters() {
		return fileSizeInCharacters;
	}

	public int getLastRowLength() {
		return lastRowLength;
	}

	private void setLastRowLength(int lastRowLength) {
		this.lastRowLength = lastRowLength;
	}

	// *****************
	// Exception classes
	// *****************
	public static class CSVParseException extends IOException {
		private static final long serialVersionUID = -2296922457924715593L;
		final int recordNumber;

		CSVParseException(String message, int lineno) {
			super(message);
			recordNumber = lineno;
		}

		CSVParseException(int i) {
			recordNumber = i;
		}

		public int getRecordNumber() {
			return recordNumber;
		}
	}
}
