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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;

import org.apache.commons.io.input.BOMInputStream;

public class CsvRowReader {

	private static final char NEWLINE = '\n';

	private static final char SPACE = ' ';

	private final StringBuilder currentColumn = new StringBuilder();

	private final StringBuilder currentRow = new StringBuilder();

	private final char quoteChar;

	private final int delimiterChar;

	private final boolean ignoreEmptyLines;

	private final int maxLinesPerRow;

	private final char quoteEscapeChar;

	private final LineNumberReader lnr;

	private static final int bufferSize = 65536;

	public static final Charset utf8Charset = Charset.forName("UTF-8");

	private enum TokenizerState {
		NORMAL, QUOTE_MODE;
	}

	public CsvRowReader(InputStream input, Charset fileCharset, char delimiterChar, char quoteChar,
			char quoteEscapeChar) {
		this(new InputStreamReader(new BOMInputStream(new FasterBufferedInputStream(input, bufferSize), false),
				utf8Decoder(CodingErrorAction.IGNORE, fileCharset)), delimiterChar, quoteChar, quoteEscapeChar);
	}

	public CsvRowReader(final Reader reader, char delimiterChar, char quoteChar, char quoteEscapeChar) {
		this.quoteChar = quoteChar;
		this.delimiterChar = delimiterChar;
		this.ignoreEmptyLines = false;
		this.maxLinesPerRow = 100;
		this.quoteEscapeChar = quoteEscapeChar;
		lnr = new LineNumberReader(reader);
	}

	public String readRow() throws IOException, ParseException {

		currentColumn.setLength(0);
		currentRow.setLength(0);

		// read a line (ignoring empty lines/comments if necessary)
		String line;
		do {
			line = readLine();
			if (line == null) {
				return null; // EOF
			}
		} while (ignoreEmptyLines && line.length() == 0);

		currentRow.append(line);

		// process each character in the line, catering for surrounding quotes
		// (QUOTE_MODE)
		TokenizerState state = TokenizerState.NORMAL;
		int quoteScopeStartingLine = -1; // the line number where a potential multi-line cell starts
		int charIndex = 0;
		while (true) {
			boolean endOfLineReached = charIndex == line.length();

			if (endOfLineReached) {
				if (TokenizerState.NORMAL.equals(state)) {
					return getUntokenizedRow();
				} else {
					currentRow.append(NEWLINE); // specific line terminator lost, \n will have to suffice

					charIndex = 0;

					if (maxLinesPerRow > 0 && getLineNumber() - quoteScopeStartingLine + 1 >= maxLinesPerRow) {
						/*
						 * The quoted section that is being parsed spans too many lines, so to avoid
						 * excessive memory usage parsing something that is probably human error
						 * anyways, throw an exception. If each row is suppose to be a single line and
						 * this has been exceeded, throw a more descriptive exception
						 */
						String msg = maxLinesPerRow == 1
								? String.format("unexpected end of line while reading quoted column on line %d",
										getLineNumber())
								: String.format(
										"max number of lines to read exceeded while reading quoted column"
												+ " beginning on line %d and ending on line %d",
										quoteScopeStartingLine, getLineNumber());
						throw new ParseException(msg, getLineNumber());
					} else if ((line = readLine()) == null) {
						return getUntokenizedRow();
					}

					currentRow.append(line); // update untokenized CSV row

					if (line.length() == 0) {
						// consecutive newlines
						continue;
					}
				}
			}

			final char c = line.charAt(charIndex);

			if (TokenizerState.NORMAL.equals(state)) {

				/*
				 * NORMAL mode (not within quotes).
				 */

				if (c == delimiterChar) {
				} else if (c == SPACE) {
					/*
					 * Space. Remember it, then continue to next character.
					 */
				} else if (c == quoteChar) {
					/*
					 * A single quote ("). Update to QUOTESCOPE (but don't save quote), then
					 * continue to next character.
					 */
					state = TokenizerState.QUOTE_MODE;
					quoteScopeStartingLine = getLineNumber();
				} else {
				}
			} else {

				/*
				 * QUOTE_MODE (within quotes).
				 */
				if (c == quoteEscapeChar) {
					int nextCharIndex = charIndex + 1;
					boolean availableCharacters = nextCharIndex < line.length();
					boolean nextCharIsQuote = availableCharacters && line.charAt(nextCharIndex) == quoteChar;
					boolean nextCharIsEscapeQuoteChar = availableCharacters
							&& line.charAt(nextCharIndex) == quoteEscapeChar;

					if (nextCharIsQuote) {
						/*
						 * An escaped quote (e.g. "" or \"). Skip over the escape char, and add the
						 * following quote char as part of the column;
						 */
						charIndex++;
					} else if (nextCharIsEscapeQuoteChar) {
						/*
						 * A double escape (normally \\). Save the escape char, then continue to next
						 * character.
						 */
						charIndex++;
					} else if (quoteEscapeChar == quoteChar) {
						/*
						 * If the escape char is also the quote char and we didn't escape a subsequent
						 * character, then this is a lone quote and the end of the field.
						 */
						state = TokenizerState.NORMAL;
						quoteScopeStartingLine = -1; // reset ready for next multi-line cell
					} else {
						/*
						 * Escape char wasn't before either another escape char or a quote char, so
						 * process it normally.
						 */
					}
				} else if (c == quoteChar) {

					/*
					 * A single quote ("). Update to NORMAL (but don't save quote), then continue to
					 * next character.
					 */
					state = TokenizerState.NORMAL;
					quoteScopeStartingLine = -1; // reset ready for next multi-line cell

					int nextCharIndex = charIndex + 1;
					boolean availableCharacters = nextCharIndex < line.length();
					boolean nextCharIsQuote = availableCharacters && line.charAt(nextCharIndex) == quoteChar;

					if (quoteEscapeChar != quoteChar && nextCharIsQuote) {
						throw new ParseException("Encountered repeat quote char (" + quoteChar
								+ ") when quoteEscapeChar was (" + quoteEscapeChar + ")"
								+ ".  Cannot process data where quotes are escaped both with " + quoteChar
								+ " and with " + quoteEscapeChar, getLineNumber());
					}
				} else {
					/*
					 * Just a normal character, delimiter (they don't count in QUOTESCOPE) or space.
					 * Add the character, then continue to next character.
					 */
				}
			}
			charIndex++; // read next char of the line
		}
	}

	private String getUntokenizedRow() {
		return currentRow.toString();
	}

	public void close() throws IOException {
		lnr.close();
	}

	public int getLineNumber() {
		return lnr.getLineNumber();
	}

	private String readLine() throws IOException {
		return lnr.readLine();
	}

	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction, Charset fileCharset) {
		try {
			if (fileCharset == null)
				fileCharset = utf8Charset;
			if (codingErrorAction == null)
				codingErrorAction = CodingErrorAction.IGNORE;
			final CharsetDecoder encoder = fileCharset.newDecoder();
			encoder.reset();
			encoder.onUnmappableCharacter(codingErrorAction);
			encoder.onMalformedInput(codingErrorAction);
			return encoder;
		} catch (Throwable t) {
			t.printStackTrace();
			return null;
		}
	}

}
