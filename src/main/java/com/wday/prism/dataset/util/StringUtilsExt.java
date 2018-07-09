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
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

public class StringUtilsExt {

	/** The Constant EOF. */
	private static final int EOF = -1;

	/** The Constant LF. */
	private static final char LF = '\n';

	/** The Constant CR. */
	private static final char CR = '\r';

	/** The Constant QUOTE. */
	private static final char QUOTE = '"';

	/** The Constant COMMA. */
	private static final char COMMA = ',';

	/** The Constant DEFAULT_BUFFER_SIZE. */
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

	/** The Constant utf8Charset. */
	public static final Charset utf8Charset = Charset.forName("UTF-8");

	public static String getHeartBeatFriendlyString(String content) {
		if (content != null && !content.isEmpty()) {
			content = replaceString(content, "" + CR, "");
			content = replaceString(content, "" + LF, "; ");
			content = replaceString(content, "" + '{', "[");
			content = replaceString(content, "" + '}', "]");
			content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}

	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction, String fileCharset) {
		return utf8Decoder(codingErrorAction, Charset.forName(fileCharset));
	}

	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction) {
		return utf8Decoder(codingErrorAction, utf8Charset);
	}

	/**
	 * Sort by value.
	 *
	 * @param <K>
	 *            the key type
	 * @param <V>
	 *            the value type
	 * @param map
	 *            the map
	 * @return the map
	 */
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	/**
	 * Read input from console.
	 *
	 * @param prompt
	 *            the prompt
	 * @return the string
	 */
	public static String readInputFromConsole(String prompt) {
		String line = null;
		Console c = System.console();
		if (c != null) {
			line = c.readLine(prompt);
		} else {
			System.out.print(prompt);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return line;
	}

	/**
	 * Read password from console.
	 *
	 * @param prompt
	 *            the prompt
	 * @return the string
	 */
	public static String readPasswordFromConsole(String prompt) {
		String line = null;
		Console c = System.console();
		if (c != null) {
			char[] tmp = c.readPassword(prompt);
			if (tmp != null)
				line = new String(tmp);
		} else {
			System.out.print(prompt);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				// Ignore
			}
		}
		return line;
	}

	/**
	 * Replace special characters.
	 *
	 * @param inString
	 *            the in string
	 * @return the string
	 */
	public static String replaceSpecialCharacters(String inString) {
		String outString = inString;
		try {
			if (inString != null && !inString.trim().isEmpty()) {
				char[] outStr = new char[inString.length()];
				int index = 0;
				for (char ch : inString.toCharArray()) {
					if (!Character.isLetterOrDigit((int) ch)) {
						outStr[index] = '_';
					} else {
						outStr[index] = ch;
					}
					index++;
				}
				outString = new String(outStr);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return outString;
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
	private static String replaceString(String original, String pattern, String replace) {
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

	/**
	 * To string.
	 *
	 * @param input
	 *            the input
	 * @return the string
	 */
	public static String toString(Reader input) {
		if (input == null)
			return null;
		try {
			StringBuffer sbuf = new StringBuffer();
			char[] cbuf = new char[DEFAULT_BUFFER_SIZE];
			int count = -1;
			try {
				int n;
				while ((n = input.read(cbuf)) != -1) {
					sbuf.append(cbuf, 0, n);
					count = ((count == -1) ? n : (count + n));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (count == -1)
				return null;
			else
				return sbuf.toString();
		} finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * To bytes.
	 *
	 * @param input
	 *            the input
	 * @return the byte[]
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static byte[] toBytes(InputStream input) throws IOException {
		if (input == null)
			return null;
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
			int n = 0;
			int count = -1;
			while (EOF != (n = input.read(buffer))) {
				output.write(buffer, 0, n);
				count = ((count == -1) ? n : (count + n));
			}
			output.flush();
			if (count == -1)
				return null;
			else
				return output.toByteArray();
		} finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * Serialize.
	 *
	 * @param obj
	 *            the obj
	 * @return the byte[]
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}

	/**
	 * Pad right.
	 *
	 * @param s
	 *            the s
	 * @param n
	 *            the n
	 * @return the string
	 */
	public static String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}

	/**
	 * Pad left.
	 *
	 * @param s
	 *            the s
	 * @param n
	 *            the n
	 * @return the string
	 */
	public static String padLeft(String s, int n) {
		return String.format("%1$" + n + "s", s);
	}

	/**
	 * Utf8 encoder.
	 *
	 * @param codingErrorAction
	 *            the coding error action
	 * @return the charset encoder
	 */
	public static CharsetEncoder utf8Encoder(CodingErrorAction codingErrorAction) {
		try {
			if (codingErrorAction == null)
				codingErrorAction = CodingErrorAction.REPORT;
			final CharsetEncoder encoder = utf8Charset.newEncoder();
			encoder.reset();
			encoder.onUnmappableCharacter(codingErrorAction);
			encoder.onMalformedInput(codingErrorAction);
			return encoder;
		} catch (Throwable t) {
			t.printStackTrace();
			return null;
		}
	}

	/**
	 * Utf8 decoder.
	 *
	 * @param codingErrorAction
	 *            the coding error action
	 * @param fileCharset
	 *            the file charset
	 * @return the charset decoder
	 */
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

	/**
	 * To bytes.
	 *
	 * @param value
	 *            the value
	 * @param codingErrorAction
	 *            the coding error action
	 * @return the byte[]
	 */
	public static byte[] toBytes(String value, CodingErrorAction codingErrorAction) {
		if (value != null) {
			try {
				if (codingErrorAction == null)
					codingErrorAction = CodingErrorAction.IGNORE;
				final CharsetEncoder encoder = utf8Encoder(codingErrorAction);
				final ByteBuffer b = encoder.encode(CharBuffer.wrap(value));
				final byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return bytes;
			} catch (Throwable t) {
				t.printStackTrace();
				return value.getBytes(utf8Charset);
			}
		}
		return null;
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

}
