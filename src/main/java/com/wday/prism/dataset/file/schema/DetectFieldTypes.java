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
package com.wday.prism.dataset.file.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import com.wday.prism.dataset.constants.Constants;
import com.wday.prism.dataset.file.loader.DatasetLoaderException;
import com.wday.prism.dataset.util.CSVReader;
import com.wday.prism.dataset.util.FileUtilsExt;

public class DetectFieldTypes {

	public static final int sampleSize = 1000;
	public static final int maxRowsToSample = sampleSize * 5;
	public static final int maxConsectiveFailures = (int) (sampleSize * .25);
	public static final String[] additionalDatePatterns = { "yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX",
			"yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
			"yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss", "MM/dd/yyyy HH:mm:ss",
			"MM/dd/yy HH:mm:ss", "MM-dd-yyyy HH:mm:ss", "MM-dd-yy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd/MM/yy HH:mm:ss",
			"dd-MM-yyyy HH:mm:ss", "dd-MM-yy HH:mm:ss", "MM/dd/yyyy", "MM/dd/yy", "dd/MM/yy", "dd/MM/yyyy",
			"MM-dd-yyyy", "MM-dd-yy", "dd-MM-yyyy", "dd-MM-yy", "M/d/yyyy HH:mm:ss", "M/d/yy HH:mm:ss",
			"M-d-yyyy HH:mm:ss", "M-d-yy HH:mm:ss", "d/M/yyyy HH:mm:ss", "d/M/yy HH:mm:ss", "d-M-yyyy HH:mm:ss",
			"d-M-yy HH:mm:ss", "M/d/yy", "M/d/yyyy", "d/M/yy", "d/M/yyyy", "M-d-yy", "M-d-yyyy", "d-M-yy", "d-M-yyyy",
			"M/dd/yyyy HH:mm:ss", "M/dd/yy HH:mm:ss", "M-dd-yyyy HH:mm:ss", "M-dd-yy HH:mm:ss", "dd/M/yyyy HH:mm:ss",
			"dd/M/yy HH:mm:ss", "dd-M-yyyy HH:mm:ss", "dd-M-yy HH:mm:ss", "M/dd/yy", "dd/M/yy", "M-dd-yy", "dd-M-yy",
			"M/dd/yyyy", "dd/M/yyyy", "M-dd-yyyy", "dd-M-yyyy", "MM/d/yyyy HH:mm:ss", "MM/d/yy HH:mm:ss",
			"MM-d-yyyy HH:mm:ss", "MM-d-yy HH:mm:ss", "d/MM/yyyy HH:mm:ss", "d/MM/yy HH:mm:ss", "d-MM-yyyy HH:mm:ss",
			"d-MM-yy HH:mm:ss", "MM/d/yy", "d/MM/yy", "MM-d-yy", "d-MM-yy", "MM/d/yyyy", "d/MM/yyyy", "MM-d-yyyy",
			"d-MM-yyyy" };

	public List<FieldType> detect(File inputCsv, FileSchema userSchema, Charset fileCharset, char delim,
			PrintStream logger) throws IOException {
		logger.println("Detecting schema from  file {" + inputCsv + "} ...");

		if (!inputCsv.exists() && !inputCsv.canRead()) {
			throw new IllegalArgumentException("Input file {" + inputCsv + "} not found or cannot be read");
		}

		InputStream is = null;
		if (FileUtilsExt.isGzipFile(inputCsv)) {
			is = new GZIPInputStream(new FileInputStream(inputCsv));
		} else {
			is = new FileInputStream(inputCsv);
		}
		List<FieldType> types = detect(is, userSchema, fileCharset, delim, logger);
		logger.println("Schema file {" + FileSchema.getSchemaFile(inputCsv, logger) + "} successfully generated...");
		return types;
	}

	public List<FieldType> detect(InputStream inputCsv, FileSchema userSchema, Charset fileCharset, char delim,
			PrintStream logger) throws IOException {

		if (fileCharset == null)
			fileCharset = Charset.forName("UTF-8");

		if (inputCsv == null) {
			throw new IllegalArgumentException("Input stream cannot be null");
		}

		LinkedList<ArrayList<String>> rows = new LinkedList<ArrayList<String>>();
		long rowCount = getSampleValuesFromCsv(inputCsv, fileCharset, delim, maxRowsToSample, rows, 0, logger);
		if (rows.isEmpty()) {
			throw new IllegalArgumentException("No rows could be read from input file");
		}

		return detect(rows, userSchema, logger);
	}

	private List<FieldType> detect(LinkedList<ArrayList<String>> rows, FileSchema userSchema, PrintStream logger)
			throws IOException {

		if (rows.isEmpty()) {
			throw new IllegalArgumentException("Input rows cannnot be empty");
		}

		LinkedList<FieldType> types = null;
		ArrayList<String> header = rows.get(0);

		if (userSchema != null && userSchema.getParseOptions() != null
				&& userSchema.getParseOptions().getHeaderLinesToIgnore() == 0) {
			List<FieldType> fields = userSchema.getFields();
			if (fields != null && !fields.isEmpty()) {
				return fields;
			}
		}

		try {

			if (userSchema != null && userSchema.getParseOptions() != null
					&& userSchema.getParseOptions().getHeaderLinesToIgnore() > 0) {
				List<FieldType> fields = userSchema.getFields();
				if (fields != null && !fields.isEmpty()) {
					int fieldCount = 0;
					for (FieldType field : fields) {
						fieldCount++;
					}

					if (header.size() != fieldCount) {
						throw new IllegalArgumentException("Input file header count {" + header.size()
								+ "} does not match json field count {" + fieldCount + "}");
					}
					return fields;
				}
			}

			// List<String> nextLine = null;
			types = new LinkedList<FieldType>();
			boolean uniqueColumnFound = false;

			if (header == null)
				return types;

			if (header.size() > 1000) {
				throw new IllegalArgumentException(
						"Input file cannot contain more than 1000 columns. found {" + header.size() + "} columns");
			}

			String devNames[] = FileSchema.createUniqueDevName(header);
			for (int i = 0; i < header.size(); i++) {
				if (i == 0) {
					if (header.get(i) != null && header.get(i).startsWith("#"))
						header.set(i, header.get(i).replace("#", ""));
				}

				LinkedList<String> columnValues = new LinkedList<String>();
				LinkedHashSet<String> uniqueColumnValues = new LinkedHashSet<String>();
				logger.print("Column: " + header.get(i));
				long rowCount = getColumnValuefromCsv(rows, i, columnValues, 0, logger);

				logger.print(", ");
				FieldType newField = null;
				int prec = detectTextPrecision(columnValues);
				DecimalFormat df = null;
				boolean isPercent = isPercent(columnValues);
				BigDecimal bd = detectNumeric(columnValues, null, isPercent);
				if (bd == null) {
					df = (DecimalFormat) NumberFormat.getInstance(Locale.getDefault());
					df.setParseBigDecimal(true);
					bd = detectNumeric(columnValues, df, isPercent);
				} else {
					if (isPercent)
						df = (DecimalFormat) NumberFormat.getPercentInstance(Locale.getDefault());
				}
				if (bd == null) {
					df = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.getDefault());
					df.setParseBigDecimal(true);
					bd = detectNumeric(columnValues, df, isPercent);
				}
				if (bd == null) {
					df = (DecimalFormat) NumberFormat.getPercentInstance(Locale.getDefault());
					df.setParseBigDecimal(true);
					bd = detectNumeric(columnValues, df, isPercent);
				}

				if (bd != null) {
					newField = FieldType.getNumericDataType(devNames[i], prec, bd.scale(), null);
					String format = "n/a";
					if (df != null) {
						if (!isPercent && bd.scale() == 0) {
							df = (DecimalFormat) NumberFormat.getIntegerInstance();
						}
						newField.setDecimalSeparator(df.getDecimalFormatSymbols().getDecimalSeparator() + "");
						format = df.toPattern().replace("¤", df.getDecimalFormatSymbols().getCurrencySymbol());
						newField.setParseFormat(format);
					}
					logger.println("Type: Decimal, Scale: " + bd.scale() + " Format: " + format);
				} else {
					SimpleDateFormat sdf = null;
					sdf = detectDate(columnValues);
					if (sdf != null) {
						newField = FieldType.getDateDataType(devNames[i], sdf.toPattern(), null);
						logger.println("Type: Date, Format: " + sdf.toPattern());
					}

					if (newField == null) {
						Boolean bool = detectBoolean(columnValues);
						if (bool != null) {
							newField = FieldType.getBooleanDataType(devNames[i], null);
							logger.println("Type: Boolean");
						}
					}

					if (newField == null) {
						newField = FieldType.getTextDataType(devNames[i], null, null);
						if (prec > 255) {
							logger.println(
									"Type: Text, Precison: " + 255 + " (Column will be truncated to 255 characters)"
											+ (newField.isUniqueId ? ", isUniqueId=true" : ""));
						} else {
							logger.println(
									"Type: Text, Precison: " + prec + (newField.isUniqueId ? ", isUniqueId=true" : ""));
						}
						newField.setPrecision(255); // Assume upper limit for
													// precision of text fields
													// even if the values may be
													// smaller
					}
				}

				if (newField != null) {
					if (header.get(i) != null && !header.get(i).trim().isEmpty()) {
						if (header.get(i).length() > 255) {
							newField.setLabel(header.get(i).substring(0, 255));
							newField.setDescription(header.get(i).substring(0, 255));
						} else {
							newField.setLabel(header.get(i));
							newField.setDescription(header.get(i));
						}
					}
					newField.setOrdinal(i + 1);

					types.add(newField);
				}
			} // end for

		} finally {
			logger.println("");
		}
		return types;
	}

	public BigDecimal detectNumeric(LinkedList<String> columnValues, DecimalFormat df, boolean isPercent) {

		BigDecimal maxScale = null;
		BigDecimal maxPrecision = null;
		int consectiveFailures = 0;
		int success = 0;
		int absoluteMaxScale = 8;
		int absoluteMaxScaleExceededCount = 0;
		int absoluteMaxPrecision = FieldType.max_precision;

		for (int j = 0; j < columnValues.size(); j++) {
			String columnValue = columnValues.get(j);
			if (columnValue == null || columnValue.isEmpty())
				continue;

			if (columnValue.length() > absoluteMaxPrecision)
				continue;

			BigDecimal bd = null;
			try {
				if (df == null) {
					if (isPercent) {
						bd = new BigDecimal(columnValue.substring(0, columnValue.length() - 1).trim());
						bd = bd.divide(new BigDecimal(100));
					} else {
						bd = new BigDecimal(columnValue);
					}
				} else {
					bd = (BigDecimal) df.parse(columnValue);
					String tmp = df.format(bd);
					if (!tmp.equals(columnValue)) {
						int decimalIndex = columnValue.indexOf(df.getDecimalFormatSymbols().getDecimalSeparator());
						if (decimalIndex != -1) {
							int compareLength = columnValue.length();
							for (int cnt = columnValue.length() - 1; cnt > decimalIndex; cnt--) {
								if (columnValue.charAt(cnt) == '0') {
									compareLength--;
								}
							}
							if (!tmp.equalsIgnoreCase(columnValue.substring(0, compareLength - 1))) {
								throw new ParseException("Invalid numeric value {" + columnValue + "}", 0);
							}
						} else {
							throw new ParseException("Invalid numeric value {" + columnValue + "}", 0);
						}
					}
				}

				if (bd.scale() > absoluteMaxScale)
					absoluteMaxScaleExceededCount++;

				if (bd.precision() > absoluteMaxPrecision || bd.scale() > absoluteMaxPrecision)
					continue;

				// logger.println("Value: {"+columnValue+"} Scale: {"+bd.scale()+"}");
				if (maxScale == null || bd.scale() > maxScale.scale())
					maxScale = bd;

				if (maxPrecision == null || bd.precision() > maxPrecision.precision())
					maxPrecision = bd;

				success++;
				consectiveFailures = 0; // reset the failure count
			} catch (Throwable t) {
				consectiveFailures++;
			}

			if (consectiveFailures >= maxConsectiveFailures) {

				return null;
			}
		}

		if (maxScale == null || maxPrecision == null)
			return null;

		if ((1.0 * absoluteMaxScaleExceededCount / columnValues.size()) > 0.1) {
			return null;
		}

		int maxScaleCalculated = absoluteMaxScale;
		if (maxPrecision != null && maxPrecision.precision() > maxPrecision.scale()) {
			maxScaleCalculated = absoluteMaxPrecision - (maxPrecision.precision() - maxPrecision.scale());
			if (maxScaleCalculated > absoluteMaxScale)
				maxScaleCalculated = absoluteMaxScale;
			else if (maxScaleCalculated < 2)
				maxScaleCalculated = 2;
		}

		if (maxScale != null && maxScale.scale() > maxScaleCalculated) {
			maxScale = maxScale.setScale(maxScaleCalculated, RoundingMode.HALF_EVEN);
		}

		maxPrecision = maxPrecision.setScale(maxScale.scale(), RoundingMode.HALF_EVEN);

		if ((1.0 * success / columnValues.size()) > 0.95) {
			return maxPrecision;
		} else {
			return null;
		}
	}

	public SimpleDateFormat detectDate(LinkedList<String> columnValues) {

		LinkedHashSet<SimpleDateFormat> dateFormats = getSuportedDateFormats();

		for (int j = 0; j < columnValues.size(); j++) {
			String columnValue = columnValues.get(j);
			Date dt = null;
			SimpleDateFormat dtf = null;
			if (columnValue == null || columnValue.isEmpty())
				continue;

			if (columnValue.length() < 6 || columnValue.length() > 30)
				continue;

			for (SimpleDateFormat sdf : dateFormats) {
				try {
					dt = sdf.parse(columnValue);
					String tmpDate = sdf.format(dt);
					if (tmpDate.length() == columnValue.length()) {
						dtf = sdf;
						break;
					} else
						dt = null;
				} catch (Throwable t) {
					dtf = null;
					dt = null;
				}
			}

			if (dt != null) {
				int consectiveFailures = 0;
				int success = 0;
				for (int k = 0; k < columnValues.size(); k++) {
					columnValue = columnValues.get(k);
					if (columnValue == null || columnValue.isEmpty())
						continue;

					try {
						Date dt1 = dtf.parse(columnValue);
						String tmpDate = dtf.format(dt1);
						if (tmpDate.length() == columnValue.length()) {
							success++;
							consectiveFailures = 0; // reset the failure count
						}
					} catch (ParseException e) {
						consectiveFailures++;
					}

					if (consectiveFailures >= maxConsectiveFailures)
						break;
				}
				if (!(consectiveFailures >= maxConsectiveFailures) && (1.0 * success / columnValues.size()) > 0.95) {
					return dtf;
				} else {
					dateFormats.remove(dtf); // lets not try this format again
					dtf = null;
					dt = null;
				}
			}
		}
		return null;

	}

	private Boolean detectBoolean(LinkedList<String> columnValues) {

		int consectiveFailures = 0;
		int success = 0;

		for (int j = 0; j < columnValues.size(); j++) {
			String columnValue = columnValues.get(j);
			if (columnValue == null || columnValue.isEmpty())
				continue;

			try {
				if (Boolean.TRUE.toString().equalsIgnoreCase(columnValue)
						|| Boolean.FALSE.toString().equalsIgnoreCase(columnValue)) {
					success++;
					consectiveFailures = 0; // reset the failure count
				} else {
					consectiveFailures++;
				}
			} catch (Throwable t) {
				consectiveFailures++;
			}

			if (consectiveFailures >= maxConsectiveFailures) {
				return null;
			}
		}

		if ((1.0 * success / columnValues.size()) > 0.95) {
			return Boolean.TRUE;
		} else {
			return null;
		}
	}

	public int detectTextPrecision(List<String> columnValues) {
		int length = 0;
		for (String columnValue : columnValues) {
			if (columnValue != null) {
				if (columnValue.length() > length)
					length = columnValue.length();
			}
		}
		return length;
	}

	public static LinkedHashSet<SimpleDateFormat> getSuportedDateFormats() {
		LinkedHashSet<SimpleDateFormat> dateFormats = new LinkedHashSet<SimpleDateFormat>();
		Locale[] locales = Locale.getAvailableLocales();
		for (int i = 0; i < locales.length; i++) {
			if (locales[i].getCountry().length() == 0) {
				continue; // Skip language-only locales
			}
			SimpleDateFormat sdf = (SimpleDateFormat) DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
					DateFormat.MEDIUM, locales[i]);
			String pat = sdf.toPattern();
			if (!pat.contains("MMM")) {
				SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
				tempSdf.setLenient(false);
				dateFormats.add(tempSdf);
			}
		}

		for (int i = 0; i < additionalDatePatterns.length; i++) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(additionalDatePatterns[i]);
				SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
				tempSdf.setLenient(false);
				dateFormats.add(tempSdf);
			} catch (Throwable t1) {
				t1.printStackTrace();
			}
		}

		for (int i = 0; i < locales.length; i++) {
			if (locales[i].getCountry().length() == 0) {
				continue; // Skip language-only locales
			}
			SimpleDateFormat sdf = (SimpleDateFormat) DateFormat.getDateInstance(DateFormat.MEDIUM, locales[i]);
			String pat = sdf.toPattern();
			if (!pat.contains("MMM")) {
				SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
				tempSdf.setLenient(false);
				dateFormats.add(tempSdf);
			}
		}
		return dateFormats;
	}

	public boolean isPercent(LinkedList<String> columnValues) {
		int success = 0;
		for (int j = 0; j < columnValues.size(); j++) {
			if (columnValues.get(j).endsWith("%")) {
				success++;
			}
		}
		if ((1.0 * success / columnValues.size()) > 0.95) {
			return true;
		}
		return false;
	}

	private long getSampleValuesFromCsv(InputStream inputCsv, Charset fileCharset, char delim, long maxRowsToSample,
			LinkedList<ArrayList<String>> rows, long rowCountToSkip, PrintStream logger) {
		CSVReader reader = null;
		long rowCount = 0L;
		long errorRowCount = 0L;
		try {

			if (inputCsv == null) {
				throw new IllegalArgumentException("Input stream cannot be null");
			}

			if (delim == 0) {
				throw new IllegalArgumentException("delimiter cannot be empty");
			}

			if (fileCharset == null) {
				throw new IllegalArgumentException("fileCharset cannot be null");
			}

			reader = new CSVReader(inputCsv, fileCharset.name(), new char[] { delim });
			boolean hasmore = true;
			while (hasmore) {

				try {
					rowCount++;
					ArrayList<String> nextLine = reader.nextRecord();
					if (nextLine == null || nextLine.isEmpty()) {
						rowCount--;
						break;
					}

					if (rowCount <= rowCountToSkip)
						continue;

					rows.add(nextLine);
				} catch (Throwable t) {
					errorRowCount++;
					if (errorRowCount >= Constants.max_error_threshhold) {
						logger.println(
								"\n*******************************************************************************");
						logger.println("Max error threshold reached. Aborting processing");
						logger.println(
								"*******************************************************************************\n");
						throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
					} else if (t instanceof MalformedInputException) {
						logger.println(
								"\n*******************************************************************************");
						logger.println("The input file is not {" + fileCharset
								+ "} encoded. Please save it as correct encoding first");
						logger.println(
								"*******************************************************************************\n");
						throw new DatasetLoaderException("The input file is not utf8 encoded");
					} else {
						logger.println("Line {" + (rowCount) + "} has error {" + t + "}");
					}
				}

				if (rowCount > maxRowsToSample) {
					break;
				}
			} // end while

		} catch (Throwable t) {
			logger.println("Line {" + (rowCount) + "} has error {" + t + "}");
		} finally {
			if (reader != null) {
				try {
					reader.finalise();
				} catch (IOException e) {
				}
			}
			reader = null;
		}
		return rowCount;
	}

	private long getColumnValuefromCsv(LinkedList<ArrayList<String>> rows, int columnIndex,
			LinkedList<String> columnValues, long rowCountToSkip, PrintStream logger) {
		ArrayList<String> header = rows.get(0);
		int rowCount = 0;
		int errorRowCount = 0;
		try {

			boolean hasmore = true;

			if (columnIndex >= header.size())
				return rowCount;

			while (hasmore) {
				rowCount++;

				if (rowCount <= rowCountToSkip)
					continue;

				if (rowCount >= rows.size())
					break;

				try {

					List<String> nextLine = rows.get(rowCount);
					if (nextLine == null || nextLine.isEmpty())
						break;
					if (columnIndex >= nextLine.size())
						continue; // This line does not have enough columns
					if (nextLine.get(columnIndex) != null && !nextLine.get(columnIndex).trim().isEmpty()) {
						columnValues.add(nextLine.get(columnIndex).trim());
					}
				} catch (Throwable t) {
					if (errorRowCount >= Constants.max_error_threshhold) {
						logger.println(
								"\n*******************************************************************************");
						logger.println("Max error threshold reached. Aborting processing");
						logger.println(
								"*******************************************************************************\n");
						throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
					} else if (t instanceof MalformedInputException) {
						logger.println(
								"\n*******************************************************************************");
						logger.println("The input file is not utf8 encoded. Please save it as UTF8 file first");
						logger.println(
								"*******************************************************************************\n");
						throw new DatasetLoaderException("The input file is not utf8 encoded");
					} else {
						logger.println("Line {" + (rowCount) + "} has error {" + t + "}");
					}
				}
			}

		} catch (Throwable t) {
			logger.println("Line {" + (rowCount) + "} has error {" + t + "}");
		} finally {
		}
		return rowCount;
	}

	private void getUniqueColumnValues(LinkedList<String> columnValues, LinkedHashSet<String> uniqueColumnValues) {
		for (String columnValue : columnValues) {
			if (columnValue != null) {
				uniqueColumnValues.add(columnValue);
			}
		}
	}

}
