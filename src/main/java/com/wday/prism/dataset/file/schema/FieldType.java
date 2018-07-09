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

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * The Class FieldType.
 */
public class FieldType {

	/** The Constant max_precision. */
	public static final int max_precision = 38;

	private int ordinal = 0;
	private String name = null; // Required
	private String description = null; // Optional
	private int precision = 0; // Required if type is Numeric, the number 256.99 has a precision of 5
	private int scale = 0; // Required if type is Numeric, the number 256.99 has a scale of 2
	private String defaultValue = null;
	private String parseFormat = null;
	@JsonDeserialize(using = FieldTypeEnumDeserializer.class)
	private FieldTypeEnum type = null; // Required - Text, Decimal, Integer, Date, Boolean

	@JsonIgnore
	private String label = null; // Required
	@JsonIgnore
	public boolean isUniqueId = false; // Optional
	@JsonIgnore
	public boolean isMultiValue = false; // Optional
	@JsonIgnore
	private String multiValueSeparator = null; // Optional - only used if IsMultiValue = true separator
	@JsonIgnore
	public boolean canTruncateValue = true; // Optional
	@JsonIgnore
	public boolean isSkipped = false; // Optional
	@JsonIgnore
	private String decimalSeparator = ".";

	/** The compiled date format. */
	private transient SimpleDateFormat compiledDateFormat = null;

	/** The compiled number format. */
	private transient DecimalFormat compiledNumberFormat = null;

	/** The default date. */
	private transient Date defaultDate = null;

	/**
	 * Instantiates a new field type.
	 */
	public FieldType() {
		super();
	}

	/**
	 * Gets the string key data type.
	 *
	 * @param name
	 *            the name
	 * @param multivalueSeperator
	 *            the multivalue seperator
	 * @param defaultValue
	 *            the default value
	 * @return the field type
	 */
	public static FieldType getTextDataType(String name, String multivalueSeperator, String defaultValue) {
		FieldType kdt = new FieldType(name);
		kdt.type = FieldTypeEnum.TEXT;
		if (multivalueSeperator != null && multivalueSeperator.length() != 0) {
			kdt.multiValueSeparator = multivalueSeperator;
			kdt.isMultiValue = true;
		}
		kdt.setDefaultValue(defaultValue);
		kdt.setLabel(name);
		kdt.setDescription(name);
		return kdt;
	}

	/**
	 * Gets the measure key data type.
	 *
	 * @param name
	 *            the name
	 * @param precision
	 *            the precision
	 * @param scale
	 *            the scale
	 * @param defaultValue
	 *            the default value
	 * @return the field type
	 */
	public static FieldType getNumericDataType(String name, int precision, int scale, BigDecimal defaultValue) {
		FieldType kdt = new FieldType(name);
		kdt.type = FieldTypeEnum.NUMERIC;

		if (precision <= 0 || precision > max_precision)
			precision = max_precision;

		if (scale <= 0)
			scale = 0;

		if (scale == 0 && precision <= 9)
			precision = 9;
		else if (scale == 0 && precision <= 18)
			precision = 18;

		// If scale greater than precision, then force the scale to be 1 less than
		// precision
		if (scale > precision)
			scale = (precision > 1) ? (precision - 1) : 0;

		kdt.setPrecision(precision);
		kdt.setScale(scale);
		if (defaultValue != null)
			kdt.setDefaultValue(defaultValue.toPlainString());
		kdt.setLabel(name);
		kdt.setDescription(name);
		return kdt;
	}

	/**
	 * Gets the date key data type.
	 *
	 * @param name
	 *            the name
	 * @param format
	 *            the format
	 * @param defaultValue
	 *            the default value
	 * @return the field type
	 */
	public static FieldType getDateDataType(String name, String format, Date defaultValue) {
		FieldType kdt = new FieldType(name);
		kdt.type = FieldTypeEnum.DATE;
		SimpleDateFormat sdt = new SimpleDateFormat(format);
		kdt.setParseFormat(format);
		kdt.setLabel(name);
		kdt.setDescription(name);

		if (defaultValue != null)
			kdt.setDefaultValue(sdt.format(defaultValue));

		return kdt;
	}

	public static FieldType getBooleanDataType(String name, Boolean defaultValue) {
		FieldType kdt = new FieldType(name);
		kdt.type = FieldTypeEnum.BOOLEAN;
		if (defaultValue != null) {
			if (defaultValue == Boolean.TRUE) {
				kdt.setDefaultValue(Boolean.TRUE.toString());
			} else {
				kdt.setDefaultValue(Boolean.FALSE.toString());
			}
		}
		kdt.setLabel(name);
		kdt.setDescription(name);
		return kdt;
	}

	/**
	 * Instantiates a new field type.
	 *
	 * @param old
	 *            the old
	 */
	public FieldType(FieldType old) {
		super();
		if (old != null) {
			this.canTruncateValue = old.canTruncateValue;
			this.compiledDateFormat = old.compiledDateFormat;
			this.decimalSeparator = old.decimalSeparator;
			this.defaultDate = old.defaultDate;
			this.defaultValue = old.defaultValue;
			this.description = old.description;
			this.parseFormat = old.parseFormat;
			this.isMultiValue = old.isMultiValue;
			this.isSkipped = old.isSkipped;
			this.isUniqueId = old.isUniqueId;
			this.label = old.label;
			this.multiValueSeparator = old.multiValueSeparator;
			this.name = old.name;
			this.precision = old.precision;
			this.scale = old.scale;
			this.type = old.type;
			this.ordinal = old.ordinal;

			if (!this.equals(old)) {
				System.out.println("FieldType Copy constructor is missing functionality");
			}
		}
	}

	/**
	 * Instantiates a new field type.
	 *
	 * @param name
	 *            the name
	 */
	FieldType(String name) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("field name is null {" + name + "}");
		}
		if (name.length() > 255) {
			throw new IllegalArgumentException("field name cannot be greater than 255 characters");
		}
		this.name = name;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getName()
	 */
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setName(java.lang.String)
	 */
	public void setName(String name) {
		this.name = name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getType()
	 */
	public FieldTypeEnum getType() {
		if (type == null) {
			type = FieldTypeEnum.TEXT;
		}
		return type;
	}

	public void setType(FieldTypeEnum type) {
		this.type = type;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getPrecision()
	 */
	public int getPrecision() {
		return precision;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#setPrecision(int)
	 */
	public void setPrecision(int precision) {
		this.precision = precision;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getScale()
	 */
	public int getScale() {
		return scale;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#setScale(int)
	 */
	public void setScale(int scale) {
		this.scale = scale;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getFormat()
	 */
	public String getParseFormat() {
		return parseFormat;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setFormat(java.lang.String)
	 */
	public void setParseFormat(String format) {
		if (this.type != null && this.type == FieldTypeEnum.DATE && format != null
				&& !format.equalsIgnoreCase("null")) {
			compiledDateFormat = new SimpleDateFormat(format);
			compiledDateFormat.setTimeZone(TimeZone.getTimeZone("GMT")); // All dates must be in GMT
			compiledDateFormat.setLenient(false);
			if (this.defaultValue != null && !this.defaultValue.isEmpty()
					&& !this.defaultValue.equalsIgnoreCase("null")) {
				try {
					this.defaultDate = compiledDateFormat.parse(this.defaultValue);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e.toString());
				}
			}
		} else if (this.type != null && this.type == FieldTypeEnum.NUMERIC && format != null
				&& !format.equalsIgnoreCase("null")) {
			try {
				DecimalFormat nf = (DecimalFormat) NumberFormat.getInstance();
				nf.applyPattern(format);
			} catch (Throwable e) {
				throw new IllegalArgumentException(e.toString());
			}
		}
		this.parseFormat = format;
	}

	/**
	 * Gets the compiled number format.
	 *
	 * @return the compiled number format
	 */
	@JsonIgnore
	public DecimalFormat getCompiledNumberFormat() {
		if (compiledNumberFormat == null) {
			if (this.type != null && this.type == FieldTypeEnum.NUMERIC && parseFormat != null
					&& !parseFormat.isEmpty()) {
				DecimalFormat indf = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.getDefault());
				if (!parseFormat.contains(indf.getDecimalFormatSymbols().getCurrencySymbol())) {
					indf = (DecimalFormat) NumberFormat.getInstance();
				}

				if (parseFormat.contains(indf.getDecimalFormatSymbols().getPercent() + "")) {
					indf = (DecimalFormat) NumberFormat.getPercentInstance();
				}
				indf.applyPattern(parseFormat);
				indf.setParseBigDecimal(true);
				compiledNumberFormat = indf;
			}
		}
		return compiledNumberFormat;
	}

	/**
	 * Gets the compiled date format.
	 *
	 * @return the compiled date format
	 */
	@JsonIgnore
	public SimpleDateFormat getCompiledDateFormat() {
		if (compiledDateFormat == null) {
			if (this.type != null && this.type == FieldTypeEnum.DATE && parseFormat != null) {
				compiledDateFormat = new SimpleDateFormat(parseFormat);
				// compiledDateFormat.setTimeZone(TimeZone.getTimeZone("GMT")); // All dates
				// must be in GMT
			}
		}
		return compiledDateFormat;
	}

	/**
	 * Gets the default date.
	 *
	 * @return the default date
	 */
	@JsonIgnore
	public Date getDefaultDate() {
		if (defaultDate == null) {
			if (getCompiledDateFormat() != null && defaultValue != null && !defaultValue.isEmpty()) {
				try {
					this.defaultDate = getCompiledDateFormat().parse(defaultValue);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e.toString());
				}
			}
		}
		return defaultDate;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getMultiValueSeparator()
	 */
	public String getMultiValueSeparator() {
		return multiValueSeparator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setMultiValueSeparator(java.
	 * lang.String)
	 */
	public void setMultiValueSeparator(String multiValueSeparator) {
		this.multiValueSeparator = multiValueSeparator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getDefaultValue()
	 */
	public String getDefaultValue() {
		return defaultValue;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setDefaultValue(java.lang.
	 * String)
	 */
	public void setDefaultValue(String defaultValue) {
		if (defaultValue != null && !defaultValue.equalsIgnoreCase("null")) {
			if (this.type != null && this.type == FieldTypeEnum.DATE && getCompiledDateFormat() != null) {
				try {
					this.defaultDate = getCompiledDateFormat().parse(defaultValue);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e.toString());
				}
			}

			if (this.type != null && (this.type == FieldTypeEnum.NUMERIC)) {
				try {
					new BigDecimal(defaultValue);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(e.toString());
				}
			}

			if (this.type != null && (this.type == FieldTypeEnum.BOOLEAN)) {
				if (!defaultValue.equalsIgnoreCase(Boolean.TRUE.toString())
						&& !defaultValue.equalsIgnoreCase(Boolean.FALSE.toString())) {
					throw new IllegalArgumentException("Invalid Boolean default value: '" + defaultValue + "'");
				}
			}

		}
		this.defaultValue = defaultValue;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getLabel()
	 */
	public String getLabel() {
		return label;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setLabel(java.lang.String)
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getDescription()
	 */
	public String getDescription() {
		return description;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setDescription(java.lang.
	 * String)
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/*
	 * @JsonIgnore public boolean isNillable() { return isNillable; }
	 * 
	 * @JsonIgnore public void setNillable(boolean isNillable) { this.isNillable =
	 * isNillable; }
	 */

	/**
	 * Checks if is unique id.
	 *
	 * @return true, if is unique id
	 */
	@JsonIgnore
	public boolean isUniqueId() {
		return isUniqueId;
	}

	/**
	 * Sets the unique id.
	 *
	 * @param isUniqueId
	 *            the new unique id
	 */
	@JsonIgnore
	public void setUniqueId(boolean isUniqueId) {
		this.isUniqueId = isUniqueId;
	}

	/**
	 * Checks if is multi value.
	 *
	 * @return true, if is multi value
	 */
	@JsonIgnore
	public boolean isMultiValue() {
		return isMultiValue;
	}

	/**
	 * Sets the multi value.
	 *
	 * @param isMultiValue
	 *            the new multi value
	 */
	@JsonIgnore
	public void setMultiValue(boolean isMultiValue) {
		this.isMultiValue = isMultiValue;
	}

	/**
	 * Checks if is can truncate value.
	 *
	 * @return true, if is can truncate value
	 */
	@JsonIgnore
	public boolean isCanTruncateValue() {
		return canTruncateValue;
	}

	/**
	 * Sets the can truncate value.
	 *
	 * @param canTruncateValue
	 *            the new can truncate value
	 */
	@JsonIgnore
	public void setCanTruncateValue(boolean canTruncateValue) {
		this.canTruncateValue = canTruncateValue;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sforce.dataset.loader.file.schema.FieldType#getDecimalSeparator()
	 */
	public String getDecimalSeparator() {
		return decimalSeparator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sforce.dataset.loader.file.schema.FieldType#setDecimalSeparator(java.lang
	 * .String)
	 */
	public void setDecimalSeparator(String decimalSeparator) {
		if (decimalSeparator == null || decimalSeparator.isEmpty())
			this.decimalSeparator = ".";
		else
			this.decimalSeparator = decimalSeparator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (canTruncateValue ? 1231 : 1237);
		result = prime * result + ((decimalSeparator == null) ? 0 : decimalSeparator.hashCode());
		result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
		result = prime * result + ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((parseFormat == null) ? 0 : parseFormat.hashCode());
		result = prime * result + (isMultiValue ? 1231 : 1237);
		result = prime * result + (isSkipped ? 1231 : 1237);
		result = prime * result + (isUniqueId ? 1231 : 1237);
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result + ((multiValueSeparator == null) ? 0 : multiValueSeparator.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + precision;
		result = prime * result + scale;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FieldType other = (FieldType) obj;
		if (defaultValue == null) {
			if (other.defaultValue != null) {
				return false;
			}
		} else if (!defaultValue.equals(other.defaultValue)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (parseFormat == null) {
			if (other.parseFormat != null) {
				return false;
			}
		} else if (!parseFormat.equals(other.parseFormat)) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (precision != other.precision) {
			return false;
		}
		if (ordinal != other.ordinal) {
			return false;
		}
		if (scale != other.scale) {
			return false;
		}
		if (type == null) {
			if (other.type != null) {
				return false;
			}
		} else if (!type.equals(other.type)) {
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FieldType [name=" + name + ",Ordinal=" + ordinal + ",label=" + label + ", type=" + type
				+ ", defaultValue=" + defaultValue + ", scale=" + scale + ", format=" + parseFormat + "]";
	}

}