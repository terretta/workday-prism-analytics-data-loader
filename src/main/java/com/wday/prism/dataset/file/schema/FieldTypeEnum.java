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

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum FieldTypeEnum {
	TEXT("fdd7dd26156610006a12d4fd1ea300ce", "Text"), NUMERIC("32e3fa0dd9ea1000072bac410415127a", "Numeric"), BOOLEAN(
			"fdd7dd26156610006a205a3d137900d3", "Boolean"), DATE("fdd7dd26156610006a71e070b08200d6", "Date");

	static final Map<String, FieldTypeEnum> ENUM_NAME_MAP = new LinkedHashMap<String, FieldTypeEnum>();
	static {
		for (FieldTypeEnum userType : FieldTypeEnum.values()) {
			ENUM_NAME_MAP.put(userType.id, userType);
		}
	}

	public final String id;

	public final String descriptor;

	FieldTypeEnum(String id, String descriptor) {
		this.id = id;
		this.descriptor = descriptor;
	}

	public String getId() {
		return id;
	}

	public String getDescriptor() {
		return descriptor;
	}

}
