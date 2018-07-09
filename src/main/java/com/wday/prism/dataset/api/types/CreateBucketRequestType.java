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
package com.wday.prism.dataset.api.types;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.wday.prism.dataset.file.schema.FileSchema;

public class CreateBucketRequestType {

	private String name = null;
	private Map<String, String> targetDataset = new LinkedHashMap<String, String>();
	private Map<String, String> operation = new LinkedHashMap<String, String>();
	private FileSchema schema = null;

	@JsonIgnore
	private String datasetId = null;

	@JsonIgnore
	private String uploadOperation = null;

	public Map<String, String> getTargetDataset() {
		targetDataset.put("id", getDatasetId());
		return targetDataset;
	}

	public void setTargetDataset(Map<String, String> targetDataset) {
		if (targetDataset != null && targetDataset.containsKey("id")) {
			setDatasetId(targetDataset.get("id"));
		} else {
			this.targetDataset.clear();
			setDatasetId(null);
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getOperation() {
		operation.put("id", "Operation_Type=" + getUploadOperation());
		return operation;
	}

	public void setOperation(Map<String, String> operation) {
		if (operation != null && operation.containsKey("id")) {
			setUploadOperation(operation.get("id").replace("Operation_Type=", ""));
		} else {
			this.operation.clear();
			setUploadOperation(null);
		}
	}

	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public String getUploadOperation() {
		return uploadOperation;
	}

	public void setUploadOperation(String uploadOperation) {
		if (uploadOperation != null
				&& (uploadOperation.equalsIgnoreCase("replace") || uploadOperation.equalsIgnoreCase("append"))) {
			this.uploadOperation = uploadOperation.toLowerCase();
		}
	}

	public FileSchema getSchema() {
		return schema;
	}

	public void setSchema(FileSchema schema) {
		this.schema = schema;
	}

}