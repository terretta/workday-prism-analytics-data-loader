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

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wday.prism.dataset.file.schema.FieldType;

public class DatasetType {
	private String id = null;
	private String name = null;
	private String description = null;
	private String displayName = null;
	private String documentation = null;	
	private boolean isEmpty = false;
	private boolean isPublished = false;
	private boolean isAPIWritable = false;
	private Map<?, ?> sourceType = null;
	private String createdMoment = null;
	private Map<?, ?> createdBy = null;
	private String updatedMoment = null;
	private Map<?, ?> updatedBy = null;
	private PermissionsType permissions = null;
	private List<FieldType> fields;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getDocumentation() {
		return documentation;
	}

	public void setDocumentation(String documentation) {
		this.documentation = documentation;
	}

	public boolean getIsEmpty() {
		return isEmpty;
	}

	public void setIsEmpty(boolean isEmpty) {
		this.isEmpty = isEmpty;
	}

	public boolean getIsPublished() {
		return isPublished;
	}

	public void setIsPublished(boolean isPublished) {
		this.isPublished = isPublished;
	}

	public Map<?, ?> getSourceType() {
		return sourceType;
	}

	public void setSourceType(Map<?, ?> sourceType) {
		this.sourceType = sourceType;
	}

	public String getCreatedMoment() {
		return createdMoment;
	}

	public void setCreatedMoment(String createdMoment) {
		this.createdMoment = createdMoment;
	}

	public Map<?, ?> getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(Map<?, ?> createdBy) {
		this.createdBy = createdBy;
	}

	public String getUpdatedMoment() {
		return updatedMoment;
	}

	public void setUpdatedMoment(String updatedMoment) {
		this.updatedMoment = updatedMoment;
	}

	public Map<?, ?> getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(Map<?, ?> updatedBy) {
		this.updatedBy = updatedBy;
	}

	public boolean getIsAPIWritable() {
		return isAPIWritable;
	}

	public void setIsAPIWritable(boolean isAPIWritable) {
		this.isAPIWritable = isAPIWritable;
	}

	public PermissionsType getPermissions() {
		return permissions;
	}

	public void setPermissions(PermissionsType permissions) {
		this.permissions = permissions;
	}

	public List<FieldType> getFields() {
		return fields;
	}

	public void setFields(List<FieldType> fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
		} catch (Throwable t) {
			t.printStackTrace();
			return super.toString();
		}
	}

}
