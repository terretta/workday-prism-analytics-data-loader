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

import java.util.ArrayList;
import java.util.List;

import com.wday.prism.dataset.file.schema.FieldType;
import com.wday.prism.dataset.file.schema.FieldTypeEnum;

public class CreateDatasetRequestType {

	private String name = null;
	private String description = null;
	private String displayName = null;
	private String documentation = null;
	private List<FieldType> fields;

	public CreateDatasetRequestType(String name,List<FieldType> fields) {
		this.name = name;
		this.fields= fields;
	}

	public CreateDatasetRequestType(DatasetType dataset) {
		this.name = dataset.getName();
		this.description = dataset.getDescription();
		this.displayName = dataset.getDisplayName();
		this.documentation = dataset.getDocumentation();
		List<FieldType> temp = new ArrayList<FieldType>(dataset.getFields().size());
		for(FieldType fld:dataset.getFields())
		{
			
			if(!fld.getName().startsWith("WPA_"))
			{
				if(fld.getType()==FieldTypeEnum.NUMERIC)
				{
					if(fld.getPrecision()==0)
						fld.setPrecision(18); //V1 describe API return precision as 0 for INT/LONG
				}
				temp.add(fld);
			}
		}
		this.fields = temp;
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

	public List<FieldType> getFields() {
		return fields;
	}

	public void setFields(List<FieldType> fields) {
		this.fields = fields;
	}

//	public FileSchema getSchema() {
//		return schema;
//	}
//
//	public void setSchema(FileSchema schema) {
//		this.schema = schema;
//	}

}