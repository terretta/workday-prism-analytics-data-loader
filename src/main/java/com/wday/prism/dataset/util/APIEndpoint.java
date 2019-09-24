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

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.wday.prism.dataset.file.loader.DatasetLoaderException;

public class APIEndpoint {
	public String services1Host = null;
	public String uiHost = null;
	public String tenantURL = null;
	public String tenant = null;
	public String apiVersion = null;

	public static APIEndpoint getAPIEndpoint(String workdayRESTAPIEndpoint)
			throws MalformedURLException, DatasetLoaderException {
		if (workdayRESTAPIEndpoint == null || workdayRESTAPIEndpoint.trim().isEmpty()) {
			throw new DatasetLoaderException("\nERROR: Invalid Workday REST API Endpoint. Endpoint must be like: "
					+ "https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
		}

		APIEndpoint endpoint = new APIEndpoint();
		URL uri = null;
		try {
			uri = new URL(workdayRESTAPIEndpoint);
		} catch (Throwable t) {
			throw new DatasetLoaderException(
					"\nERROR: {" + t.getMessage() + "}. Invalid Workday REST API Endpoint. Endpoint must be like: "
							+ "https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
		}

		if (uri.getProtocol() != null
				&& (uri.getProtocol().equalsIgnoreCase("https") || uri.getProtocol().equalsIgnoreCase("http"))) {
			if (uri.getPath() == null || uri.getPath().isEmpty() || uri.getPath().equals("/")
					|| !(uri.getPath().contains("/ccx/api/v1/") || uri.getPath().contains("/ccx/api/v2Alpha/") || uri.getPath().contains("/ccx/api/v2/"))) {
				throw new DatasetLoaderException("\nERROR: Invalid Workday REST API Endpoint. Endpoint must be like: "
						+ "https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
			} else {
				Path path = Paths.get(uri.getPath());
				if (path.getNameCount() > 3) {
					endpoint.tenant = path.getName(3).toString();
					endpoint.apiVersion = path.getName(2).toString();
					endpoint.services1Host = uri.getHost();
					URL newUri = new URL(uri.getProtocol(), uri.getHost(), uri.getPort(), "");
					endpoint.tenantURL = newUri.toString();
					endpoint.uiHost = uri.getHost().replaceAll("-services1", "");
				} else {
					throw new DatasetLoaderException(
							"\nERROR: Invalid Workday REST API Endpoint. Endpoint must be like: "
									+ "https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
				}
			}
		} else {
			throw new DatasetLoaderException("\nERROR: Invalid Workday REST API Endpoint. Endpoint must be like: "
					+ "https://wd2-impl-services1.workday.com/ccx/api/v1/{tenantName}");
		}
		return endpoint;

	}
}
