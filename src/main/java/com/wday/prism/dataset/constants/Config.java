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
package com.wday.prism.dataset.constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wday.prism.dataset.util.StringUtilsExt;

public class Config {
	public String proxyUsername = null;
	public String proxyPassword = null;
	public String proxyNtlmDomain = null;
	public String proxyHost = null;
	public int proxyPort = 0;

	public int timeoutSecs = 540;
	public int connectionTimeoutSecs = 60;

	public boolean noCompression = false;

	public boolean debugMessages = false;

	public String oauthClientId = null;
	public String oauthClientSecret = null;
	public String oauthRefreshToken = null;
	public String serviceEndpointURL = null;

	public String oauthClientId2 = null;
	public String oauthClientSecret2 = null;
	public String oauthRefreshToken2 = null;
	public String serviceEndpointURL2 = null;

	public static final Config getSystemConfig() {
		Config conf = new Config();
		if (getAppDir() != null) {
			File configDir = new File(getAppDir(), Constants.configDirName);
			try {
				FileUtils.forceMkdir(configDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
			File configFile = new File(configDir, Constants.configFileName);
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			if (configFile != null && configFile.exists()) {
				InputStreamReader reader = null;
				try {
					reader = new InputStreamReader(new BOMInputStream(new FileInputStream(configFile), false),
							StringUtilsExt.utf8Decoder(null, Charset.forName("UTF-8")));
					conf = mapper.readValue(reader, Config.class);
				} catch (Throwable e) {
					e.printStackTrace();
				} finally {
					IOUtils.closeQuietly(reader);
				}
			} else {
				try {
					mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, conf);
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
		return conf;
	}

	public static final void setSystemConfig(String proxyUsername, String proxyPassword, String proxyNtlmDomain,
			String proxyHost, int proxyPort, int timeoutSecs, int connectionTimeoutSecs, boolean noCompression,
			boolean debugMessages)
			throws JsonGenerationException, JsonMappingException, IOException, URISyntaxException {
		Config conf = new Config();
		conf.proxyUsername = proxyUsername;
		conf.proxyPassword = proxyPassword;
		conf.proxyNtlmDomain = proxyNtlmDomain;
		conf.proxyHost = proxyHost;
		conf.proxyPort = proxyPort;
		conf.timeoutSecs = timeoutSecs;
		conf.noCompression = noCompression;
		conf.debugMessages = debugMessages;

		// HttpUtils.testProxyConfig(conf);

		File configDir = new File(getAppDir(), Constants.configDirName);
		try {
			FileUtils.forceMkdir(configDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File configFile = new File(configDir, Constants.configFileName);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, conf);
	}

	public static final File getAppDir() {

		File currentDir = null;
		File userDir = null;
		try {
			currentDir = new File("").getAbsoluteFile();
			userDir = new File(System.getProperty("user.home"), "DatasetUtils").getAbsoluteFile();
			if (!userDir.exists()) {

				FileUtils.forceMkdir(userDir);

			}
		} catch (Throwable e) {
			e.printStackTrace();
		}

		if (userDir != null) {
			if (userDir.isDirectory()) {
				return userDir;
			}
			return currentDir;
		} else
			return null;
	}

}
