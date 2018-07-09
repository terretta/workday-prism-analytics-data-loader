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
package com.wday.prism.dataset.monitor;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.wday.prism.dataset.constants.Constants;

public class Session implements Comparable<Session> {

	static final LinkedList<Session> q = new LinkedList<Session>();

	long startTime = 0l;
	long endTime = 0l;
	long lastModifiedTime = 0l;
	String name = null;
	String id = null;
	File sessionLog = null;
	String orgId = null;
	long sourceTotalRowCount = 0;
	long sourceErrorRowCount = 0;
	long targetTotalRowCount = 0;
	long targetErrorCount = 0;
	String status;
	String message = "";
	String workflowId = null;
	String type = "FileUpload";

	// String jobTrackerid = null;
	volatile AtomicBoolean isDone = new AtomicBoolean(false);
	volatile AtomicBoolean isDoneDone = new AtomicBoolean(false);

	Map<String, String> params = new LinkedHashMap<String, String>();
	private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

	public Session(String orgId, String name) {
		super();
		if (orgId == null || name == null || orgId.trim().isEmpty() || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Input arguments (orgId, name) cannot be null");
		}
		this.name = name;
		this.id = UUID.randomUUID().toString();
		this.sessionLog = new File(name + "_" + id + ".log");
		this.orgId = orgId;
		this.status = "INIT";
		updateLastModifiedTime();
		q.add(this);
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	@JsonIgnore
	public void setParam(String key, String value) {
		if (key != null && !key.isEmpty()) {
			params.put(key, value);
			if (key.equals(Constants.serverStatusParam)) {
				if (value != null) {
					if (value.equalsIgnoreCase("Completed") || value.equalsIgnoreCase("Failed")
							|| value.equalsIgnoreCase("NotProcessed")) {
						isDoneDone.set(true);
					}
				}
			}
		}

	}

	@JsonIgnore
	public String getParam(String key) {
		if (key != null && !key.isEmpty())
			return params.get(key);
		return null;
	}

	public long getSourceTotalRowCount() {
		return sourceTotalRowCount;
	}

	public void setSourceTotalRowCount(long sourceTotalRowCount) {
		this.sourceTotalRowCount = sourceTotalRowCount;
		updateLastModifiedTime();
	}

	public long getSourceErrorRowCount() {
		return sourceErrorRowCount;
	}

	public void setSourceErrorRowCount(long sourceErrorRowCount) {
		this.sourceErrorRowCount = sourceErrorRowCount;
		updateLastModifiedTime();
	}

	public long getTargetTotalRowCount() {
		return targetTotalRowCount;
	}

	public void setTargetTotalRowCount(long targetTotalRowCount) {
		this.targetTotalRowCount = targetTotalRowCount;
		updateLastModifiedTime();
	}

	public long getTargetErrorCount() {
		return targetErrorCount;
	}

	public void setTargetErrorCount(long targetErrorCount) {
		this.targetErrorCount = targetErrorCount;
		updateLastModifiedTime();
	}

	public long getStartTime() {
		return startTime;
	}

	public String getStartTimeFormatted() {
		if (startTime != 0)
			return sdf.format(new Date(startTime));
		else
			return null;
	}

	public long getEndTime() {
		return endTime;
	}

	public String getEndTimeFormatted() {
		if (endTime != 0)
			return sdf.format(new Date(endTime));
		else
			return null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public File getSessionLog() {
		return sessionLog;
	}

	public String getOrgId() {
		return orgId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
		updateLastModifiedTime();
	}

	public String getMessage() {
		return message;
	}

	public String getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(String workflowId) {
		this.workflowId = workflowId;
	}

	public void start() {
		this.status = "RUNNING";
		updateLastModifiedTime();
		this.startTime = this.lastModifiedTime;
	}

	public void end() {
		this.status = "COMPLETED";
		isDone.set(true);
		updateLastModifiedTime();
		this.endTime = this.lastModifiedTime;
	}

	public void fail(String message) {
		this.status = "FAILED";
		this.message = message;
		isDone.set(true);
		// isDoneDone.set(true);
		updateLastModifiedTime();
		this.endTime = this.lastModifiedTime;
	}

	public void terminate(String message) {
		this.status = "TERMINATED";
		if (message != null)
			this.message = message;
		else
			this.message = "TERMINATED ON USER REQUEST";

		isDone.set(true);
		// isDoneDone.set(true);
		updateLastModifiedTime();
		this.endTime = this.lastModifiedTime;
	}

	public boolean isDone() {
		return isDone.get();
	}

	public boolean isDoneDone() {
		return isDoneDone.get();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	void updateLastModifiedTime() {
		this.lastModifiedTime = System.currentTimeMillis();
	}

	public static final LinkedList<Session> listSessions(String orgId) {
		LinkedList<Session> sessionList = new LinkedList<Session>();
		long sevenDaysAgo = System.currentTimeMillis() - 1 * 24 * 60 * 60 * 1000;
		List<Session> copy = new LinkedList<Session>(q);
		for (Session s : copy) {
			if (s.lastModifiedTime > sevenDaysAgo) {
				if (s.orgId.equals(orgId)) {
					sessionList.add(s);
				}
			} else {
				q.remove(s);
			}
		}
		Collections.sort(sessionList, Collections.reverseOrder());
		return sessionList;
	}

	public static final Session getSession(String orgId, String id) {
		for (Session s : q) {
			if (s.orgId.equals(orgId) && s.id.equals(id)) {
				return s;
			}
		}
		return null;
	}

	public static final Session getCurrentSession() {
		ThreadContext threadContext = ThreadContext.get();
		return threadContext.getSession();
	}

	public static final void setCurrentSession(Session session) {
		ThreadContext threadContext = ThreadContext.get();
		threadContext.setSession(session);
	}

	public static final Session getCurrentSession(String orgId, String name, boolean force) {
		ThreadContext threadContext = ThreadContext.get();
		Session session = threadContext.getSession();
		if (force || session == null) {
			if (orgId == null || name == null || orgId.trim().isEmpty() || name.trim().isEmpty()) {
				throw new IllegalArgumentException("Input arguments (orgId, name) cannot be null");
			}
			session = new Session(orgId, name);
			threadContext.setSession(session);
		}
		return session;
	}

	public static final void removeCurrentSession(Session session) {
		ThreadContext threadContext = ThreadContext.get();
		if (session == null) {
			session = threadContext.getSession();
		}
		if (session != null) {
			q.remove(session);
			threadContext.setSession(null);
		}
	}

	@Override
	public int compareTo(Session o) {
		if (this.lastModifiedTime > o.lastModifiedTime)
			return 1;
		else if (this.lastModifiedTime < o.lastModifiedTime)
			return -1;
		else
			return 0;
	}

}
