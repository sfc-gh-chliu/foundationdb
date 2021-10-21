/*
 * TenantBalancerInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache Lic---ense, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,+
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/TenantBalancerInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.

std::string TenantBalancerInterface::movementStateToString(MovementState movementState) {
	switch (movementState) {
	case MovementState::INITIALIZING:
		return "Initializing";
	case MovementState::STARTED:
		return "Started";
	case MovementState::READY_FOR_SWITCH:
		return "ReadyForSwitch";
	case MovementState::SWITCHING:
		return "Switching";
	case MovementState::COMPLETED:
		return "Completed";
	case MovementState::ERROR:
		return "Error";
	default:
		ASSERT(false);
	}
}

std::string TenantBalancerInterface::movementLocationToString(MovementLocation movementLocation) {
	switch (movementLocation) {
	case MovementLocation::SOURCE:
		return "Source";
	case MovementLocation::DEST:
		return "Destination";
	default:
		ASSERT(false);
	}
}

std::unordered_map<std::string, std::string> TenantMovementInfo::getStatusInfoMap() const {
	// TODO don't put if unset
	std::unordered_map<std::string, std::string> statusInfoMap;
	statusInfoMap["movementID"] = movementId.toString();
	statusInfoMap["movementLocation"] = TenantBalancerInterface::movementLocationToString(movementLocation);
	statusInfoMap["sourceConnectionString"] = sourceConnectionString;
	statusInfoMap["destinationConnectionString"] = destinationConnectionString;
	statusInfoMap["sourcePrefix"] = sourcePrefix.toString();
	statusInfoMap["destPrefix"] = destPrefix.toString();
	statusInfoMap["isSourceLocked"] = isSourceLocked;
	statusInfoMap["isDestinationLocked"] = isDestinationLocked;
	statusInfoMap["movementState"] = TenantBalancerInterface::movementStateToString(movementState);
	statusInfoMap["mutationLag"] = std::to_string(mutationLag);
	statusInfoMap["databaseTimingDelay"] = std::to_string(databaseTimingDelay);
	statusInfoMap["switchVersion"] = std::to_string(switchVersion);
	statusInfoMap["errorMessage"] = errorMessage;
	statusInfoMap["databaseBackupStatus"] = databaseBackupStatus;
	return statusInfoMap;
}

std::string TenantMovementInfo::toJson() const {
	// TODO 1. use actual type rather than string 2. Exclude values without being set
	json_spirit::mValue statusRootValue;
	JSONDoc statusRoot(statusRootValue);
	for (const auto& itr : getStatusInfoMap()) {
		statusRoot.create(itr.first) = itr.second;
	}
	return json_spirit::write_string(statusRootValue);
}

std::string TenantMovementInfo::toString() const {
	std::string movementInfo;
	for (const auto& itr : getStatusInfoMap()) {
		movementInfo += itr.first + " : " + itr.second + "\n";
	}
	return movementInfo;
}
