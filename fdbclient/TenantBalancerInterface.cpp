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

std::string TenantMovementInfo::toJson() const {
	json_spirit::mValue statusRootValue;
	JSONDoc statusRoot(statusRootValue);
	if (movementId.present()) {
		statusRoot.create("movementID") = movementId.get().toString();
	}
	if (movementLocation.present()) {
		statusRoot.create("movementLocation") =
		    TenantBalancerInterface::movementLocationToString(movementLocation.get());
	}
	if (sourceConnectionString.present()) {
		statusRoot.create("sourceConnectionString") = sourceConnectionString.get();
	}
	if (destinationConnectionString.present()) {
		statusRoot.create("destinationConnectionString") = destinationConnectionString.get();
	}
	if (sourcePrefix.present()) {
		statusRoot.create("sourcePrefix") = sourcePrefix.get().toString();
	}
	if (destPrefix.present()) {
		statusRoot.create("destPrefix") = destPrefix.get().toString();
	}
	if (isSourceLocked.present()) {
		statusRoot.create("isSourceLocked") = isSourceLocked.get();
	}
	if (isDestinationLocked.present()) {
		statusRoot.create("isDestinationLocked") = isDestinationLocked.get();
	}
	if (movementState.present()) {
		statusRoot.create("movementState") = TenantBalancerInterface::movementStateToString(movementState.get());
	}
	if (mutationLag.present()) {
		statusRoot.create("mutationLag") = mutationLag.get();
	}
	if (databaseTimingDelay.present()) {
		statusRoot.create("databaseTimingDelay") = databaseTimingDelay.get();
	}
	if (switchVersion.present()) {
		statusRoot.create("switchVersion") = switchVersion.get();
	}
	if (errorMessage.present()) {
		statusRoot.create("errorMessage") = errorMessage.get();
	}
	if (databaseBackupStatus.present()) {
		statusRoot.create("databaseBackupStatus") = databaseBackupStatus.get();
	}
	return json_spirit::write_string(statusRootValue);
}

std::unordered_map<std::string, std::string> TenantMovementInfo::getStatusInfoMap() const {
	std::unordered_map<std::string, std::string> statusInfoMap;
	if (movementId.present()) {
		statusInfoMap["movementID"] = movementId.get().toString();
	}
	if (movementLocation.present()) {
		statusInfoMap["movementLocation"] = TenantBalancerInterface::movementLocationToString(movementLocation.get());
	}
	if (sourceConnectionString.present()) {
		statusInfoMap["sourceConnectionString"] = sourceConnectionString.get();
	}
	if (destinationConnectionString.present()) {
		statusInfoMap["destinationConnectionString"] = destinationConnectionString.get();
	}
	if (sourcePrefix.present()) {
		statusInfoMap["sourcePrefix"] = sourcePrefix.get().toString();
	}
	if (destPrefix.present()) {
		statusInfoMap["destPrefix"] = destPrefix.get().toString();
	}
	if (isSourceLocked.present()) {
		statusInfoMap["isSourceLocked"] = std::to_string(isSourceLocked.get());
	}
	if (isDestinationLocked.present()) {
		statusInfoMap["isDestinationLocked"] = std::to_string(isDestinationLocked.get());
	}
	if (movementState.present()) {
		statusInfoMap["movementState"] = TenantBalancerInterface::movementStateToString(movementState.get());
	}
	if (mutationLag.present()) {
		statusInfoMap["mutationLag"] = std::to_string(mutationLag.get());
	}
	if (databaseTimingDelay.present()) {
		statusInfoMap["databaseTimingDelay"] = std::to_string(databaseTimingDelay.get());
	}
	if (switchVersion.present()) {
		statusInfoMap["switchVersion"] = std::to_string(switchVersion.get());
	}
	if (errorMessage.present()) {
		statusInfoMap["errorMessage"] = errorMessage.get();
	}
	if (databaseBackupStatus.present()) {
		statusInfoMap["databaseBackupStatus"] = databaseBackupStatus.get();
	}
	return statusInfoMap;
}

std::string TenantMovementInfo::toString() const {
	std::string movementInfo;
	for (const auto& itr : getStatusInfoMap()) {
		movementInfo += itr.first + " : " + itr.second + "\n";
	}
	return movementInfo;
}
