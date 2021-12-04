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
#include <string>
#include <unordered_map>

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

std::string TenantMovementStatus::toJson() const {
	json_spirit::mValue statusRootValue;
	JSONDoc statusRoot(statusRootValue);

	// Insert tenantMoveInfo into JSON
	statusRoot.create("movementId") = tenantMovementInfo.movementId.toString();
	statusRoot.create("peerConnectionString") = tenantMovementInfo.peerConnectionString;
	statusRoot.create("sourcePrefix") = printable(tenantMovementInfo.sourcePrefix);
	statusRoot.create("destinationPrefix") = printable(tenantMovementInfo.destinationPrefix);
	statusRoot.create("movementState") =
	    TenantBalancerInterface::movementStateToString(tenantMovementInfo.movementState);
	if (tenantMovementInfo.tenantMovementInfoErrorMessage.present()) {
		statusRoot.create("tenantMovementInfoError") = tenantMovementInfo.tenantMovementInfoErrorMessage.get();
	}

	// Insert movement status into JSON
	statusRoot.create("isSourceLocked") = isSourceLocked;
	statusRoot.create("isDestinationLocked") = isDestinationLocked;
	if (databaseVersionLag.present()) {
		statusRoot.create("destinationDatabaseVersionLag") = databaseVersionLag.get();
	}
	if (mutationLag.present()) {
		statusRoot.create("mutationLag") = mutationLag.get();
	}
	if (switchVersion.present()) {
		statusRoot.create("switchVersion") = switchVersion.get();
	}
	if (!errorMessages.empty()) {
		statusRoot.create("TenantMovementStatusErrors") =
		    json_spirit::mArray(errorMessages.begin(), errorMessages.end());
	}
	return json_spirit::write_string(statusRootValue);
}
