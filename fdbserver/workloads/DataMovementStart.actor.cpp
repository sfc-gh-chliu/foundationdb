/*
 * DataMovementAbort.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/flow.h"
#include <string>

// Functions copied from backup.actor.cpp file
ACTOR Future<Void> submitDBMove(Database src, Database dest, Key srcPrefix, Key destPrefix) {
	try {
		state MoveTenantToClusterRequest srcRequest(
		    srcPrefix, destPrefix, dest->getConnectionRecord()->getConnectionString().toString());

		state Future<ErrorOr<MoveTenantToClusterReply>> replyFuture = Never();
		state Future<Void> initialize = Void();

		loop choose {
			when(ErrorOr<MoveTenantToClusterReply> reply =
			         wait(timeoutError(replyFuture, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				break;
			}
			when(wait(src->onTenantBalancerChanged() || initialize)) {
				initialize = Never();
				replyFuture = src->getTenantBalancer().present()
				                  ? src->getTenantBalancer().get().moveTenantToCluster.tryGetReply(srcRequest)
				                  : Never();
			}
		}

		printf("The data movement was successfully submitted.\n");
	} catch (Error& e) {
		// TODO This list of errors may change
		if (e.code() == error_code_actor_cancelled)
			throw;

		fprintf(stderr, "ERROR: %s\n", e.what());
	}

	return Void();
}

ACTOR Future<TenantMovementStatus> getMovementStatus(Database database, Key prefix, MovementLocation movementLocation) {
	state GetMovementStatusRequest getMovementStatusRequest(prefix, movementLocation);
	state Future<ErrorOr<GetMovementStatusReply>> getMovementStatusReply = Never();
	state Future<Void> initialize = Void();
	loop choose {
		when(ErrorOr<GetMovementStatusReply> reply =
		         wait(timeoutError(getMovementStatusReply, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
			if (reply.isError()) {
				throw reply.getError();
			}
			return reply.get().movementStatus;
		}
		when(wait(database->onTenantBalancerChanged() || initialize)) {
			initialize = Never();
			getMovementStatusReply =
			    database->getTenantBalancer().present()
			        ? database->getTenantBalancer().get().getMovementStatus.tryGetReply(getMovementStatusRequest)
			        : Never();
		}
	}
}

ACTOR Future<std::vector<TenantMovementInfo>> getActiveMovements(
    Database database,
    Optional<std::string> peerDatabaseConnectionStringFilter,
    Optional<MovementLocation> locationFilter) {
	state GetActiveMovementsRequest getActiveMovementsRequest(
	    Optional<Key>(), peerDatabaseConnectionStringFilter, locationFilter);
	state Future<ErrorOr<GetActiveMovementsReply>> getActiveMovementsReply = Never();
	state Future<Void> initialize = Void();
	loop choose {
		when(ErrorOr<GetActiveMovementsReply> reply =
		         wait(timeoutError(getActiveMovementsReply, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
			if (reply.isError()) {
				throw reply.getError();
			}
			return reply.get().activeMovements;
		}
		when(wait(database->onTenantBalancerChanged() || initialize)) {
			initialize = Never();
			getActiveMovementsReply =
			    database->getTenantBalancer().present()
			        ? database->getTenantBalancer().get().getActiveMovements.tryGetReply(getActiveMovementsRequest)
			        : Never();
		}
	}
}

ACTOR Future<Void> finishDBMove(Database src, Key srcPrefix, Optional<double> maxLagSeconds) {
	try {
		// Send request to source cluster
		state FinishSourceMovementRequest finishSourceMovementRequest(srcPrefix, maxLagSeconds);
		state Future<ErrorOr<FinishSourceMovementReply>> finishSourceMovementReply = Never();
		state Future<Void> initialize = Void();
		state double timeoutLimit;
		if (maxLagSeconds.present()) {
			timeoutLimit = maxLagSeconds.get();
		} else {
			state TenantMovementStatus status = wait(getMovementStatus(src, srcPrefix, MovementLocation::SOURCE));
			if (!status.mutationLag.present()) {
				fprintf(stderr, "ERROR: The movement is not ready to be finished.\n");
				return Void();
			}
			timeoutLimit = status.mutationLag.get();
		}

		loop choose {
			when(ErrorOr<FinishSourceMovementReply> reply =
			         wait(timeoutError(finishSourceMovementReply,
			                           std::max(2 * timeoutLimit, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT)))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				break;
			}
			when(wait(src->onTenantBalancerChanged() || initialize)) {
				initialize = Never();
				finishSourceMovementReply =
				    src->getTenantBalancer().present()
				        ? src->getTenantBalancer().get().finishSourceMovement.tryGetReply(finishSourceMovementRequest)
				        : Never();
			}
		}
		printf("The data movement was successfully finished.\n");
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		fprintf(stderr, "ERROR: %s\n", e.what());
	}

	return Void();
}

ACTOR Future<Void> insertSingleData(const Database& database, KeyRef key, ValueRef value) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(database);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->set(key, value);
			wait(tr->commit());
		} catch (Error& e) {
			TraceEvent(SevDebug, "InsertTestDataError").error(e);

			wait(tr->onError(e));
		}
	}
	return Void();
}

ACTOR Future<Optional<Value>> retrieveSingleData(const Database& database, Key key) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(database);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> value = wait(tr->get(key));
			return value;
		} catch (Error& e) {
			TraceEvent(SevDebug, "InsertTestDataError").error(e);

			wait(tr->onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> cleanupDBMove(Database src, Key srcPrefix, CleanupMovementSourceRequest::CleanupType cleanupType) {
	try {
		state CleanupMovementSourceRequest cleanupMovementSourceRequest(srcPrefix, cleanupType);
		state Future<ErrorOr<CleanupMovementSourceReply>> cleanupMovementSourceReply = Never();
		state Future<Void> initialize = Void();
		loop choose {
			when(ErrorOr<CleanupMovementSourceReply> reply =
			         wait(timeoutError(cleanupMovementSourceReply, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				break;
			}
			when(wait(src->onTenantBalancerChanged() || initialize)) {
				initialize = Never();
				cleanupMovementSourceReply =
				    src->getTenantBalancer().present()
				        ? src->getTenantBalancer().get().cleanupMovementSource.tryGetReply(cleanupMovementSourceRequest)
				        : Never();
			}
		}
		printf("The data movement on %s was successfully cleaned up.\n",
		       src->getConnectionRecord()->getConnectionString().toString().c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
	}

	return Void();
}

struct DataMovementStart : TestWorkload {
	Database extraDB;

	explicit DataMovementStart(const WorkloadContext& wcx) : TestWorkload(wcx) {
		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);
	}

	std::string description() const override { return "DataMovementStart"; }

	ACTOR static Future<Void> insertTestData(const Database& database) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(database);
		std::string dummy = "test";
		std::vector<std::string> prefixes{ "a", "b", "c" };

		for (const auto& prefix : prefixes) {
			insertSingleData(database, KeyRef(prefix + dummy + "key"), ValueRef(prefix + dummy + "val"));
		}
		return Void();
	}

	Future<Void> setup(const Database& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(this, cx);
	}

	ACTOR static Future<Void> _setup(DataMovementStart* self, Database cx) {
		wait(insertTestData(cx));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(DataMovementStart* self, Database cx) {
		// go though start->status->list->finish->clean
		std::string targetPrefixStr = "a";
		Key targetPrefix(targetPrefixStr);

		// start
		wait(submitDBMove(cx, self->extraDB, targetPrefix, targetPrefix));

		// status
		state TenantMovementStatus srcStatus = wait(getMovementStatus(cx, targetPrefix, MovementLocation::SOURCE));
		state TenantMovementStatus destStatus =
		    wait(getMovementStatus(self->extraDB, targetPrefix, MovementLocation::DEST));

		// list
		state std::vector<TenantMovementInfo> activeSrcMovements = wait(getActiveMovements(
		    cx, self->extraDB->getConnectionRecord()->getConnectionString().toString(), MovementLocation::SOURCE));
		state std::vector<TenantMovementInfo> activeDestMovements =
		    wait(getActiveMovements(self->extraDB,
		                            self->extraDB->getConnectionRecord()->getConnectionString().toString(),
		                            MovementLocation::DEST));

		// finish
		finishDBMove(cx, targetPrefix, 3.0);

		// clean
		cleanupDBMove(cx, targetPrefix, CleanupMovementSourceRequest::CleanupType::ERASE_AND_UNLOCK);

		return Void();
	}

	ACTOR static Future<bool> _check(DataMovementStart* self, Database db) {
		// check result
		// expected result: db doesn't have atestkey, extraDb has it. They both have btestkey and ctestkey.

		// For prefix a
		state std::string aKeyStr = "atestkey";
		Optional<Value> aValueDB = wait(retrieveSingleData(db, Key(aKeyStr)));
		if (aValueDB.present()) {
			return false;
		}
		Optional<Value> aValueExtra = wait(retrieveSingleData(self->extraDB, Key(aKeyStr)));
		if (!aValueExtra.present() || aValueExtra.get().toString() != "atestkey") {
			return false;
		}

		// For prefix b
		state std::string bKeyStr = "btestkey";
		Optional<Value> bValueDB = wait(retrieveSingleData(db, Key(bKeyStr)));
		if (!bValueDB.present() || bValueDB.get().toString() != "btestkey") {
			return false;
		}
		Optional<Value> bValueExtra = wait(retrieveSingleData(self->extraDB, Key(bKeyStr)));
		if (bValueExtra.present()) {
			return false;
		}

		// For prefix c
		std::string cKeyStr = "ctestkey";
		Optional<Value> cValueDB = wait(retrieveSingleData(db, Key(cKeyStr)));
		if (!cValueDB.present() || cValueDB.get().toString() != "ctestkey") {
			return false;
		}
		Optional<Value> cValueExtra = wait(retrieveSingleData(self->extraDB, Key(cKeyStr)));
		if (!cValueExtra.present()) {
			return false;
		}

		return true;
	}

	Future<bool> check(const Database& cx) override { return _check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

REGISTER_WORKLOAD(DataMovementStart);