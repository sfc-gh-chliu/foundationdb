/*
 * DataMovementStart.actor.cpp
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
#include "flow/flow.h"
#include <string>
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR template <class Request>
Future<REPLY_TYPE(Request)> sendTenantBalancerRequest(Database peerDb,
                                                      Request request,
                                                      RequestStream<Request> TenantBalancerInterface::*stream) {
	state Future<ErrorOr<REPLY_TYPE(Request)>> replyFuture = Never();
	state Future<Void> initialize = Void();

	loop choose {
		when(ErrorOr<REPLY_TYPE(Request)> reply = wait(replyFuture)) {
			if (reply.isError()) {
				throw reply.getError();
			}
			return reply.get();
		}
		when(wait(peerDb->onTenantBalancerChanged() || initialize)) {
			initialize = Never();
			replyFuture = peerDb->getTenantBalancer().present()
			                  ? (peerDb->getTenantBalancer().get().*stream).tryGetReply(request)
			                  : Never();
		}
	}
}

ACTOR Future<Void> submitDBMove(Database src, Database dest, Key srcPrefix, Key destPrefix) {
	wait(sendTenantBalancerRequest(
	    src,
	    MoveTenantToClusterRequest(
	        srcPrefix, destPrefix, dest->getConnectionRecord()->getConnectionString().toString()),
	    &TenantBalancerInterface::moveTenantToCluster));
	return Void();
}

ACTOR Future<TenantMovementStatus> getMovementStatus(Database database, Key prefix, MovementLocation movementLocation) {
	state GetMovementStatusReply reply = wait(sendTenantBalancerRequest(
	    database, GetMovementStatusRequest(prefix, movementLocation), &TenantBalancerInterface::getMovementStatus));
	return reply.movementStatus;
}

ACTOR Future<Void> finishDBMove(Database database, Key prefix, Optional<double> maxLagSeconds) {
	wait(sendTenantBalancerRequest(
	    database, FinishSourceMovementRequest(prefix, maxLagSeconds), &TenantBalancerInterface::finishSourceMovement));
	return Void();
}

ACTOR Future<Void> cleanupDBMove(Database src, Key srcPrefix, CleanupMovementSourceRequest::CleanupType cleanupType) {
	wait(sendTenantBalancerRequest(
	    src, CleanupMovementSourceRequest(srcPrefix, cleanupType), &TenantBalancerInterface::cleanupMovementSource));
	return Void();
}

ACTOR
template <class Result>
Future<Result> runTransaction(const Database& database,
                              std::function<Future<Result>(Reference<ReadYourWritesTransaction>)> func) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(database);
	state Result result;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			result = wait(func(tr));
			return result;
		} catch (Error& e) {
			TraceEvent(SevDebug, "InsertTestDataError").error(e);
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> insertSingleData(const Database& database, Key key, Value value) {
	wait(runTransaction<Void>(database, [key, value](Reference<ReadYourWritesTransaction> tr) {
		tr->set(key, value);
		return tr->commit();
	}));
	return Void();
}

ACTOR Future<Optional<Value>> retrieveSingleData(const Database& database, Key key) {
	Optional<Value> value = wait(runTransaction<Optional<Value>>(
	    database, [key](Reference<ReadYourWritesTransaction> tr) { return tr->get(key); }));
	return value;
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
			insertSingleData(database, Key(prefix + dummy + "key"), Value(prefix + dummy + "val"));
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
		state TenantMovementStatus destStatus =
		    wait(getMovementStatus(self->extraDB, targetPrefix, MovementLocation::DEST));
		for (;;) {
			state TenantMovementStatus srcStatus = wait(getMovementStatus(cx, targetPrefix, MovementLocation::SOURCE));
			if (srcStatus.mutationLag.present() && srcStatus.mutationLag.get() < 1) {
				break;
			}
		}

		// finish
		wait(finishDBMove(cx, targetPrefix, 3.0));

		// clean
		wait(cleanupDBMove(cx, targetPrefix, CleanupMovementSourceRequest::CleanupType::ERASE_AND_UNLOCK));

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