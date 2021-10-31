/*
 * TenantBalancer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionKey.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ExternalDatabaseMap.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ITrace.h"
#include "flow/Trace.h"
#include "fdbclient/StatusClient.h"
#include <cfloat>
#include <string>
#include <unordered_map>
#include <vector>
#include "flow/actorcompiler.h" // This must be the last #include.

static const StringRef DBMOVE_TAG_PREFIX = "MovingData/"_sr;

ACTOR Future<Void> checkTenantBalancerOwnership(UID id, Reference<ReadYourWritesTransaction> tr) {
	Optional<Value> value = wait(tr->get(tenantBalancerActiveProcessKey));
	if (!value.present() || value.get().toString() != id.toString()) {
		TraceEvent("TenantBalancerLostOwnership", id).detail("CurrentOwner", value);
		throw tenant_balancer_terminated();
	}

	return Void();
}

ACTOR template <class Result>
Future<Result> runTenantBalancerTransaction(Database db,
                                            UID id,
                                            std::string context,
                                            std::function<Future<Result>(Reference<ReadYourWritesTransaction>)> func) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	state int count = 0;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkTenantBalancerOwnership(id, tr));
			Result r = wait(func(tr));
			return r;
		} catch (Error& e) {
			TraceEvent(SevDebug, "TenantBalancerTransactionError", id)
			    .error(e)
			    .detail("Context", context)
			    .detail("ErrorCount", ++count);

			wait(tr->onError(e));
		}
	}
}

class MovementRecord : public ReferenceCounted<MovementRecord> {
public:
	MovementRecord() {}
	MovementRecord(MovementLocation movementLocation,
	               Standalone<StringRef> sourcePrefix,
	               Standalone<StringRef> destinationPrefix,
	               std::string peerDatabaseName,
	               Database peerDatabase)
	  : id(deterministicRandom()->randomUniqueID()), movementLocation(movementLocation), sourcePrefix(sourcePrefix),
	    destinationPrefix(destinationPrefix), peerDatabaseName(peerDatabaseName), peerDatabase(peerDatabase) {}

	MovementRecord(UID id,
	               MovementLocation movementLocation,
	               Standalone<StringRef> sourcePrefix,
	               Standalone<StringRef> destinationPrefix,
	               std::string peerDatabaseName,
	               Database peerDatabase)
	  : id(id), movementLocation(movementLocation), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix),
	    peerDatabaseName(peerDatabaseName), peerDatabase(peerDatabase) {}

	UID getMovementId() const { return id; }
	MovementLocation getMovementLocation() const { return movementLocation; }

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }

	Standalone<StringRef> getLocalPrefix() const {
		return movementLocation == MovementLocation::SOURCE ? sourcePrefix : destinationPrefix;
	}
	Standalone<StringRef> getRemotePrefix() const {
		return movementLocation == MovementLocation::SOURCE ? destinationPrefix : sourcePrefix;
	}

	Database getPeerDatabase() const { return peerDatabase; }
	std::string getPeerDatabaseName() const { return peerDatabaseName; }

	Standalone<StringRef> getTagName() const { return DBMOVE_TAG_PREFIX.withSuffix(id.toString()); }

	void setPeerDatabase(Database db) { peerDatabase = db; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           id,
		           movementState,
		           movementLocation,
		           sourcePrefix,
		           destinationPrefix,
		           peerDatabaseName,
		           errorMessage,
		           switchVersion);
	}

	Key getKey() const {
		if (movementLocation == MovementLocation::SOURCE) {
			return StringRef(id.toString()).withPrefix(tenantBalancerSourceMovementPrefix);
		} else {
			return StringRef(id.toString()).withPrefix(tenantBalancerDestinationMovementPrefix);
		}
	}

	Value toValue() const {
		BinaryWriter wr(IncludeVersion());
		wr << *this;
		return wr.toValue();
	}

	static Reference<MovementRecord> fromValue(Value value) {
		Reference<MovementRecord> record = makeReference<MovementRecord>();
		BinaryReader rd(value, IncludeVersion());
		rd >> *record;

		return record;
	}

	void setMovementError(const std::string& errorMessage) {
		movementState = MovementState::ERROR;
		if (!this->errorMessage.present()) {
			this->errorMessage = errorMessage;
		}
	}

	Optional<std::string> getErrorMessage() const { return errorMessage; }

	void abort() const {
		if (!abortPromise.isSet()) {
			abortPromise.sendError(movement_aborted());
		}
	}

	Future<Void> onAbort() { return abortPromise.getFuture(); }

	MovementState movementState = MovementState::INITIALIZING;
	Version switchVersion = invalidVersion;

	Reference<MovementRecord> clone() const {
		Reference<MovementRecord> record = makeReference<MovementRecord>(
		    id, movementLocation, sourcePrefix, destinationPrefix, peerDatabaseName, peerDatabase);

		record->movementState = movementState;
		record->switchVersion = switchVersion;
		record->errorMessage = errorMessage;
		record->abortPromise = abortPromise;

		return record;
	}

private:
	// Private variables are not intended to be modified by requests
	UID id;

	MovementLocation movementLocation;
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;

	std::string peerDatabaseName;
	Database peerDatabase;
	Optional<std::string> errorMessage;

	Promise<Void> abortPromise;
};

class MovementRecordMap {
private:
	struct MovementRecordState {
		Reference<MovementRecord> movementRecord;
		FlowLock writeLock;

		MovementRecordState(Reference<MovementRecord> record) : movementRecord(record) {}
	};

public:
	struct MutableRecord : ReferenceCounted<MutableRecord> {
	public:
		MutableRecord(Reference<MovementRecord> record, FlowLock& lock) : record(record->clone()), lockReleaser(lock) {}
		Reference<MovementRecord> record;

	private:
		FlowLock::Releaser lockReleaser;
	};

	class SnapshotIterator {
	public:
		bool operator==(SnapshotIterator const& other) const { return itr == other.itr; }
		bool operator!=(SnapshotIterator const& other) const { return !(*this == other); }
		SnapshotIterator& operator++() {
			resetRecord();
			++itr;
			return *this;
		}
		SnapshotIterator& operator--() {
			resetRecord();
			--itr;
			return *this;
		}
		SnapshotIterator operator++(int) {
			SnapshotIterator old(*this);
			resetRecord();
			++itr;
			return old;
		}
		SnapshotIterator operator--(int) {
			SnapshotIterator old(*this);
			resetRecord();
			--itr;
			return old;
		}
		std::pair<Key, Reference<const MovementRecord>> const* operator->() {
			if (!record.present()) {
				record = std::make_pair(itr->first, itr->second.movementRecord);
			}
			return &record.get();
		}

		SnapshotIterator(std::map<Key, MovementRecordState>::const_iterator itr) : itr(itr) {}

	private:
		std::map<Key, MovementRecordState>::const_iterator itr;
		Optional<std::pair<Key, Reference<const MovementRecord>>> record;
		void resetRecord() { record = Optional<std::pair<Key, Reference<const MovementRecord>>>(); }
	};

	MovementRecordMap(UID id) : id(id) {}

	Reference<const MovementRecord> getRecordSnapshot(Key prefix) const {
		auto itr = movementRecords.find(prefix);
		if (itr != movementRecords.end()) {
			return itr->second.movementRecord;
		} else {
			throw movement_not_found();
		}
	}

	ACTOR Future<Reference<MutableRecord>> insertRecoveredRecordImpl(MovementRecordMap* self,
	                                                                 Reference<MovementRecord> record) {
		state std::pair<std::map<Key, MovementRecordState>::iterator, bool> result =
		    self->movementRecords.try_emplace(record->getLocalPrefix(), record);
		ASSERT(result.second);
		wait(result.first->second.writeLock.take());
		return makeReference<MutableRecord>(record, result.first->second.writeLock);
	}
	Future<Reference<MutableRecord>> insertRecoveredRecord(Reference<MovementRecord> record) {
		return insertRecoveredRecordImpl(this, record);
	}

	ACTOR Future<Reference<MutableRecord>> insertAndLockMovementRecordImpl(MovementRecordMap* self,
	                                                                       Database db,
	                                                                       Reference<MovementRecord> record) {
		// Check if there are any conflicting movements. A conflicting movement would be one that is a prefix of our
		// requested movement or that is contained within our requested movement.
		auto movementItr = self->movementRecords.upper_bound(record->getLocalPrefix());
		if (movementItr != self->movementRecords.end() && movementItr->first.startsWith(record->getLocalPrefix())) {
			TraceEvent(SevWarn, "TenantBalancerMoveConflict", self->id)
			    .detail("ConflictingPrefix", movementItr->first)
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix())
			    .detail("PeerConnectionString",
			            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()));
			throw movement_conflict();
		}
		if (movementItr != self->movementRecords.begin() &&
		    record->getLocalPrefix().startsWith((--movementItr)->first)) {
			TraceEvent(SevWarn, "TenantBalancerMoveConflict", self->id)
			    .detail("ConflictingPrefix", movementItr->first)
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix())
			    .detail("PeerConnectionString",
			            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()));
			throw movement_conflict();
		}

		state std::pair<std::map<Key, MovementRecordState>::iterator, bool> result =
		    self->movementRecords.try_emplace(record->getLocalPrefix(), record);
		ASSERT(result.second);
		wait(result.first->second.writeLock.take());
		state Reference<MutableRecord> mutableRecord =
		    makeReference<MutableRecord>(record, result.first->second.writeLock);

		try {
			wait(self->saveRecord(db, mutableRecord));
		} catch (Error& e) {
			TraceEvent(SevWarn, "TenantBalancerErrorCreatingMovementRecord", self->id)
			    .error(e)
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix())
			    .detail("PeerConnectionString",
			            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()));

			self->movementRecords.erase(record->getLocalPrefix());
			throw;
		}

		return mutableRecord;
	}
	Future<Reference<MutableRecord>> insertAndLockMovementRecord(Database db, Reference<MovementRecord> record) {
		return insertAndLockMovementRecordImpl(this, db, record);
	}

	ACTOR static Future<Optional<Reference<MutableRecord>>> tryCopyAndLockRecordImpl(MovementRecordMap* self,
	                                                                                 Key prefix) {
		state std::map<Key, MovementRecordState>::iterator itr = self->movementRecords.find(prefix);
		if (itr != self->movementRecords.end()) {
			if (itr->second.writeLock.available() == 1) {
				try {
					wait(itr->second.writeLock.take());
				} catch (Error& e) {
					if (e.code() == error_code_broken_promise) {
						throw movement_not_found();
					}
					throw;
				}
				return makeReference<MutableRecord>(itr->second.movementRecord, itr->second.writeLock);
			} else {
				return Optional<Reference<MutableRecord>>();
			}
		} else {
			throw movement_not_found();
		}
	}
	Future<Optional<Reference<MutableRecord>>> tryCopyAndLockRecord(Key prefix) {
		return tryCopyAndLockRecordImpl(this, prefix);
	}

	ACTOR static Future<Reference<MutableRecord>> copyAndLockRecordImpl(MovementRecordMap* self, Key prefix) {
		state std::map<Key, MovementRecordState>::iterator itr = self->movementRecords.find(prefix);
		if (itr != self->movementRecords.end()) {
			try {
				wait(itr->second.writeLock.take());
			} catch (Error& e) {
				if (e.code() == error_code_broken_promise) {
					throw movement_not_found();
				}
				throw;
			}
			return makeReference<MutableRecord>(itr->second.movementRecord, itr->second.writeLock);
		} else {
			throw movement_not_found();
		}
	}
	Future<Reference<MutableRecord>> copyAndLockRecord(Key prefix) { return copyAndLockRecordImpl(this, prefix); }

	void releaseRecord(Reference<const MovementRecord> record) {
		auto itr = movementRecords.find(record->getLocalPrefix());
		ASSERT(itr != movementRecords.end());
		itr->second.writeLock.release();
	}

	ACTOR static Future<Void> saveRecordImpl(MovementRecordMap* self,
	                                         Database db,
	                                         Reference<const MutableRecord> mutableRecord) {

		state Reference<MovementRecord> record = mutableRecord->record;
		auto itr = self->movementRecords.find(record->getLocalPrefix());
		ASSERT(itr != self->movementRecords.end());
		ASSERT(itr->second.writeLock.activePermits() == 1);

		Key key = record->getKey();
		Value value = record->toValue();

		wait(runTenantBalancerTransaction<Void>(
		    db, self->id, "SaveMovementRecord", [key, value](Reference<ReadYourWritesTransaction> tr) {
			    tr->set(key, value);
			    return tr->commit();
		    }));

		auto afterTrItr = self->movementRecords.find(record->getLocalPrefix());
		ASSERT(afterTrItr != self->movementRecords.end());
		afterTrItr->second.movementRecord = record;

		return Void();
	}
	Future<Void> saveRecord(Database db, Reference<const MutableRecord> mutableRecord) {
		return saveRecordImpl(this, db, mutableRecord);
	}

	ACTOR static Future<Void> eraseRecordImpl(MovementRecordMap* self,
	                                          Database db,
	                                          Reference<const MutableRecord> mutableRecord) {
		state Reference<MovementRecord> record = mutableRecord->record;
		auto itr = self->movementRecords.find(record->getLocalPrefix());
		ASSERT(itr != self->movementRecords.end());
		ASSERT(itr->second.writeLock.activePermits() == 1);

		Key key = itr->second.movementRecord->getKey();

		wait(runTenantBalancerTransaction<Void>(
		    db, self->id, "ClearMovementRecord", [key](Reference<ReadYourWritesTransaction> tr) {
			    tr->clear(key);
			    return tr->commit();
		    }));

		self->movementRecords.erase(record->getLocalPrefix());
		return Void();
	}
	Future<Void> eraseRecord(Database db, Reference<const MutableRecord> mutableRecord) {
		return eraseRecordImpl(this, db, mutableRecord);
	}

	SnapshotIterator find(Key prefix) const { return SnapshotIterator(movementRecords.find(prefix)); }
	SnapshotIterator begin() const { return SnapshotIterator(movementRecords.begin()); }
	SnapshotIterator end() const { return SnapshotIterator(movementRecords.end()); }

	std::map<Key, MovementRecordState>::size_type size() const { return movementRecords.size(); }

private:
	UID id;
	std::map<Key, MovementRecordState> movementRecords;
};

ACTOR static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                            Reference<AsyncVar<ClientDBInfo>> info) {
	loop {
		ClientDBInfo clientInfo = dbInfo->get().client;
		info->set(clientInfo);
		wait(dbInfo->onChange());
	}
}

ACTOR Future<bool> insertDbKey(Reference<ReadYourWritesTransaction> tr, Key dbKey, Value dbValue) {
	Optional<Value> existingValue = wait(tr->get(dbKey));
	if (existingValue.present() && existingValue.get() != dbValue) {
		return false;
	}

	tr->set(dbKey, dbValue);
	wait(tr->commit());

	return true;
}

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi,
	               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	               Reference<IClusterConnectionRecord> connRecord)
	  : tbi(tbi), dbInfo(dbInfo), connRecord(connRecord), actors(false),
	    tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
	    moveTenantToClusterRequests("MoveTenantToClusterRequests", tenantBalancerMetrics),
	    receiveTenantFromClusterRequests("ReceiveTenantFromClusterRequests", tenantBalancerMetrics),
	    getActiveMovementsRequests("GetActiveMovementsRequests", tenantBalancerMetrics),
	    getMovementStatusRequests("GetMovementStatusRequests", tenantBalancerMetrics),
	    finishSourceMovementRequests("FinishSourceMovementRequests", tenantBalancerMetrics),
	    finishDestinationMovementRequests("FinishDestinationMovementRequests", tenantBalancerMetrics),
	    recoverMovementRequests("RecoverMovementRequests", tenantBalancerMetrics),
	    abortMovementRequests("AbortMovementRequests", tenantBalancerMetrics),
	    cleanupMovementSourceRequests("CleanupMovementSourceRequests", tenantBalancerMetrics),
	    outgoingMovements(tbi.id()), incomingMovements(tbi.id()) {
		auto info = makeReference<AsyncVar<ClientDBInfo>>();
		db = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::False, EnableLocalityLoadBalance::True);

		agent = DatabaseBackupAgent(db);

		specialCounter(tenantBalancerMetrics, "OpenDatabases", [this]() { return externalDatabases.size(); });
		specialCounter(tenantBalancerMetrics, "ActiveMovesAsSource", [this]() { return outgoingMovements.size(); });
		specialCounter(
		    tenantBalancerMetrics, "ActiveMovesAsDestination", [this]() { return incomingMovements.size(); });

		actors.add(traceCounters("TenantBalancerMetrics",
		                         tbi.id(),
		                         SERVER_KNOBS->STORAGE_LOGGING_DELAY,
		                         &tenantBalancerMetrics,
		                         tbi.id().toString() + "/TenantBalancerMetrics"));
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> connRecord;

	Database db;

	ActorCollection actors;
	DatabaseBackupAgent agent;

	Reference<const MovementRecord> getMovementSnapshot(MovementLocation movementLocation,
	                                                    Key prefix,
	                                                    Optional<UID> movementId = Optional<UID>()) const {
		auto& movements = movementLocation == MovementLocation::SOURCE ? outgoingMovements : incomingMovements;
		Reference<const MovementRecord> movement = movements.getRecordSnapshot(prefix);

		if (movementId.present() && movementId.get() != movement->getMovementId()) {
			TraceEvent(SevWarn, "TenantBalancerMovementIdMismatch", tbi.id())
			    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(movementLocation))
			    .detail("Prefix", prefix)
			    .detail("ExpectedId", movementId)
			    .detail("ActualId", movement->getMovementId());

			throw movement_id_mismatch();
		}

		return movement;
	}

	Reference<const MovementRecord> getOutgoingMovementSnapshot(Key prefix,
	                                                            Optional<UID> movementId = Optional<UID>()) const {
		return getMovementSnapshot(MovementLocation::SOURCE, prefix, movementId);
	}

	Reference<const MovementRecord> getIncomingMovementSnapshot(Key prefix,
	                                                            Optional<UID> movementId = Optional<UID>()) const {
		return getMovementSnapshot(MovementLocation::DEST, prefix, movementId);
	}

	Future<Optional<Reference<MovementRecordMap::MutableRecord>>> tryMutateMovement(MovementLocation movementLocation,
	                                                                                Key prefix) {
		MovementRecordMap& movements =
		    movementLocation == MovementLocation::SOURCE ? outgoingMovements : incomingMovements;
		return movements.tryCopyAndLockRecord(prefix);
	}

	ACTOR Future<Reference<MovementRecordMap::MutableRecord>> mutateMovementImpl(
	    TenantBalancer* self,
	    MovementLocation movementLocation,
	    Key prefix,
	    Optional<UID> movementId = Optional<UID>()) {
		MovementRecordMap& movements =
		    movementLocation == MovementLocation::SOURCE ? self->outgoingMovements : self->incomingMovements;
		Reference<MovementRecordMap::MutableRecord> mutableRecord = wait(movements.copyAndLockRecord(prefix));

		if (movementId.present() && movementId.get() != mutableRecord->record->getMovementId()) {
			TraceEvent(SevWarn, "TenantBalancerMovementIdMismatch", self->tbi.id())
			    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(movementLocation))
			    .detail("Prefix", prefix)
			    .detail("ExpectedId", movementId)
			    .detail("ActualId", mutableRecord->record->getMovementId());

			throw movement_id_mismatch();
		}

		return mutableRecord;
	}

	Future<Reference<MovementRecordMap::MutableRecord>> mutateMovement(MovementLocation movementLocation,
	                                                                   Key prefix,
	                                                                   Optional<UID> movementId = Optional<UID>()) {
		return mutateMovementImpl(this, movementLocation, prefix, movementId);
	}

	Future<Reference<MovementRecordMap::MutableRecord>> mutateOutgoingMovement(
	    Key prefix,
	    Optional<UID> movementId = Optional<UID>()) {
		return mutateMovement(MovementLocation::SOURCE, prefix, movementId);
	}

	Future<Reference<MovementRecordMap::MutableRecord>> mutateIncomingMovement(
	    Key prefix,
	    Optional<UID> movementId = Optional<UID>()) {
		return mutateMovement(MovementLocation::DEST, prefix, movementId);
	}

	Future<Void> saveMovementRecord(Reference<const MovementRecordMap::MutableRecord> mutableRecord) {
		auto& movements = mutableRecord->record->getMovementLocation() == MovementLocation::SOURCE ? outgoingMovements
		                                                                                           : incomingMovements;
		return movements.saveRecord(db, mutableRecord);
	}
	ACTOR Future<Reference<MovementRecordMap::MutableRecord>> createAndGetMovementRecordImpl(
	    TenantBalancer* self,
	    MovementLocation location,
	    Key sourcePrefix,
	    Key destPrefix,
	    std::string peerConnectionString,
	    Optional<UID> movementId) {
		state Database peerDb;
		state std::string databaseName = peerConnectionString;

		state MovementRecordMap& movements =
		    location == MovementLocation::SOURCE ? self->outgoingMovements : self->incomingMovements;
		state Key prefix = location == MovementLocation::SOURCE ? sourcePrefix : destPrefix;

		loop {
			Optional<Database> db = wait(self->getOrInsertDatabase(peerConnectionString, peerConnectionString));
			if (db.present()) {
				peerDb = db.get();
				break;
			}

			// This will generate a unique random database name, so we won't get the benefits of sharing
			databaseName = peerConnectionString + "/" + deterministicRandom()->randomUniqueID().toString();
			TraceEvent(SevDebug, "TenantBalancerCreateDatabaseUniqueNameFallback", self->tbi.id())
			    .detail("ConnectionString", peerConnectionString)
			    .detail("DatabaseName", databaseName);
		}

		Reference<MovementRecord> record;
		if (movementId.present()) {
			record = makeReference<MovementRecord>(
			    movementId.get(), location, sourcePrefix, destPrefix, peerConnectionString, peerDb);
		} else {
			record = makeReference<MovementRecord>(location, sourcePrefix, destPrefix, peerConnectionString, peerDb);
		}

		Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(movements.insertAndLockMovementRecord(self->db, record));
		self->externalDatabases.addDatabaseRef(databaseName);

		return mutableRecord;
	}

	Future<Reference<MovementRecordMap::MutableRecord>> createAndGetMovementRecord(
	    MovementLocation location,
	    Key sourcePrefix,
	    Key destPrefix,
	    std::string peerConnectionString,
	    Optional<UID> movementId = Optional<UID>()) {
		return createAndGetMovementRecordImpl(
		    this, location, sourcePrefix, destPrefix, peerConnectionString, movementId);
	}

	ACTOR static Future<Optional<Database>> getOrInsertDatabaseImpl(TenantBalancer* self,
	                                                                std::string name,
	                                                                std::string connectionString) {
		Optional<Database> existingDb = self->externalDatabases.get(name);
		if (existingDb.present()) {
			if (existingDb.get()->getConnectionRecord()->getConnectionString().toString() == connectionString) {
				return existingDb;
			}
			return Optional<Database>();
		}

		self->externalDatabases.cancelCleanup(name);

		state Key dbKey = KeyRef(name).withPrefix(tenantBalancerExternalDatabasePrefix);
		Key dbKeyCapture = dbKey;
		Value dbValue = ValueRef(connectionString);

		bool inserted =
		    wait(runTenantBalancerTransaction<bool>(self->db,
		                                            self->tbi.id(),
		                                            "GetOrInsertDatabase",
		                                            [dbKeyCapture, dbValue](Reference<ReadYourWritesTransaction> tr) {
			                                            return insertDbKey(tr, dbKeyCapture, dbValue);
		                                            }));

		if (!inserted) {
			return Optional<Database>();
		}

		Database db = Database::createDatabase(
		    makeReference<ClusterConnectionKey>(self->db, dbKey, ClusterConnectionString(connectionString)),
		    Database::API_VERSION_LATEST,
		    IsInternal::True,
		    self->tbi.locality);

		if (!self->externalDatabases.insert(name, db)) {
			Optional<Database> collision = self->externalDatabases.get(name);
			ASSERT(collision.present() &&
			       collision.get()->getConnectionRecord()->getConnectionString().toString() == connectionString);

			return collision.get();
		}

		return db;
	}

	Future<Optional<Database>> getOrInsertDatabase(std::string name, std::string connectionString) {
		return getOrInsertDatabaseImpl(this, name, connectionString);
	}

	Future<Void> clearExternalDatabase(std::string databaseName) {
		Key key = KeyRef(databaseName).withPrefix(tenantBalancerExternalDatabasePrefix);

		return runTenantBalancerTransaction<Void>(
		    db, tbi.id(), "ClearExternalDatabase", [key](Reference<ReadYourWritesTransaction> tr) {
			    // This conflict range prevents a race if this transaction gets canceled and a new
			    // value is inserted while the commit is in flight.
			    tr->addReadConflictRange(singleKeyRange(key));
			    tr->clear(key);
			    return tr->commit();
		    });
	}

	Future<Void> clearMovementRecord(Reference<const MovementRecordMap::MutableRecord> mutableRecord) {
		auto& movements = mutableRecord->record->getMovementLocation() == MovementLocation::SOURCE ? outgoingMovements
		                                                                                           : incomingMovements;
		return movements.eraseRecord(db, mutableRecord);
	}

	ACTOR static Future<ErrorOr<Void>> recoverSourceMovement(TenantBalancer* self,
	                                                         Reference<MovementRecordMap::MutableRecord> mutableRecord);
	ACTOR static Future<ErrorOr<Void>> recoverDestinationMovement(
	    TenantBalancer* self,
	    Reference<MovementRecordMap::MutableRecord> mutableRecord);

	Future<ErrorOr<Void>> recoverSourceMovement(Reference<MovementRecordMap::MutableRecord> mutableRecord) {
		return recoverSourceMovement(this, mutableRecord);
	}
	Future<ErrorOr<Void>> recoverDestinationMovement(Reference<MovementRecordMap::MutableRecord> mutableRecord) {
		return recoverDestinationMovement(this, mutableRecord);
	}

	ACTOR static Future<Void> recoverImpl(TenantBalancer* self) {
		TraceEvent("TenantBalancerRecovering", self->tbi.id());
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

		state std::set<std::string> unusedDatabases;
		state std::vector<Reference<MovementRecord>> recoveredOutgoingMovements;
		state std::vector<Reference<MovementRecord>> recoveredIncomingMovements;

		state Key begin = tenantBalancerKeys.begin;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkTenantBalancerOwnership(self->tbi.id(), tr));
				Standalone<RangeResultRef> result =
				    wait(tr->getRange(KeyRangeRef(begin, tenantBalancerKeys.end), 1000));
				for (auto kv : result) {
					if (kv.key.startsWith(tenantBalancerSourceMovementPrefix)) {
						Reference<MovementRecord> record = MovementRecord::fromValue(kv.value);
						recoveredOutgoingMovements.push_back(record);

						TraceEvent(SevDebug, "TenantBalancerRecoverSourceMove", self->tbi.id())
						    .detail("MovementId", record->getMovementId())
						    .detail("SourcePrefix", record->getSourcePrefix())
						    .detail("DestinationPrefix", record->getDestinationPrefix())
						    .detail("DatabaseName", record->getPeerDatabaseName())
						    .detail("MovementState", record->movementState);
					} else if (kv.key.startsWith(tenantBalancerDestinationMovementPrefix)) {
						Reference<MovementRecord> record = MovementRecord::fromValue(kv.value);
						recoveredIncomingMovements.push_back(record);

						TraceEvent(SevDebug, "TenantBalancerRecoverDestinationMove", self->tbi.id())
						    .detail("MovementId", record->getMovementId())
						    .detail("SourcePrefix", record->getSourcePrefix())
						    .detail("DestinationPrefix", record->getDestinationPrefix())
						    .detail("DatabaseName", record->getPeerDatabaseName())
						    .detail("MovementState", record->movementState);
					} else if (kv.key.startsWith(tenantBalancerExternalDatabasePrefix)) {
						std::string name = kv.key.removePrefix(tenantBalancerExternalDatabasePrefix).toString();
						Database db = Database::createDatabase(
						    makeReference<ClusterConnectionKey>(self->db,
						                                        kv.key,
						                                        ClusterConnectionString(kv.value.toString()),
						                                        ConnectionStringNeedsPersisted::False),
						    Database::API_VERSION_LATEST,
						    IsInternal::True,
						    self->tbi.locality);

						self->externalDatabases.insert(name, db);
						unusedDatabases.insert(name);

						TraceEvent(SevDebug, "TenantBalancerRecoverDatabaseConnection", self->tbi.id())
						    .detail("Name", name)
						    .detail("ConnectionString", kv.value);
					} else {
						ASSERT(kv.key == tenantBalancerActiveProcessKey);
					}
				}

				if (result.more) {
					ASSERT(result.size() > 0);
					begin = keyAfter(result.rbegin()->key);
					tr->reset();
				} else {
					break;
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "TenantBalancerRecoveryError", self->tbi.id()).error(e);
				wait(tr->onError(e));
			}
		}

		state std::vector<Future<ErrorOr<Void>>> movementRecoveries;
		state std::vector<Reference<MovementRecord>>::iterator movementItr = recoveredOutgoingMovements.begin();
		while (movementItr != recoveredOutgoingMovements.end()) {
			Optional<Database> externalDb = self->externalDatabases.get((*movementItr)->getPeerDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverOutgoingMovementDatabase", self->tbi.id())
			    .detail("MovementId", (*movementItr)->getMovementId())
			    .detail("DatabaseName", (*movementItr)->getPeerDatabaseName())
			    .detail("DestinationConnectionString",
			            externalDb.get()->getConnectionRecord()->getConnectionString().toString())
			    .detail("SourcePrefix", (*movementItr)->getSourcePrefix())
			    .detail("DestinationPrefix", (*movementItr)->getDestinationPrefix());

			(*movementItr)->setPeerDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef((*movementItr)->getPeerDatabaseName());
			unusedDatabases.erase((*movementItr)->getPeerDatabaseName());

			Reference<MovementRecordMap::MutableRecord> mutableRecord =
			    wait(self->outgoingMovements.insertRecoveredRecord(*movementItr));
			movementRecoveries.push_back(recoverSourceMovement(self, mutableRecord));
			++movementItr;
		}

		movementItr = recoveredIncomingMovements.begin();
		while (movementItr != recoveredIncomingMovements.end()) {
			Optional<Database> externalDb = self->externalDatabases.get((*movementItr)->getPeerDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverIncomingMovementDatabase", self->tbi.id())
			    .detail("MovementId", (*movementItr)->getMovementId())
			    .detail("DatabaseName", (*movementItr)->getPeerDatabaseName())
			    .detail("SourceConnectionString",
			            externalDb.get()->getConnectionRecord()->getConnectionString().toString())
			    .detail("SourcePrefix", (*movementItr)->getSourcePrefix())
			    .detail("DestinationPrefix", (*movementItr)->getDestinationPrefix());

			(*movementItr)->setPeerDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef((*movementItr)->getPeerDatabaseName());
			unusedDatabases.erase((*movementItr)->getPeerDatabaseName());

			Reference<MovementRecordMap::MutableRecord> mutableRecord =
			    wait(self->incomingMovements.insertRecoveredRecord(*movementItr));
			movementRecoveries.push_back(self->recoverDestinationMovement(mutableRecord));
			++movementItr;
		}

		for (auto dbName : unusedDatabases) {
			TraceEvent(SevDebug, "TenantBalancerRecoveredUnusedDatabase", self->tbi.id())
			    .detail("DatabaseName", dbName);
			self->externalDatabases.markDeleted(dbName, self->clearExternalDatabase(dbName));
		}

		wait(waitForAll(movementRecoveries));

		TraceEvent("TenantBalancerRecovered", self->tbi.id());
		return Void();
	}
	Future<Void> recover() { return recoverImpl(this); }

	ACTOR static Future<Void> takeTenantBalancerOwnershipImpl(TenantBalancer* self) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

		TraceEvent("TenantBalancerTakeOwnership", self->tbi.id());

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->set(tenantBalancerActiveProcessKey, StringRef(self->tbi.id().toString()));
				wait(tr->commit());

				TraceEvent("TenantBalancerTookOwnership", self->tbi.id());
				return Void();
			} catch (Error& e) {
				TraceEvent(SevDebug, "TenantBalancerTakeOwnershipError", self->tbi.id()).error(e);
				wait(tr->onError(e));
			}
		}
	}
	Future<Void> takeTenantBalancerOwnership() { return takeTenantBalancerOwnershipImpl(this); }

	ACTOR static Future<bool> isTenantEmpty(Reference<ReadYourWritesTransaction> tr, Key prefix) {
		state RangeResult rangeResult = wait(tr->getRange(prefixRange(prefix), 1));
		return rangeResult.empty();
	}

	Future<bool> static isTenantEmpty(Database db, Key prefix) {
		return runRYWTransaction(db,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return isTenantEmpty(tr, prefix); });
	}

	MovementRecordMap const& getOutgoingMovements() const { return outgoingMovements; }
	MovementRecordMap const& getIncomingMovements() const { return incomingMovements; }

	CounterCollection tenantBalancerMetrics;

	Counter moveTenantToClusterRequests;
	Counter receiveTenantFromClusterRequests;
	Counter getActiveMovementsRequests;
	Counter getMovementStatusRequests;
	Counter finishSourceMovementRequests;
	Counter finishDestinationMovementRequests;
	Counter recoverMovementRequests;
	Counter abortMovementRequests;
	Counter cleanupMovementSourceRequests;

	ExternalDatabaseMap externalDatabases;

private:
	MovementRecordMap outgoingMovements;
	MovementRecordMap incomingMovements;
};

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

Future<Void> abortPeer(TenantBalancer* self, Reference<const MovementRecord> record) {
	return success(sendTenantBalancerRequest(
	    record->getPeerDatabase(),
	    AbortMovementRequest(record->getMovementId(),
	                         record->getRemotePrefix(),
	                         record->getMovementLocation() == MovementLocation::SOURCE ? MovementLocation::DEST
	                                                                                   : MovementLocation::SOURCE),
	    &TenantBalancerInterface::abortMovement));
}

ACTOR Future<ErrorOr<Void>> abortDr(TenantBalancer* self, Reference<const MovementRecord> record) {
	try {
		if (record->getMovementLocation() == MovementLocation::SOURCE) {
			wait(self->agent.abortBackup(record->getPeerDatabase(),
			                             record->getTagName(),
			                             PartialBackup{ false },
			                             AbortOldBackup::False,
			                             DstOnly{ false }));
		} else {
			state DatabaseBackupAgent sourceAgent(record->getPeerDatabase());
			wait(sourceAgent.abortBackup(
			    self->db, record->getTagName(), PartialBackup{ false }, AbortOldBackup::False, DstOnly{ false }));
		}
	} catch (Error& e) {
		return e;
	}

	return Void();
}

ACTOR Future<EBackupState> getDrState(TenantBalancer* self,
                                      Standalone<StringRef> tag,
                                      Reference<ReadYourWritesTransaction> tr) {
	UID logUid = wait(self->agent.getLogUid(tr, tag));
	EBackupState backupState = wait(self->agent.getStateValue(tr, logUid));
	return backupState;
}

ACTOR Future<EBackupState> getDrState(TenantBalancer* self, Reference<const MovementRecord> record) {
	Database db = record->getMovementLocation() == MovementLocation::SOURCE ? record->getPeerDatabase() : self->db;
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			EBackupState backupState = wait(getDrState(self, record->getTagName(), tr));
			return backupState;
		} catch (Error& e) {
			TraceEvent(SevDebug, "TenantBalancerGetDRStateError", self->tbi.id())
			    .error(e)
			    .detail("Tag", record->getTagName());
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<bool> checkForActiveDr(TenantBalancer* self, Reference<const MovementRecord> record) {
	EBackupState backupState = wait(getDrState(self, record));
	return backupState == EBackupState::STATE_SUBMITTED || backupState == EBackupState::STATE_RUNNING ||
	       backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL;
}

// Returns a new movement state based on the current movement state and DR state. If the movement state is an error,
// also includes an error message. In the event that the movement state should not change, return an empty Optional.
Optional<std::pair<MovementState, std::string>> drStateToMovementState(MovementState startingMovementState,
                                                                       EBackupState drState) {
	if (startingMovementState == MovementState::STARTED && drState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
		return std::make_pair(MovementState::READY_FOR_SWITCH, "");
	} else if (startingMovementState != MovementState::COMPLETED &&
	           (drState == EBackupState::STATE_ABORTED || drState == EBackupState::STATE_PARTIALLY_ABORTED)) {
		return std::make_pair(MovementState::ERROR, "The DR scheduled to move this data has been aborted.");
	} else if (drState == EBackupState::STATE_ERRORED) {
		return std::make_pair(MovementState::ERROR, "The DR scheduled to move this data has failed.");
	} else if (startingMovementState != MovementState::INITIALIZING && drState == EBackupState::STATE_NEVERRAN) {
		return std::make_pair(MovementState::ERROR, "The DR scheduled to move this data is missing.");
	}

	return Optional<std::pair<MovementState, std::string>>();
}

bool updateMovementRecordWithDrState(TenantBalancer* self,
                                     Reference<MovementRecord> record,
                                     DatabaseBackupStatus const* drStatus) {
	Optional<std::pair<MovementState, std::string>> newMovementState =
	    drStateToMovementState(record->movementState, drStatus->backupState);

	if (!newMovementState.present()) {
		return false;
	}

	if (newMovementState.get().first == MovementState::ERROR) {
		if (drStatus->backupState == EBackupState::STATE_ERRORED && drStatus->errorValues.size() == 1) {
			record->setMovementError(
			    format("%s: %s.",
			           newMovementState.get().second.substr(0, newMovementState.get().second.size() - 1).c_str(),
			           drStatus->errorValues[0].value.toString().c_str()));
		} else {
			record->setMovementError(newMovementState.get().second);
		}
	} else {
		record->movementState = newMovementState.get().first;
	}

	return true;
}

ACTOR Future<ReceiveTenantFromClusterReply> startSourceMovement(
    TenantBalancer* self,
    Reference<MovementRecordMap::MutableRecord> mutableRecord,
    bool cleanupOnError) {
	state Reference<MovementRecord> record = mutableRecord->record;

	// Send a request to the destination database to prepare for the move
	state ReceiveTenantFromClusterReply reply;
	try {
		ReceiveTenantFromClusterReply r =
		    wait(waitOrError(sendTenantBalancerRequest(
		                         record->getPeerDatabase(),
		                         ReceiveTenantFromClusterRequest(record->getMovementId(),
		                                                         record->getSourcePrefix(),
		                                                         record->getDestinationPrefix(),
		                                                         self->connRecord->getConnectionString().toString()),
		                         &TenantBalancerInterface::receiveTenantFromCluster),
		                     record->onAbort()));

		reply = r;
	} catch (Error& e) {
		state Error destError = e;
		if (cleanupOnError) {
			wait(self->clearMovementRecord(mutableRecord));
		}

		throw destError;
	}

	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	backupRanges.push_back_deep(backupRanges.arena(), prefixRange(record->getSourcePrefix()));

	// Submit a DR to move the target range
	bool replacePrefix = record->getSourcePrefix() != record->getDestinationPrefix();

	try {
		wait(waitOrError(self->agent.submitBackup(record->getPeerDatabase(),
		                                          record->getTagName(),
		                                          backupRanges,
		                                          StopWhenDone::False,
		                                          replacePrefix ? record->getDestinationPrefix() : StringRef(),
		                                          replacePrefix ? record->getSourcePrefix() : StringRef(),
		                                          LockDB::False),
		                 record->onAbort()));
	} catch (Error& e) {
		state Error submitErr = e;

		if (cleanupOnError) {
			try {
				state Future<Void> abortPeerFuture = abortPeer(self, record);
				ErrorOr<Void> abortDrResult = wait(abortDr(self, record));
				if (!abortDrResult.isError() || abortDrResult.getError().code() == error_code_backup_unneeded) {
					wait(self->clearMovementRecord(mutableRecord));
				} else {
					TraceEvent(SevWarn, "TenantBalancerStartMovementAbortDRFailed", self->tbi.id())
					    .error(abortDrResult.getError())
					    .detail("MovementId", record->getMovementId())
					    .detail("SourcePrefix", record->getSourcePrefix())
					    .detail("DestinationPrefix", record->getDestinationPrefix())
					    .detail("DestinationConnectionString",
					            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

					record->setMovementError(format("Failed to start movement: %s", submitErr.what()));
					wait(self->saveMovementRecord(mutableRecord));
				}
				wait(abortPeerFuture);
			} catch (Error& cleanupErr) {
				TraceEvent(SevWarn, "TenantBalancerStartMovementCleanupFailed", self->tbi.id())
				    .error(cleanupErr)
				    .detail("MovementId", record->getMovementId())
				    .detail("SourcePrefix", record->getSourcePrefix())
				    .detail("DestinationPrefix", record->getDestinationPrefix())
				    .detail("DestinationConnectionString",
				            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());
			}
		}

		throw submitErr;
	}

	// Update the state of the movement to started
	record->movementState = MovementState::STARTED;
	wait(waitOrError(self->saveMovementRecord(mutableRecord), record->onAbort()));

	return reply;
}

ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerMoveTenantToCluster", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("DestinationConnectionString", req.destConnectionString);

	++self->moveTenantToClusterRequests;

	try {
		state Reference<MovementRecordMap::MutableRecord> mutableRecord = wait(self->createAndGetMovementRecord(
		    MovementLocation::SOURCE, req.sourcePrefix, req.destPrefix, req.destConnectionString));
		state Reference<MovementRecord> record = mutableRecord->record;

		// Start the movement
		state ReceiveTenantFromClusterReply replyFromDestinationDatabase =
		    wait(startSourceMovement(self, mutableRecord, true));

		// Check if a DR agent is running to process the move
		state bool agentRunning = wait(self->agent.checkActive(record->getPeerDatabase()));
		if (!agentRunning) {
			throw movement_agent_not_running();
		}

		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

		MoveTenantToClusterReply reply(record->getMovementId(), replyFromDestinationDatabase.tenantName);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterError", self->tbi.id())
		    .error(e)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromCluster", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("SourceConnectionString", req.srcConnectionString);

	++self->receiveTenantFromClusterRequests;

	try {
		state Reference<MovementRecordMap::MutableRecord> destinationMovementRecord;
		try {
			Reference<MovementRecordMap::MutableRecord> r =
			    wait(self->mutateIncomingMovement(req.destPrefix, req.movementId));
			destinationMovementRecord = r;
		} catch (Error& e) {
			state Error getMovementError = e;
			if (e.code() == error_code_movement_not_found) {
				Reference<MovementRecordMap::MutableRecord> r = wait(self->createAndGetMovementRecord(
				    MovementLocation::DEST, req.sourcePrefix, req.destPrefix, req.srcConnectionString, req.movementId));
				destinationMovementRecord = r;
			} else {
				throw getMovementError;
			}
		}

		state Reference<MovementRecord> record = destinationMovementRecord->record;
		state std::string lockedTenant = "";
		if (record->movementState == MovementState::INITIALIZING) {
			// Check if prefix is empty.
			bool isPrefixEmpty = wait(self->isTenantEmpty(self->db, req.destPrefix));
			if (!isPrefixEmpty) {
				throw movement_dest_prefix_not_empty();
			}

			wait(waitOrError(self->saveMovementRecord(destinationMovementRecord), record->onAbort()));

			// TODO: Lock the destination before we start the movement

			// Update record
			record->movementState = MovementState::STARTED;
			wait(waitOrError(self->saveMovementRecord(destinationMovementRecord), record->onAbort()));
		}

		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterComplete", self->tbi.id())
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		ReceiveTenantFromClusterReply reply(lockedTenant);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		req.reply.sendError(e);
	}

	return Void();
}

void filterActiveMoves(MovementRecordMap const& movements,
                       std::vector<Reference<const MovementRecord>>& filteredMovements,
                       Optional<Key> prefixFilter,
                       Optional<std::string> peerDatabaseConnectionStringFilter) {
	auto itr = prefixFilter.present() ? movements.find(prefixFilter.get()) : movements.begin();
	while (itr != movements.end()) {
		if (!peerDatabaseConnectionStringFilter.present() ||
		    peerDatabaseConnectionStringFilter.get() ==
		        itr->second->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString()) {
			filteredMovements.push_back(itr->second);
		}
		if (prefixFilter.present()) {
			break;
		}

		++itr;
	}
}

std::vector<Reference<const MovementRecord>> getFilteredMovements(
    TenantBalancer* self,
    Optional<Key> prefixFilter,
    Optional<std::string> peerDatabaseConnectionStringFilter,
    Optional<MovementLocation> locationFilter) {
	std::vector<Reference<const MovementRecord>> filteredMovements;

	if (!locationFilter.present() || locationFilter.get() == MovementLocation::SOURCE) {
		filterActiveMoves(
		    self->getOutgoingMovements(), filteredMovements, prefixFilter, peerDatabaseConnectionStringFilter);
	}
	if (!locationFilter.present() || locationFilter.get() == MovementLocation::DEST) {
		filterActiveMoves(
		    self->getIncomingMovements(), filteredMovements, prefixFilter, peerDatabaseConnectionStringFilter);
	}

	return filteredMovements;
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	++self->getActiveMovementsRequests;

	TraceEvent(SevDebug, "TenantBalancerGetActiveMovements", self->tbi.id())
	    .detail("PrefixFilter", req.prefixFilter)
	    .detail("LocationFilter",
	            req.locationFilter.present()
	                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
	                : "[not set]")
	    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);

	try {
		state std::vector<Reference<const MovementRecord>> filteredMovements =
		    getFilteredMovements(self, req.prefixFilter, req.peerDatabaseConnectionStringFilter, req.locationFilter);

		state std::vector<Future<EBackupState>> backupStateFutures;
		for (auto record : filteredMovements) {
			backupStateFutures.push_back(getDrState(self, record));
		}

		wait(waitForAll(backupStateFutures));

		GetActiveMovementsReply reply;
		for (int i = 0; i < filteredMovements.size(); ++i) {
			Optional<std::pair<MovementState, std::string>> newMovementState =
			    drStateToMovementState(filteredMovements[i]->movementState, backupStateFutures[i].get());

			reply.activeMovements.emplace_back(
			    filteredMovements[i]->getMovementId(),
			    filteredMovements[i]->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString(),
			    filteredMovements[i]->getSourcePrefix(),
			    filteredMovements[i]->getDestinationPrefix(),
			    newMovementState.present() ? newMovementState.get().first : filteredMovements[i]->movementState);
		}

		TraceEvent(SevDebug, "TenantBalancerGetActiveMovementsComplete", self->tbi.id())
		    .detail("PrefixFilter", req.prefixFilter)
		    .detail("LocationFilter",
		            req.locationFilter.present()
		                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
		                : "[not set]")
		    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);

		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetActiveMovementsError", self->tbi.id())
		    .error(e)
		    .detail("PrefixFilter", req.prefixFilter)
		    .detail("LocationFilter",
		            req.locationFilter.present()
		                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
		                : "[not set]")
		    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Version> getDatabaseVersion(Database db) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Version v = wait(tr->getReadVersion());
			return v;
		} catch (Error& e) {
			TraceEvent(SevDebug, "TenantBalancerGetDatabaseVersionError").error(e);
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<double> getDatabaseVersionLag(TenantBalancer* self, Reference<const MovementRecord> record) {
	state Version sourceVersion;
	state Version destVersion;

	if (record->getMovementLocation() == MovementLocation::SOURCE) {
		Version dv = wait(getDatabaseVersion(record->getPeerDatabase()));
		destVersion = dv;
		Version sv = wait(getDatabaseVersion(self->db));
		sourceVersion = sv;
	} else {
		Version dv = wait(getDatabaseVersion(self->db));
		destVersion = dv;
		Version sv = wait(getDatabaseVersion(record->getPeerDatabase()));
		sourceVersion = sv;
	}

	return std::max<double>(sourceVersion - destVersion, 0) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
}

ACTOR Future<TenantMovementStatus> getStatusAndUpdateMovementRecord(TenantBalancer* self,
                                                                    Reference<MovementRecord> record) {
	state DatabaseBackupStatus drStatus;
	if (record->getMovementLocation() == MovementLocation::SOURCE) {
		DatabaseBackupStatus s = wait(self->agent.getStatusData(record->getPeerDatabase(), 1, record->getTagName()));
		drStatus = s;
	} else {
		state DatabaseBackupAgent sourceAgent(record->getPeerDatabase());
		DatabaseBackupStatus s = wait(sourceAgent.getStatusData(self->db, 1, record->getTagName()));
		drStatus = s;
	}

	updateMovementRecordWithDrState(self, record, &drStatus);

	state TenantMovementStatus status;
	status.tenantMovementInfo =
	    TenantMovementInfo(record->getMovementId(),
	                       record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString(),
	                       record->getSourcePrefix(),
	                       record->getDestinationPrefix(),
	                       record->movementState);

	// TODO update isSourceLocked and isDestinationLocked
	status.isSourceLocked = false;
	status.isDestinationLocked = false;

	if (record->movementState == MovementState::STARTED || record->movementState == MovementState::READY_FOR_SWITCH ||
	    record->movementState == MovementState::SWITCHING) {
		double versionLag = wait(getDatabaseVersionLag(self, record));
		status.databaseVersionLag = versionLag;
		if (drStatus.secondsBehind != -1) {
			status.mutationLag = drStatus.secondsBehind;
		}
	}

	if (record->switchVersion != invalidVersion) {
		status.switchVersion = record->switchVersion;
	}

	status.errorMessage = record->getErrorMessage();
	return status;
}

ACTOR Future<Void> getMovementStatus(TenantBalancer* self, GetMovementStatusRequest req) {
	++self->getMovementStatusRequests;

	TraceEvent(SevDebug, "TenantBalancerGetMovementStatus", self->tbi.id())
	    .detail("Prefix", req.prefix)
	    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation));

	try {
		state Optional<Reference<MovementRecordMap::MutableRecord>> mutableRecord =
		    wait(self->tryMutateMovement(req.movementLocation, req.prefix));
		state Reference<MovementRecord> record;

		if (mutableRecord.present()) {
			record = mutableRecord.get()->record;
		} else {
			record = self->getMovementSnapshot(req.movementLocation, req.prefix)->clone();
		}

		state TenantMovementStatus status = wait(getStatusAndUpdateMovementRecord(self, record));
		if (mutableRecord.present()) {
			wait(self->saveMovementRecord(mutableRecord.get()));
		}

		TraceEvent(SevDebug, "TenantBalancerGetMovementStatusComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("MovementLocation",
		            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix())
		    .detail("PeerConnectionString",
		            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
		    .detail("MovementStatus", status.toJson());

		GetMovementStatusReply reply(status);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetMovementStatusError", self->tbi.id())
		    .error(e)
		    .detail("Prefix", req.prefix)
		    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation));

		req.reply.sendError(e);
	}

	return Void();
}

Future<Version> lockSourceTenant(TenantBalancer* self, Key prefix) {
	TraceEvent("TenantBalancerLockSourceTenant", self->tbi.id()).detail("Prefix", prefix);
	return runTenantBalancerTransaction<Version>(
	    self->db, self->tbi.id(), "LockSourceTenant", [](Reference<ReadYourWritesTransaction> tr) {
		    tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		    return tr->getReadVersion();
	    });
}

ACTOR Future<Void> sendFinishRequestToDestination(TenantBalancer* self,
                                                  Reference<MovementRecordMap::MutableRecord> mutableRecord) {
	state Reference<MovementRecord> record = mutableRecord->record;
	FinishDestinationMovementReply destinationReply = wait(waitOrError(
	    sendTenantBalancerRequest(record->getPeerDatabase(),
	                              FinishDestinationMovementRequest(
	                                  record->getMovementId(), record->getDestinationPrefix(), record->switchVersion),
	                              &TenantBalancerInterface::finishDestinationMovement),
	    record->onAbort()));

	record->movementState = MovementState::COMPLETED;
	wait(waitOrError(self->saveMovementRecord(mutableRecord), record->onAbort()));
	wait(abortPeer(self, record));

	return Void();
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	++self->finishSourceMovementRequests;

	// TODO: handle a new finish request if one is in flight

	TraceEvent(SevDebug, "TenantBalancerFinishSourceMovement", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("MaxLagSeconds", req.maxLagSeconds);

	try {
		state Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(self->mutateOutgoingMovement(req.sourcePrefix));
		state Reference<MovementRecord> record = mutableRecord->record;

		state TenantMovementStatus movementStatus =
		    wait(waitOrError(getStatusAndUpdateMovementRecord(self, record), record->onAbort()));

		wait(self->saveMovementRecord(mutableRecord));

		if (record->movementState == MovementState::ERROR) {
			TraceEvent(SevWarn, "TenantBalancerCannotFinish", self->tbi.id())
			    .detail("MovementId", record->getMovementId())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix())
			    .detail("PeerConnectionString",
			            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
			    .detail("MovementError", record->getErrorMessage());

			throw movement_error();
		}
		if (record->movementState != MovementState::READY_FOR_SWITCH) {
			throw movement_not_ready_for_operation();
		}
		if (movementStatus.mutationLag.orDefault(DBL_MAX) > req.maxLagSeconds) {
			TraceEvent(SevDebug, "TenantBalancerLagCheckFailed", self->tbi.id())
			    .detail("MaxLagSeconds", req.maxLagSeconds)
			    .detail("CurrentLagSeconds", movementStatus.mutationLag);
			throw movement_lag_too_large();
		}

		TraceEvent(SevDebug, "TenantBalancerStartingSwitch", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix())
		    .detail("DestinationConnectionString",
		            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
		    .detail("MaxLagSeconds", req.maxLagSeconds)
		    .detail("CurrentLagSeconds", movementStatus.mutationLag);

		state std::string destinationConnectionString =
		    record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString();

		state Version version = wait(waitOrError(lockSourceTenant(self, req.sourcePrefix), record->onAbort()));
		// TODO: get a locked tenant
		state std::string lockedTenant = "";
		record->switchVersion = version;

		// Update movement record
		record->movementState = MovementState::SWITCHING;
		wait(waitOrError(self->saveMovementRecord(mutableRecord), record->onAbort()));
		wait(waitOrError(sendFinishRequestToDestination(self, mutableRecord), record->onAbort()));

		TraceEvent(SevDebug, "TenantBalancerFinishSourceMovementComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix())
		    .detail("DestinationConnectionString",
		            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

		FinishSourceMovementReply reply(lockedTenant, version);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerFinishSourceMovementError", self->tbi.id())
		    .error(e)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("MaxLagSeconds", req.maxLagSeconds);
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	++self->finishDestinationMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovement", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("DestinationPrefix", req.destinationPrefix)
	    .detail("SwitchVersion", req.version);

	try {
		state Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(self->mutateIncomingMovement(Key(req.destinationPrefix), req.movementId));
		state Reference<MovementRecord> record = mutableRecord->record;

		if (record->movementState == MovementState::STARTED) {
			EBackupState backupState = wait(getDrState(self, record));
			if (backupState != EBackupState::STATE_RUNNING_DIFFERENTIAL) {
				TraceEvent(SevWarn, "TenantBalancerInvalidFinishDestinationMovementRequest", self->tbi.id())
				    .detail("MovementId", record->getMovementId())
				    .detail("SourcePrefix", record->getSourcePrefix())
				    .detail("DestinationPrefix", record->getDestinationPrefix())
				    .detail("MovementState", TenantBalancerInterface::movementStateToString(record->movementState))
				    .detail("BackupState", DatabaseBackupAgent::getStateName(backupState));

				throw movement_error();
			}
		}

		if (record->movementState == MovementState::STARTED ||
		    record->movementState == MovementState::READY_FOR_SWITCH) {
			record->movementState = MovementState::SWITCHING;
			record->switchVersion = req.version;
			wait(waitOrError(self->saveMovementRecord(mutableRecord), record->onAbort()));
		}

		if (record->movementState == MovementState::SWITCHING) {
			state DatabaseBackupAgent sourceBackupAgent(record->getPeerDatabase());
			wait(waitOrError(sourceBackupAgent.flushBackup(self->db, record->getTagName(), record->switchVersion),
			                 record->onAbort()));
			// TODO: unlock DR prefix

			record->movementState = MovementState::COMPLETED;
			wait(waitOrError(self->saveMovementRecord(mutableRecord), record->onAbort()));
		}

		if (record->movementState != MovementState::COMPLETED) {
			TraceEvent(SevWarn, "TenantBalancerFinishDestinationMovementFailed", self->tbi.id())
			    .detail("MovementId", record->getMovementId())
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix())
			    .detail("MovementState", TenantBalancerInterface::movementStateToString(record->movementState));

			throw movement_error();
		}

		TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovementComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix())
		    .detail("SourceConnectionString",
		            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
		    .detail("SwitchVersion", req.version);

		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovementError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.destinationPrefix)
		    .detail("SwitchVersion", req.version);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> recoverMovement(TenantBalancer* self, RecoverMovementRequest req) {
	++self->recoverMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerRecoverMovement", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("Prefix", req.prefix);

	try {
		state Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(self->mutateOutgoingMovement(req.prefix, req.movementId));
		ErrorOr<Void> result = wait(self->recoverSourceMovement(mutableRecord));

		if (!result.isError()) {
			TraceEvent(SevDebug, "TenantBalancerRecoverMovementComplete", self->tbi.id())
			    .detail("MovementId", req.movementId)
			    .detail("Prefix", req.prefix);

			RecoverMovementReply reply;
			req.reply.send(reply);
		} else {
			req.reply.sendError(result.getError());
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerRecoverMovementError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("Prefix", req.prefix);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	++self->abortMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerAbortMovement", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation))
	    .detail("Prefix", req.prefix);

	try {
		state Reference<const MovementRecord> record =
		    self->getMovementSnapshot(req.movementLocation, req.prefix, req.movementId);
		record->abort(); // abort other in-flight movements

		state Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(self->mutateMovement(req.movementLocation, req.prefix, req.movementId));

		record = mutableRecord->record;

		ErrorOr<Void> result = wait(abortDr(self, record));
		if (result.isError() && result.getError().code() != error_code_backup_unneeded) {
			throw result.getError();
		}

		wait(self->clearMovementRecord(mutableRecord));

		TraceEvent(SevDebug, "TenantBalancerAbortComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("MovementLocation",
		            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix())
		    .detail("PeerConnectionString",
		            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerAbortError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation))
		    .detail("Prefix", req.prefix);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	++self->cleanupMovementSourceRequests;

	TraceEvent(SevDebug, "TenantBalancerCleanupMovementSource", self->tbi.id())
	    .detail("Prefix", req.prefix)
	    .detail("CleanupType", req.cleanupType);

	try {
		state Reference<MovementRecordMap::MutableRecord> mutableRecord =
		    wait(self->mutateOutgoingMovement(req.prefix));
		state Reference<MovementRecord> record = mutableRecord->record;

		if (record->movementState != MovementState::COMPLETED) {
			TraceEvent(SevDebug, "TenantBalancerMovementNotReadyForCleanup", self->tbi.id())
			    .detail("Prefix", req.prefix)
			    .detail("CurrentState", TenantBalancerInterface::movementStateToString(record->movementState));

			throw movement_not_ready_for_operation();
		}

		// Erase the moved data, if desired
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::UNLOCK) {
			KeyRange rangeToErase = prefixRange(req.prefix);
			wait(
			    waitOrError(runTenantBalancerTransaction<Void>(self->db,
			                                                   self->tbi.id(),
			                                                   "CleanupMovementSourceErase",
			                                                   [rangeToErase](Reference<ReadYourWritesTransaction> tr) {
				                                                   tr->clear(rangeToErase);
				                                                   return tr->commit();
			                                                   }),
			                record->onAbort()));

			TraceEvent("TenantBalancerPrefixErased", self->tbi.id())
			    .detail("MovementId", record->getMovementId())
			    .detail("Prefix", req.prefix);
		}

		// Unlock the moved range, if desired
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::ERASE) {
			// TODO unlock tenant

			wait(waitOrError(self->clearMovementRecord(mutableRecord), record->onAbort()));

			TraceEvent(SevDebug, "TenantBalancerPrefixUnlocked", self->tbi.id())
			    .detail("MovementId", record->getMovementId())
			    .detail("Prefix", req.prefix);
		}

		TraceEvent(SevDebug, "TenantBalancerCleanupMovementSourceComplete", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("Prefix", req.prefix)
		    .detail("CleanupType", req.cleanupType);

		CleanupMovementSourceReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerCleanupMovementSourceError", self->tbi.id())
		    .error(e)
		    .detail("Prefix", req.prefix)
		    .detail("CleanupType", req.cleanupType);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovementDueToFailedDr(TenantBalancer* self,
                                              Reference<MovementRecordMap::MutableRecord> mutableRecord) {
	state Reference<MovementRecord> record = mutableRecord->record;
	TraceEvent(SevWarn, "TenantBalancerRecoverMovementAborted", self->tbi.id())
	    .detail("Reason", "DR is not running")
	    .detail("MovementId", record->getMovementId())
	    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
	    .detail("MovementState", TenantBalancerInterface::movementStateToString(record->movementState))
	    .detail("DestinationDatabase",
	            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
	    .detail("SourcePrefix", record->getSourcePrefix())
	    .detail("DestinationPrefix", record->getDestinationPrefix());

	wait(abortPeer(self, record) && success(abortDr(self, record)));

	record->setMovementError("The DR copying the data has failed or stopped running");
	wait(self->saveMovementRecord(mutableRecord));

	return Void();
}

ACTOR Future<ErrorOr<Void>> TenantBalancer::recoverSourceMovement(
    TenantBalancer* self,
    Reference<MovementRecordMap::MutableRecord> mutableRecord) {
	state Reference<MovementRecord> record = mutableRecord->record;

	try {
		bool activeDr = wait(checkForActiveDr(self, record));

		if (record->movementState == MovementState::INITIALIZING) {
			// If DR is already running, then we can just move to the started phase.
			if (activeDr) {
				record->movementState = MovementState::STARTED;
				wait(self->saveMovementRecord(mutableRecord));
			}
			// Otherwise, attempt to start the movement
			else {
				ReceiveTenantFromClusterReply reply = wait(startSourceMovement(self, mutableRecord, false));
			}
		} else if (record->movementState == MovementState::STARTED) {
			if (!activeDr) {
				wait(abortMovementDueToFailedDr(self, mutableRecord));
			}
		} else if (record->movementState == MovementState::READY_FOR_SWITCH) {
			// TODO: unlock the source
			if (!activeDr) {
				wait(abortMovementDueToFailedDr(self, mutableRecord));
			}
		} else if (record->movementState == MovementState::SWITCHING) {
			wait(sendFinishRequestToDestination(self, mutableRecord));
		} else if (record->movementState == MovementState::COMPLETED) {
			wait(abortPeer(self, record));
		} else if (record->movementState == MovementState::ERROR) {
			// Do nothing
		} else {
			ASSERT(false);
		}
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarn, "TenantBalancerRecoverSourceMovementError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", record->getMovementId())
		    .detail("MovementState", TenantBalancerInterface::movementStateToString(record->movementState))
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix());

		record->setMovementError(format("Could not recover movement: %s", e.what()));
		wait(self->saveMovementRecord(mutableRecord));

		return err;
	}

	return Void();
}

ACTOR Future<ErrorOr<Void>> TenantBalancer::recoverDestinationMovement(
    TenantBalancer* self,
    Reference<MovementRecordMap::MutableRecord> mutableRecord) {
	state Reference<MovementRecord> record = mutableRecord->record;
	try {
		choose {
			when(wait(delay(SERVER_KNOBS->TENANT_BALANCER_MOVEMENT_RECOVERY_TIMEOUT))) { throw timed_out(); }
			when(RecoverMovementReply reply = wait(sendTenantBalancerRequest(
			         record->getPeerDatabase(),
			         RecoverMovementRequest(record->getMovementId(), record->getSourcePrefix()),
			         &TenantBalancerInterface::recoverMovement))) {
				return Void();
			}
		}
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarn, "TenantBalancerRecoverDestinationMovementError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", record->getMovementId())
		    .detail("DatabaseName", record->getPeerDatabaseName())
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix());

		record->setMovementError(format("Could not recover movement: %s", e.what()));
		wait(self->saveMovementRecord(mutableRecord));
		return err;
	}
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	TraceEvent("TenantBalancerStarting", self->tbi.id());
	loop choose {
		when(MoveTenantToClusterRequest req = waitNext(self->tbi.moveTenantToCluster.getFuture())) {
			self->actors.add(moveTenantToCluster(self, req));
		}
		when(ReceiveTenantFromClusterRequest req = waitNext(self->tbi.receiveTenantFromCluster.getFuture())) {
			self->actors.add(receiveTenantFromCluster(self, req));
		}
		when(GetActiveMovementsRequest req = waitNext(self->tbi.getActiveMovements.getFuture())) {
			self->actors.add(getActiveMovements(self, req));
		}
		when(GetMovementStatusRequest req = waitNext(self->tbi.getMovementStatus.getFuture())) {
			self->actors.add(getMovementStatus(self, req));
		}
		when(FinishSourceMovementRequest req = waitNext(self->tbi.finishSourceMovement.getFuture())) {
			self->actors.add(finishSourceMovement(self, req));
		}
		when(FinishDestinationMovementRequest req = waitNext(self->tbi.finishDestinationMovement.getFuture())) {
			self->actors.add(finishDestinationMovement(self, req));
		}
		when(RecoverMovementRequest req = waitNext(self->tbi.recoverMovement.getFuture())) {
			self->actors.add(recoverMovement(self, req));
		}
		when(AbortMovementRequest req = waitNext(self->tbi.abortMovement.getFuture())) {
			self->actors.add(abortMovement(self, req));
		}
		when(CleanupMovementSourceRequest req = waitNext(self->tbi.cleanupMovementSource.getFuture())) {
			self->actors.add(cleanupMovementSource(self, req));
		}
		when(wait(self->actors.getResult())) {}
	}
}

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi,
                                  Reference<AsyncVar<ServerDBInfo> const> db,
                                  Reference<IClusterConnectionRecord> connRecord) {
	state TenantBalancer self(tbi, db, connRecord);

	try {
		wait(self.takeTenantBalancerOwnership());
		wait(self.recover());
		wait(tenantBalancerCore(&self));
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated", tbi.id()).error(e);
		throw e;
	}
}