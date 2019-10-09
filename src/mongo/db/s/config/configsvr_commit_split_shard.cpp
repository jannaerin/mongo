/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/shard_key_util.h"
#include "mongo/db/server_options.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/split_shard_gen.h"
#include "mongo/util/log.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/s/range_deletion_task_gen.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"

namespace mongo {
namespace {

bool writeRangeDeletionBatch(OperationContext* opCtx, std::shared_ptr<Shard> shard, std::vector<ChunkType>* orphanedChunks, std::map<NamespaceString, mongo::UUID> nssToUuid) {
    BSONArrayBuilder arrBuilder;
    auto iter = orphanedChunks->begin();
    for (; iter != orphanedChunks->end(); ++iter) {
        auto chunk = *iter;

        // Get the UUID for this collection
        boost::optional<mongo::UUID> collUUID;
        auto iter = nssToUuid.find(chunk.getNS());
        if (iter == nssToUuid.end()) {
            invariant(chunk.getNS().db() == NamespaceString::kConfigDb);
            const auto catalogClient = Grid::get(opCtx)->catalogClient();
            auto collInfo = uassertStatusOK(catalogClient->getCollection(opCtx, chunk.getNS(), repl::ReadConcernLevel::kLocalReadConcern));
            collUUID = collInfo.value.getUUID().get();
        }
        collUUID = iter->second;

        // Create the RangeDeletionTask object
        RangeDeletionTask rangeDeletion(chunk.getNS(), collUUID.get(), CleanWhenEnum::kDelayed);
        rangeDeletion.setRange(ChunkRange(chunk.getMin(), chunk.getMax()));
        auto rangeDeletionObj = rangeDeletion.toBSON();

        if (arrBuilder.arrSize() && (arrBuilder.len() + rangeDeletionObj.objsize() + 1024) > BSONObjMaxUserSize) {
            break;
        }

        arrBuilder.append(rangeDeletionObj);
    }

    orphanedChunks->erase(orphanedChunks->begin(), iter);

    // Write to config.rangeDeletions
    std::vector<BSONObj> rangeDeletions;
    auto arr = arrBuilder.arr();
    auto it = arr.begin();

    if (it == arr.end())
        return false;

    while (it != arr.end()) {
        const auto& doc = *it;
        rangeDeletions.push_back(doc.Obj());
        ++it;
    }

    write_ops::Insert insertOp(NamespaceString::kRangeDeletionNamespace);
    insertOp.setDocuments(rangeDeletions);
    auto insertOpMsg = OpMsgRequest::fromDBAndBody(NamespaceString::kConfigDb.toString(), insertOp.toBSON({}));
    auto insertRequest = BatchedCommandRequest::parseInsert(insertOpMsg);
    insertRequest.setAllowImplicitCreate(true);
    BatchedCommandResponse insertResponse = shard->runBatchWriteCommand(opCtx,
                                                       Shard::kDefaultConfigCommandTimeout,
                                                       insertRequest,
                                                       Shard::RetryPolicy::kNotIdempotent);
    uassertStatusOK(insertResponse.toStatus());
    return true;
}

void writeToRangeDeletions(OperationContext* opCtx, std::shared_ptr<Shard> shard, std::vector<ChunkType> orphanedChunks, std::map<NamespaceString, mongo::UUID> nssToUuid) {
    if (orphanedChunks.size() < 1)
        return;

    bool batchWritten;
    do {
        batchWritten = writeRangeDeletionBatch(opCtx, shard, &orphanedChunks, nssToUuid);
    } while (batchWritten);
}

void markOrphanedChunksForDeletion(OperationContext* opCtx, std::shared_ptr<Shard> fromShard, std::shared_ptr<Shard> newShard, std::vector<ChunkType> chunksOwnedByFromShard, std::vector<ChunkType> chunksToMoveToNewShard, std::map<NamespaceString, mongo::UUID> nssToUuid) {
    // Flush the routing table on both shards for every namespace we moved chunks for. This is so that the shards' metadata manager will have be updated and the range deleter can delete orphaned chunks
    for (const auto& chunk : chunksOwnedByFromShard) {
        auto refreshCmdResponseFromShard = uassertStatusOK(fromShard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            "admin",
            BSON("_flushRoutingTableCacheUpdates" << chunk.getNS().toString() << "syncFromConfig" << true),
            Seconds{30},
            Shard::RetryPolicy::kIdempotent));
        uassertStatusOK(refreshCmdResponseFromShard.commandStatus);

        auto refreshCmdResponseNewShard = uassertStatusOK(newShard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            "admin",
            BSON("_flushRoutingTableCacheUpdates" << chunk.getNS().toString() << "syncFromConfig" << true),
            Seconds{30},
            Shard::RetryPolicy::kIdempotent));
        uassertStatusOK(refreshCmdResponseNewShard.commandStatus);

    }

    // Add chunks to config.rangeDeletions on the from shard that are now owned by the new shard
    writeToRangeDeletions(opCtx, fromShard, std::move(chunksToMoveToNewShard), nssToUuid);

    // Add chunks that remain owned by the original shard to config.rangeDeletions to the new shard
    std::vector<ChunkType> chunksNotMoved(chunksOwnedByFromShard.begin(), chunksOwnedByFromShard.begin() + chunksOwnedByFromShard.size() / 2);
    writeToRangeDeletions(opCtx, newShard, std::move(chunksNotMoved), nssToUuid);
}

std::vector<std::string> getUnshardedCollectionsForDb(OperationContext* opCtx, std::shared_ptr<Shard> fromShard, std::string dbName, std::map<NamespaceString, mongo::UUID> nssToUuid) {
    BSONObj res;
    auto listCollectionsCmd =
        BSON("listCollections" << 1);
    auto allRes = uassertStatusOK(fromShard->runExhaustiveCursorCommand(
        opCtx,
        ReadPreferenceSetting(ReadPreference::PrimaryOnly),
        dbName,
        listCollectionsCmd,
        Milliseconds(-1)));
    const auto& allCollections = allRes.docs;

    std::vector<std::string> unshardedCollections;
    for (const auto& coll : allCollections) {
        std::string collName;
        uassertStatusOK(bsonExtractStringField(coll, "name", &collName));
        NamespaceString nss(dbName, collName);

        if (nssToUuid.find(nss) != nssToUuid.end()) {
            continue;
        }

        unshardedCollections.push_back(collName);
    }

    return unshardedCollections;
}

void dropCollectionsFromShard(OperationContext* opCtx, std::shared_ptr<Shard> shard, std::vector<std::string> unshardedCollections, std::string dbName) {
    for (const auto& nss : unshardedCollections) {
        auto swDropResult = shard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            dbName,
            BSON("drop" << nss),
            Shard::RetryPolicy::kIdempotent);

        const std::string dropCollectionErrMsg = str::stream()
            << "Error dropping collection on shard " << shard->getId();

        auto dropResult = uassertStatusOKWithContext(swDropResult, dropCollectionErrMsg);
        uassertStatusOKWithContext(dropResult.writeConcernStatus, dropCollectionErrMsg);

        auto dropCommandStatus = std::move(dropResult.commandStatus);
        if (dropCommandStatus.code() == ErrorCodes::NamespaceNotFound) {
            // The dropCollection command on the shard is not idempotent, and can return
            // NamespaceNotFound. We can ignore NamespaceNotFound since we have already asserted
            // that there is no writeConcern error.
            continue;
        }

        uassertStatusOKWithContext(dropCommandStatus, dropCollectionErrMsg);
    }
}

void dropOrphanedUnshardedCollections(OperationContext* opCtx, std::shared_ptr<Shard> fromShard, std::shared_ptr<Shard> newShard, std::vector<std::string> dbsOwnedByShard, std::vector<std::string> dbsToMoveToNewShard, std::map<NamespaceString, mongo::UUID> nssToUuid) {
    for (std::string db : dbsOwnedByShard) {
        if (db == NamespaceString::kConfigDb)
            continue;

        auto unshardedCollections = getUnshardedCollectionsForDb(opCtx, fromShard, db, nssToUuid);

        auto iter = std::find(dbsToMoveToNewShard.begin(), dbsToMoveToNewShard.end(), db);
        if (iter != dbsToMoveToNewShard.end()) {
            // This db now belongs to the new shard, so drop all of the unsharded collections from the old
            dropCollectionsFromShard(opCtx, fromShard, unshardedCollections, db);
        } else {
            // This db still belongs to the original shard, drop all the unsharded collections fr0m the new
            dropCollectionsFromShard(opCtx, newShard, unshardedCollections, db);
        }
    }
}

class ConfigsvrCommitSplitShardCommand final
    : public TypedCommand<ConfigsvrCommitSplitShardCommand> {
public:
    using Request = ConfigsvrCommitSplitShard;

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            uassert(ErrorCodes::IllegalOperation,
                    "_configsvrCommitSplitShard can only be run on config servers",
                    serverGlobalParams.clusterRole == ClusterRole::ConfigServer);

            uassert(ErrorCodes::CommandNotSupported,
                    "'_configsvrCommitSplitShard' is only supported in feature compatibility version "
                    "4.4",
                    serverGlobalParams.featureCompatibility.getVersion() ==
                        ServerGlobalParams::FeatureCompatibility::Version::kFullyUpgradedTo44);

            repl::ReadConcernArgs::get(opCtx) =
                    repl::ReadConcernArgs(repl::ReadConcernLevel::kLocalReadConcern);

            const auto catalogClient = Grid::get(opCtx)->catalogClient();
            const auto dbsOwnedByShard = uassertStatusOK(catalogClient->getDatabasesForShard(opCtx, ShardId(request().getFromShard().toString())));

                const auto chunksOwnedByShard = uassertStatusOK(catalogClient->getChunks(opCtx, 
                    BSON(ChunkType::shard(request().getFromShard().toString())), BSONObj(), 
                    boost::none, nullptr, repl::ReadConcernLevel::kLocalReadConcern));

                std::vector<std::string> dbsToMoveToNewShard(dbsOwnedByShard.begin() + dbsOwnedByShard.size() / 2, dbsOwnedByShard.end());
                std::vector<ChunkType> chunksToMoveToNewShard(chunksOwnedByShard.begin() + chunksOwnedByShard.size() / 2, chunksOwnedByShard.end());
            
                if (chunksToMoveToNewShard.size() > 0) {
                    auto response = ShardingCatalogManager::get(opCtx)->commitSplitChunksOnShard(
                        opCtx,
                        chunksToMoveToNewShard,
                        request().getFromShard().toString(),
                        request().getNewShard().toString(), 
                        LogicalClock::get(opCtx)->getClusterTime().asTimestamp());
                    uassertStatusOK(response);
                }

                for (const auto& db : dbsToMoveToNewShard) {
                    uassertStatusOK(ShardingCatalogManager::get(opCtx)->commitMovePrimary(
                    opCtx, db, request().getNewShard().toString()));
                }

            if (request().getRemoveOrphans() || request().getDropOrphanedCollections()) {
                const auto newShard = uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, request().getNewShard().toString()));
                const auto fromShard = uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, request().getFromShard().toString()));

                // Get all sharded collections and their uuid to be used when marking chunks as orphans and dropping unsharded collections later
                std::map<NamespaceString, mongo::UUID> nssToUuid;
                for (const auto& db : dbsOwnedByShard) {
                    auto shardedCollections = uassertStatusOK(catalogClient->getCollections(opCtx, &db, nullptr, repl::ReadConcernLevel::kLocalReadConcern));
                    for (const auto& collection : shardedCollections) {
                        nssToUuid.try_emplace(collection.getNs(), collection.getUUID().get());
                    }
                }

                if (request().getRemoveOrphans() && chunksToMoveToNewShard.size() > 0) {
                    markOrphanedChunksForDeletion(opCtx, fromShard, newShard, chunksOwnedByShard, chunksToMoveToNewShard, nssToUuid);
                }

                if (request().getDropOrphanedCollections() && dbsToMoveToNewShard.size() > 0) {
                    dropOrphanedUnshardedCollections(opCtx, fromShard, newShard, dbsOwnedByShard, dbsToMoveToNewShard, nssToUuid);

                }
            }
    }

    private:
        NamespaceString ns() const override {
            return NamespaceString(request().getDbName(), "");
        }

        bool supportsWriteConcern() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                           ActionType::internal));
        }
    };

    std::string help() const override {
        return "Internal command, which is exported by the sharding config server. Do not call "
               "directly. Adds a suffix to the shard key of an existing collection ('refines the "
               "shard key').";
    }

    bool adminOnly() const override {
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
} configsvrCommitSplitShardCommand;

}  // namespace
}  // namespace mongo
