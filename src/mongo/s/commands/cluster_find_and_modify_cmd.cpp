
/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand
#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/find_and_modify_common.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/commands/cluster_explain.h"
#include "mongo/s/commands/strategy.h"
#include "mongo/s/grid.h"
#include "mongo/s/multi_statement_transaction_requests_sender.h"
#include "mongo/s/stale_exception.h"
#include "mongo/s/transaction_router.h"
#include "mongo/s/write_ops/cluster_write.h"
#include "mongo/util/timer.h"
#include "mongo/db/logical_session_cache.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/session_catalog.h"
#include "mongo/db/logical_session_id_helpers.h"
#include "mongo/s/would_change_owning_shard_exception.h"
#include "mongo/transport/service_entry_point.h"

namespace mongo {
namespace {

const ReadPreferenceSetting kPrimaryOnlyReadPreference(ReadPreference::PrimaryOnly);

BSONObj getCollation(const BSONObj& cmdObj) {
    BSONElement collationElement;
    auto status = bsonExtractTypedField(cmdObj, "collation", BSONType::Object, &collationElement);
    if (status.isOK()) {
        return collationElement.Obj();
    } else if (status != ErrorCodes::NoSuchKey) {
        uassertStatusOK(status);
    }

    return BSONObj();
}

BSONObj getShardKey(OperationContext* opCtx, const ChunkManager& chunkMgr, const BSONObj& query) {
    BSONObj shardKey =
        uassertStatusOK(chunkMgr.getShardKeyPattern().extractShardKeyFromQuery(opCtx, query));
    uassert(ErrorCodes::ShardKeyNotFound,
            "Query for sharded findAndModify must contain the shard key",
            !shardKey.isEmpty());
    return shardKey;
}

Status runUpdateShardKeyTxn(OperationContext* opCtx,
                            std::vector<BSONObj> cmdObjs,
                            NamespaceString nss,
                            LogicalSessionId lsid,
                            boost::optional<int> stmtId,
                            bool inTxn) {
    auto executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
    if (inTxn) {
        OperationContextSession::checkIn(opCtx);
    }
    for (std::size_t i = 0; i != cmdObjs.size(); ++i) {
        auto cbHandle = uassertStatusOK(
            executor->scheduleWork([ nss, cmdObj = cmdObjs[i], lsid, i, stmtId, inTxn ](
                const executor::TaskExecutor::CallbackArgs& args) {
                log() << "xxx top of lambda";
                ThreadClient threadClient("UpdateShardKey", getGlobalServiceContext());
                auto opCtxHolder = Client::getCurrent()->makeOperationContext();
                auto opCtx = opCtxHolder.get();

                BSONObjBuilder cmdObjWithLsidBuilder(cmdObj);
                cmdObjWithLsidBuilder.append("lsid", lsid.toBSON());
                if (stmtId) {
                    cmdObjWithLsidBuilder.append("stmtId", stmtId.get());
                } else {
                    cmdObjWithLsidBuilder.append("stmtId", static_cast<int>(i));
                }
                if (!inTxn && (cmdObj["delete"])) {
                    cmdObjWithLsidBuilder.append("startTransaction", true);
                }

                auto b = cmdObjWithLsidBuilder.obj();

                auto db = nss.db();
                if (cmdObj["commitTransaction"]) {
                    db = NamespaceString::kAdminDb;
                }
                auto requestOpMsg = OpMsgRequest::fromDBAndBody(db, b).serialize();

                const auto replyMsg = OpMsg::parseOwned(opCtx->getServiceContext()
                                                            ->getServiceEntryPoint()
                                                            ->handleRequest(opCtx, requestOpMsg)
                                                            .response);

                invariant(replyMsg.sequences.empty());

                return replyMsg.body;
            }));

        executor->wait(cbHandle, opCtx);
    }

    return Status::OK();
}

Status updateShardKey(OperationContext* opCtx,
                      NamespaceString nss,
                      WouldChangeOwningShardInfo errInfo,
                      TxnNumber txnNumber,
                      LogicalSessionId lsid,
                      boost::optional<int> stmtId,
                      bool inTxn) {

    write_ops::Delete deleteOp(nss);
    deleteOp.setDeletes({[&] {
        write_ops::DeleteOpEntry entry;
        entry.setQ(errInfo.getOriginalShardKey());
        entry.setMulti(false);
        return entry;
    }()});

    write_ops::Insert insertOp(nss);
    insertOp.setDocuments({errInfo.getPostImg()});

    std::vector<BSONObj> cmdsToRunInTxn;

    if (inTxn) {
        auto deleteCmdObj =
            deleteOp.toBSON(BSON("txnNumber" << txnNumber << "autocommit" << false));

        auto insertCmdObj =
            insertOp.toBSON(BSON("txnNumber" << txnNumber << "autocommit" << false));


        cmdsToRunInTxn.emplace_back(deleteCmdObj);
        cmdsToRunInTxn.emplace_back(insertCmdObj);
        auto updateStatus = runUpdateShardKeyTxn(opCtx, cmdsToRunInTxn, nss, lsid, stmtId, inTxn);
        uassertStatusOK(updateStatus);
        return updateStatus;
    } else {

        auto deleteCmdObj =
            deleteOp.toBSON(BSON("txnNumber" << txnNumber << "autocommit" << false << "readConcern"
                                             << BSON("level"
                                                     << "snapshot")));

        auto insertCmdObj =
            insertOp.toBSON(BSON("txnNumber" << txnNumber << "autocommit" << false));

        auto commitCmdObj =
            BSON("commitTransaction" << 1 << "txnNumber" << txnNumber << "autocommit" << false);

        cmdsToRunInTxn.emplace_back(deleteCmdObj);
        cmdsToRunInTxn.emplace_back(insertCmdObj);
        cmdsToRunInTxn.emplace_back(commitCmdObj);
        auto updateStatus = runUpdateShardKeyTxn(opCtx, cmdsToRunInTxn, nss, lsid, stmtId, inTxn);

        return updateStatus;
    }
}

class FindAndModifyCmd : public BasicCommand {
public:
    FindAndModifyCmd() : BasicCommand("findAndModify", "findandmodify") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return false;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    bool supportsReadConcern(const std::string& dbName,
                             const BSONObj& cmdObj,
                             repl::ReadConcernLevel level) const final {
        return true;
    }

    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) const override {
        find_and_modify::addPrivilegesRequiredForFindAndModify(this, dbname, cmdObj, out);
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& request,
                   ExplainOptions::Verbosity verbosity,
                   rpc::ReplyBuilderInterface* result) const override {
        std::string dbName = request.getDatabase().toString();
        const BSONObj& cmdObj = request.body;
        const NamespaceString nss(CommandHelpers::parseNsCollectionRequired(dbName, cmdObj));

        auto routingInfo =
            uassertStatusOK(Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfo(opCtx, nss));

        std::shared_ptr<ChunkManager> chunkMgr;
        std::shared_ptr<Shard> shard;

        if (!routingInfo.cm()) {
            shard = routingInfo.db().primary();
        } else {
            chunkMgr = routingInfo.cm();

            const BSONObj query = cmdObj.getObjectField("query");
            const BSONObj collation = getCollation(cmdObj);
            const BSONObj shardKey = getShardKey(opCtx, *chunkMgr, query);
            const auto chunk = chunkMgr->findIntersectingChunk(shardKey, collation);

            shard = uassertStatusOK(
                Grid::get(opCtx)->shardRegistry()->getShard(opCtx, chunk.getShardId()));
        }

        const auto explainCmd = ClusterExplain::wrapAsExplain(cmdObj, verbosity);

        // Time how long it takes to run the explain command on the shard.
        Timer timer;
        BSONObjBuilder bob;
        _runCommand(opCtx,
                    shard->getId(),
                    (chunkMgr ? chunkMgr->getVersion(shard->getId()) : ChunkVersion::UNSHARDED()),
                    nss,
                    explainCmd,
                    &bob);
        const auto millisElapsed = timer.millis();

        Strategy::CommandResult cmdResult;
        cmdResult.shardTargetId = shard->getId();
        cmdResult.target = shard->getConnString();
        cmdResult.result = bob.obj();

        std::vector<Strategy::CommandResult> shardResults;
        shardResults.push_back(cmdResult);

        auto bodyBuilder = result->getBodyBuilder();
        return ClusterExplain::buildExplainResult(
            opCtx, shardResults, ClusterExplain::kSingleShard, millisElapsed, &bodyBuilder);
    }

    bool run(OperationContext* opCtx,
             const std::string& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        const NamespaceString nss(CommandHelpers::parseNsCollectionRequired(dbName, cmdObj));

        // findAndModify should only be creating database if upsert is true, but this would require
        // that the parsing be pulled into this function.
        uassertStatusOK(createShardDatabase(opCtx, nss.db()));

        const auto routingInfo = uassertStatusOK(getCollectionRoutingInfoForTxnCmd(opCtx, nss));
        if (!routingInfo.cm()) {
            _runCommand(opCtx,
                        routingInfo.db().primaryId(),
                        ChunkVersion::UNSHARDED(),
                        nss,
                        cmdObj,
                        &result);
            return true;
        }

        const auto chunkMgr = routingInfo.cm();

        const BSONObj query = cmdObj.getObjectField("query");
        const BSONObj collation = getCollation(cmdObj);
        const BSONObj shardKey = getShardKey(opCtx, *chunkMgr, query);
        auto chunk = chunkMgr->findIntersectingChunk(shardKey, collation);

        _runCommand(opCtx,
                    chunk.getShardId(),
                    chunkMgr->getVersion(chunk.getShardId()),
                    nss,
                    cmdObj,
                    &result);

        return true;
    }

private:
    static void _runCommand(OperationContext* opCtx,
                            const ShardId& shardId,
                            const ChunkVersion& shardVersion,
                            const NamespaceString& nss,
                            const BSONObj& cmdObj,
                            BSONObjBuilder* result) {
        const auto response = [&] {
            std::vector<AsyncRequestsSender::Request> requests;
            requests.emplace_back(
                shardId,
                appendShardVersion(CommandHelpers::filterCommandRequestForPassthrough(cmdObj),
                                   shardVersion));
            log() << "xxx impl find and mod mongos";
            bool isRetryableWrite = opCtx->getTxnNumber() && !TransactionRouter::get(opCtx);

            MultiStatementTransactionRequestsSender ars(
                opCtx,
                Grid::get(opCtx)->getExecutorPool()->getArbitraryExecutor(),
                nss.db().toString(),
                requests,
                kPrimaryOnlyReadPreference,
                isRetryableWrite ? Shard::RetryPolicy::kIdempotent : Shard::RetryPolicy::kNoRetry);

            auto response = ars.next();
            invariant(ars.done());

            return uassertStatusOK(std::move(response.swResponse));
        }();

        uassertStatusOK(response.status);

        const auto responseStatus = getStatusFromCommandResult(response.data);
        if (ErrorCodes::isNeedRetargettingError(responseStatus.code()) ||
            ErrorCodes::isSnapshotError(responseStatus.code())) {
            // Command code traps this exception and re-runs
            uassertStatusOK(responseStatus.withContext("findAndModify"));
        }

        if (responseStatus.code() == ErrorCodes::WouldChangeOwningShard) {
            log() << "xxx got would change status in cluster find and mod";
            BSONObjBuilder extraInfoBuilder;
            responseStatus.extraInfo()->serialize(&extraInfoBuilder);
            auto extraInfo = extraInfoBuilder.obj();
            auto wouldChangeInfo =
                WouldChangeOwningShardInfo::parseFromCommandError(extraInfo);
            log() << "xxx response data " << response.data;
            Status txnStatus = Status::OK();
            if (TransactionRouter::get(opCtx)) {
                boost::optional<int> stmtIdToSend;
                /*if (request["stmtIds"]) {
                    stmtIdToSend = request["stmtIds"][0];
                    log() << "xxx send " << stmtIdToSend.get();
                }*/

                txnStatus = updateShardKey(opCtx,
                                               nss,
                                               wouldChangeInfo,
                                               opCtx->getTxnNumber().get(),
                                               opCtx->getLogicalSessionId().get(),
                                               boost::none,
                                               true);
                uassertStatusOK(txnStatus);
            } else if (opCtx->getTxnNumber() && !TransactionRouter::get(opCtx)) {
                // retryable write
                auto lsid = opCtx->getLogicalSessionId().get();
                auto txnNumber = opCtx->getTxnNumber().get();

                txnStatus = updateShardKey(opCtx,
                                           nss,
                                           wouldChangeInfo,
                                           txnNumber,
                                           lsid,
                                           boost::none,
                                           false);

                uassertStatusOK(txnStatus);
            }
            // do something to return correctly here
            BSONObjBuilder responseBuilder;
            responseBuilder.append("value", wouldChangeInfo.getPostImg());
            BSONObjBuilder subBuilder (responseBuilder.subobjStart("lastErrorObject"));
            subBuilder.appendBool("n", 1);
            subBuilder.appendBool("updatedExisting", true);
            subBuilder.done();

            result->appendElementsUnique(
                CommandHelpers::filterCommandReplyForPassthrough(responseBuilder.obj()));
        } else {

            // First append the properly constructed writeConcernError. It will then be skipped in
            // appendElementsUnique.
            if (auto wcErrorElem = response.data["writeConcernError"]) {
                appendWriteConcernErrorToCmdResponse(shardId, wcErrorElem, *result);
            }
            
            result->appendElementsUnique(
                CommandHelpers::filterCommandReplyForPassthrough(response.data));
        }
    }

} findAndModifyCmd;

}  // namespace
}  // namespace mongo
