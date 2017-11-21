/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands/distinct_request.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {

DistinctRequest::DistinctRequest(NamespaceString nss, DistinctRequestIDL parsedDistinct)
    : _nss(std::move(nss)), _parsedDistinct(parsedDistinct) {}

StatusWith<BSONObj> DistinctRequest::asAggregationCommand(BSONObj cmdObj) const {
    BSONObjBuilder aggregationBuilder;
    aggregationBuilder.append("aggregate", _parsedDistinct.getNamespace().coll());

    BSONArrayBuilder pipelineBuilder(aggregationBuilder.subarrayStart("pipeline"));
    if (_parsedDistinct.getQuery()) {
        BSONObjBuilder matchStageBuilder(pipelineBuilder.subobjStart());
        matchStageBuilder.append("$match", _parsedDistinct.getQuery().value_or(BSONObj()));
        matchStageBuilder.doneFast();
    }
    BSONObjBuilder unwindStageBuilder(pipelineBuilder.subobjStart());
    {
        BSONObjBuilder unwindBuilder(unwindStageBuilder.subobjStart("$unwind"));
        unwindBuilder.append("path", str::stream() << "$" << _parsedDistinct.getKey());
        unwindBuilder.append("preserveNullAndEmptyArrays", true);
        unwindBuilder.doneFast();
    }
    unwindStageBuilder.doneFast();
    BSONObjBuilder groupStageBuilder(pipelineBuilder.subobjStart());
    {
        BSONObjBuilder groupBuilder(groupStageBuilder.subobjStart("$group"));
        groupBuilder.appendNull("_id");
        {
            BSONObjBuilder distinctBuilder(groupBuilder.subobjStart("distinct"));
            distinctBuilder.append("$addToSet", str::stream() << "$" << _parsedDistinct.getKey());
            distinctBuilder.doneFast();
        }
        groupBuilder.doneFast();
    }
    groupStageBuilder.doneFast();
    pipelineBuilder.doneFast();

    if (_parsedDistinct.getCollation()) {
        aggregationBuilder.append("collation", *_parsedDistinct.getCollation());
    }

    if (_parsedDistinct.getComment()) {
        aggregationBuilder.append("comment", *_parsedDistinct.getComment());
    }

    BSONObj aggregationObjPassthrough =
        BasicCommand::appendPassthroughFields(cmdObj, aggregationBuilder.obj());

    BSONObjBuilder aggBuilder;
    aggBuilder.appendElements(aggregationObjPassthrough);

    // The 'cursor' option is always specified so that aggregation uses the cursor interface.
    aggBuilder.append("cursor", BSONObj());

    return aggBuilder.obj();
}

}  // namespace mongo