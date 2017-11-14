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

#include "mongo/db/commands/count_request.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {

CountRequest::CountRequest(NamespaceString nss, CountRequestIDL parsedCount)
    : _nss(std::move(nss)), _parsedCount(parsedCount) {}


StatusWith<BSONObj> CountRequest::asAggregationCommand(BSONObj cmdObj) const {
    BSONObjBuilder aggregationBuilder;
    aggregationBuilder.append("aggregate", _parsedCount.getNamespace().coll());

    // Build an aggregation pipeline that performs the counting. We add stages that satisfy the
    // query, skip and limit before finishing with the actual $count stage.
    BSONArrayBuilder pipelineBuilder(aggregationBuilder.subarrayStart("pipeline"));
    if (_parsedCount.getQuery()) {
        BSONObjBuilder matchBuilder(pipelineBuilder.subobjStart());
        matchBuilder.append("$match", _parsedCount.getQuery().value_or(BSONObj()));
        matchBuilder.doneFast();
    }
    if (_parsedCount.getSkip()) {
        BSONObjBuilder skipBuilder(pipelineBuilder.subobjStart());
        skipBuilder.append("$skip", _parsedCount.getSkip().value_or(0));
        skipBuilder.doneFast();
    }
    if (_parsedCount.getLimit()) {
        BSONObjBuilder limitBuilder(pipelineBuilder.subobjStart());
        limitBuilder.append("$limit", _parsedCount.getLimit().value_or(0));
        limitBuilder.doneFast();
    }

    BSONObjBuilder countBuilder(pipelineBuilder.subobjStart());
    countBuilder.append("$count", "count");
    countBuilder.doneFast();
    pipelineBuilder.doneFast();

    if (_parsedCount.getCollation()) {
        aggregationBuilder.append("collation", *_parsedCount.getCollation());
    }

    if (_parsedCount.getHint()) {
        aggregationBuilder.append("hint", _parsedCount.getHint().value_or(BSONObj()));
    }

    BSONObj aggregationObjPassthrough =
        BasicCommand::appendPassthroughFields(cmdObj, aggregationBuilder.obj());

    BSONObjBuilder aggBuilder;
    aggBuilder.appendElements(aggregationObjPassthrough);

    // The 'cursor' option is always specified so that aggregation uses the cursor interface.
    aggBuilder.append("cursor", BSONObj());

    return aggBuilder.obj();
}

bool CountRequest::validate() {
    if (_parsedCount.getLimit().value_or(0) < 0) {
        _parsedCount.setLimit(_parsedCount.getLimit().value_or(0) * -1);
    }

    if (_parsedCount.getSkip().value_or(0) < 0) {
        return false;
    }

    return true;
}

}  // namespace mongo