/**
 *    Copyright (C) 2018 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/type_shard_database.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/util/assert_util.h"

namespace mongo {

using std::string;

const NamespaceString ShardDatabaseType::ConfigNS(
    NamespaceString::kShardConfigDatabasesCollectionName);

const BSONField<std::string> ShardDatabaseType::name("_id");
const BSONField<DatabaseVersion> ShardDatabaseType::version("version");
const BSONField<int> ShardDatabaseType::enterCriticalSectionCounter("enterCriticalSectionCounter");

ShardDatabaseType::ShardDatabaseType(const std::string& dbName,
                                     boost::optional<DatabaseVersion> version)
    : _name(dbName), _version(version) {}

StatusWith<ShardDatabaseType> ShardDatabaseType::fromBSON(const BSONObj& source) {
    std::string dbName;
    {
        Status status = bsonExtractStringField(source, name.name(), &dbName);
        if (!status.isOK())
            return status;
    }

    boost::optional<DatabaseVersion> dbVersion = boost::none;
    {
        BSONObj versionField = source.getObjectField("version");
        // TODO: Parse this unconditionally once featureCompatibilityVersion 3.6 is no longer
        // supported.
        if (!versionField.isEmpty()) {
            dbVersion = DatabaseVersion::parse(IDLParserErrorContext("DatabaseType"), versionField);
        }
    }

    ShardDatabaseType shardDatabaseType(dbName, dbVersion);


    return shardDatabaseType;
}

BSONObj ShardDatabaseType::toBSON() const {
    BSONObjBuilder builder;

    builder.append(name.name(), _name);
    if (_version) {
        builder.append(version.name(), _version->toBSON());
    }


    return builder.obj();
}

std::string ShardDatabaseType::toString() const {
    return toBSON().toString();
}

void ShardDatabaseType::setDbVersion(DatabaseVersion version) {
    _version = version;
}

void ShardDatabaseType::setDbName(const std::string& dbName) {
    invariant(!dbName.empty());
    _name = dbName;
}

}  // namespace mongo
