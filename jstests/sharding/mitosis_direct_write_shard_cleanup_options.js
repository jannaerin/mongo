(function() {
'use strict';

var dbpathPrefix = MongoRunner.dataPath + 'mitosis';
resetDbpath(dbpathPrefix);
var dbpathFormat = dbpathPrefix + '/mongod-$port';

jsTestLog("Starting up cluster with one shard (3-node rs) and creating collections.\n");
var st = new ShardingTest({shards: 1, rs: {nodes: 3}, other: {rsOptions: {dbpath: dbpathFormat}}});
st.rs0.awaitNodesAgreeOnPrimary();
var primary = st.rs0.getPrimary();
var secondary = st.rs0.getSecondary();
let configDB = st.s.getDB("config");

// Create 5 databases each with one collection each. Three will be unsharded collections, one will
// be a sharded collection with 2 chunks, and one will be a sharded collection with 5 chunks.
assert.commandWorked(st.s.getDB("test1").coll1.insert({x: 1}));
assert.commandWorked(st.s.getDB("test2").coll2.insert({x: 1}));
assert.commandWorked(st.s.getDB("test3").coll3.insert({x: 1}));

assert.commandWorked(st.s.adminCommand({enableSharding: "test4"}));
assert.commandWorked(st.s.adminCommand({shardCollection: "test4.coll4", key: {_id: 1}}));
assert.commandWorked(st.s.getDB("test4").coll4.insert({_id: 1}));
assert.commandWorked(st.s.getDB("test4").coll4.insert({_id: 5}));
assert.commandWorked(configDB.adminCommand({split: "test4.coll4", middle: {_id: 2}}));

assert.commandWorked(st.s.adminCommand({enableSharding: "test5"}));
assert.commandWorked(st.s.adminCommand({shardCollection: "test5.coll5", key: {_id: 1}}));
assert.commandWorked(st.s.getDB("test5").coll5.insert({_id: 1}));
assert.commandWorked(st.s.getDB("test5").coll5.insert({_id: 5}));
assert.commandWorked(st.s.getDB("test5").coll5.insert({_id: 10}));
assert.commandWorked(st.s.getDB("test5").coll5.insert({_id: 20}));
assert.commandWorked(configDB.adminCommand({split: "test5.coll5", middle: {_id: 0}}));
assert.commandWorked(configDB.adminCommand({split: "test5.coll5", middle: {_id: 3}}));
assert.commandWorked(configDB.adminCommand({split: "test5.coll5", middle: {_id: 7}}));
assert.commandWorked(configDB.adminCommand({split: "test5.coll5", middle: {_id: 15}}));
st.rs0.awaitReplication();

// Make sure shard0 knows that it owns all of these collections and databases.
assert.commandWorked(st.shard0.adminCommand({_flushRoutingTableCacheUpdates: "test4.coll4"}));
assert.commandWorked(st.shard0.adminCommand({_flushRoutingTableCacheUpdates: "test5.coll5"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test1"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test2"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test3"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test4"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test5"}));

// Print count of each collection shard0 has (doing direct count against the shard)
jsTestLog("Shard0 has the following number of docs for each collection:\n" +
          "test1.coll1: " + st.rs0.getPrimary().getDB("test1").coll1.find().itcount() + "\n" +
          "test2.coll2: " + st.rs0.getPrimary().getDB("test2").coll2.find().itcount() + "\n" +
          "test3.coll3: " + st.rs0.getPrimary().getDB("test3").coll3.find().itcount() + "\n" +
          "test4.coll4: " + st.rs0.getPrimary().getDB("test4").coll4.find().itcount() + "\n" +
          "test5.coll5: " + st.rs0.getPrimary().getDB("test5").coll5.find().itcount() + "\n");

// Print the catalog for both the config server and the shard
jsTestLog("Config server's config.databases: " +
          tojson(st.configRS.getPrimary().getDB("config").databases.find().toArray()) + "\n");
jsTestLog("Config server's config.chunks: " +
          tojson(st.configRS.getPrimary().getDB("config").chunks.find().toArray()) + "\n");
jsTestLog("Shard0's config.cache.databases: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.databases.find().toArray()) + "\n");
jsTestLog("Shard0's config.cache.chunks.test4.coll4: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.chunks.test4.coll4.find().toArray()) +
          "\n");
jsTestLog("Shard0's config.cache.chunks.test5.coll5: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.chunks.test5.coll5.find().toArray()) +
          "\n");

// COPY DATA INTO 3 NEW PATHS

var dbpathSecondary = secondary.dbpath;
var copiedDbPath1 = dbpathPrefix + '/mongod-copy1';
var copiedDbPath2 = dbpathPrefix + '/mongod-copy2';
var copiedDbPath3 = dbpathPrefix + '/mongod-copy3';
let dbPaths = [copiedDbPath1, copiedDbPath2, copiedDbPath3];

st.rs0.stop(secondary.nodeId);

let numRsNodes = 3;
const restoredNodePorts = allocatePorts(numRsNodes);

jsTestLog(
    "Copying data files from secondary node into 3 new db paths. Updating shardIdentity doc and dropping local db.\n");
for (let i = 0; i < numRsNodes; i++) {
    resetDbpath(dbPaths[i]);
    copyDbpath(dbpathSecondary, dbPaths[i]);
    removeFile(dbPaths[i] + '/mongod.lock');
    var copiedFiles = ls(dbPaths[i]);
    assert.gt(copiedFiles.length, 0, ' no files copied');

    // Start standalone
    let conn = MongoRunner.runMongod({dbpath: dbPaths[i], noCleanData: true});

    // Manipulate local db etc.
    conn.getDB("local").dropDatabase();
    let localDb = conn.getDB("local");
    let adminDb = conn.getDB("admin");
    assert.commandWorked(
        adminDb.system.version.update({_id: "shardIdentity"}, {$set: {shardName: "newShard"}}));

    MongoRunner.stopMongod(conn, {noCleanData: true});
}
st.rs0.start(secondary.nodeId, {}, true);
st.rs0.waitForState(st.rs0.getSecondaries(), ReplSetTest.State.SECONDARY);

jsTestLog("Inserting document for new shard into config.shards.\n");
let configDb = st.configRS.getPrimary().getDB("config");
assert.commandWorked(configDb.shards.insert({
    "_id": "newShard",
    "host": "newShard/janna-HP-Z420-Workstation:" + restoredNodePorts[0] +
        ",janna-HP-Z420-Workstation:" + restoredNodePorts[1] +
        ",janna-HP-Z420-Workstation:" + restoredNodePorts[2],
    "state": 1
}));
jsTestLog("Config.shards: " + tojson(configDb.shards.find().toArray()) + "\n");

jsTestLog(
    "Starting new replica set 'newShard' using the db paths we just copied files into and host/ports added to config.shards. The original shard should still own all of the data.\n");
let newShard = new ReplSetTest({
    name: "newShard",
    nodes: [
        {noCleanData: true, dbpath: copiedDbPath1, port: restoredNodePorts[0]},
        {noCleanData: true, dbpath: copiedDbPath2, port: restoredNodePorts[1]},
        {noCleanData: true, dbpath: copiedDbPath3, port: restoredNodePorts[2]}
    ]
});
newShard.ports = restoredNodePorts;
var nodes = newShard.startSet({shardsvr: ''});
newShard.initiate();
newShard.awaitNodesAgreeOnPrimary();

jsTestLog("newShard has the following number of docs for each collection:\n" +
          "test1.coll1: " + newShard.getPrimary().getDB("test1").coll1.find().itcount() + "\n" +
          "test2.coll2: " + newShard.getPrimary().getDB("test2").coll2.find().itcount() + "\n" +
          "test3.coll3: " + newShard.getPrimary().getDB("test3").coll3.find().itcount() + "\n" +
          "test4.coll4: " + newShard.getPrimary().getDB("test4").coll4.find().itcount() + "\n" +
          "test5.coll5: " + newShard.getPrimary().getDB("test5").coll5.find().itcount() + "\n");

// Check that the first shard owns chunks and databases
assert.eq(
    st.configRS.getPrimary().getDB("config").chunks.find({"ns": "test4.coll4"}).toArray()[0].shard,
    "mitosis_direct_write_shard_cleanup_options-rs0");
assert.eq(st.rs0.getPrimary().getDB("config").cache.chunks.test4.coll4.find().toArray()[0].shard,
          "mitosis_direct_write_shard_cleanup_options-rs0");
assert.eq(2, st.rs0.getPrimary().getDB("config").cache.chunks.test4.coll4.find().toArray().length);

assert.eq(
    st.configRS.getPrimary().getDB("config").chunks.find({"ns": "test5.coll5"}).toArray()[0].shard,
    "mitosis_direct_write_shard_cleanup_options-rs0");
assert.eq(5, st.rs0.getPrimary().getDB("config").cache.chunks.test5.coll5.find().toArray().length);
assert.eq(st.rs0.getPrimary().getDB("config").cache.chunks.test5.coll5.find().toArray()[0].shard,
          "mitosis_direct_write_shard_cleanup_options-rs0");

assert.eq(st.configRS.getPrimary()
              .getDB("config")
              .databases.find({primary: "mitosis_direct_write_shard_cleanup_options-rs0"})
              .toArray()
              .length,
          5);
let numChunksOnOrigShard = st.configRS.getPrimary()
                               .getDB("config")
                               .chunks.find({"shard": "mitosis_direct_write_shard_cleanup_options-rs0"})
                               .toArray()
                               .length;

// send splitShard and the flush the cache
jsTestLog("Running splitShard command\n");
assert.commandWorked(st.s.adminCommand(
    {splitShard: 1, fromShard: "mitosis_direct_write_shard_cleanup_options-rs0", newShard: "newShard", removeOrphans: true, dropOrphanedCollections: true}));

assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test1"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test2"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test3"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test4"}));
assert.commandWorked(st.shard0.adminCommand({_flushDatabaseCacheUpdates: "test5"}));

assert.commandWorked(
    st.shard0.adminCommand({_flushRoutingTableCacheUpdates: "test4.coll4", syncFromConfig: true}));
assert.commandWorked(
    st.shard0.adminCommand({_flushRoutingTableCacheUpdates: "test5.coll5", syncFromConfig: true}));

// Check that the new shard nows owns some databases/chunks
assert.gte(
    st.configRS.getPrimary().getDB("config").chunks.find({"shard": "newShard"}).toArray().length,
    numChunksOnOrigShard / 2);

// Now print catalog on config and both shards
jsTestLog("Config server's config.databases: " +
          tojson(st.configRS.getPrimary().getDB("config").databases.find().toArray()) + "\n");
jsTestLog("Config server's config.chunks: " +
          tojson(st.configRS.getPrimary().getDB("config").chunks.find().toArray()) + "\n");
jsTestLog("Shard0's config.cache.databases: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.databases.find().toArray()) + "\n");
jsTestLog("Shard0's config.cache.chunks.test4.coll4: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.chunks.test4.coll4.find().toArray()) +
          "\n");
jsTestLog("Shard0's config.cache.chunks.test5.coll5: " +
          tojson(st.rs0.getPrimary().getDB("config").cache.chunks.test5.coll5.find().toArray()) +
          "\n");
jsTestLog("newShard's config.cache.databases: " +
          tojson(newShard.getPrimary().getDB("config").cache.databases.find().toArray()) + "\n");
jsTestLog("newShard's config.cache.chunks.test4.coll4: " +
          tojson(newShard.getPrimary().getDB("config").cache.chunks.test4.coll4.find().toArray()) +
          "\n");
jsTestLog("newShard's config.cache.chunks.test5.coll5: " +
          tojson(newShard.getPrimary().getDB("config").cache.chunks.test5.coll5.find().toArray()) +
          "\n");

jsTestLog("newShard has the following number of docs for each collection:\n" +
          "test1.coll1: " + newShard.getPrimary().getDB("test1").coll1.find().itcount() + "\n" +
          "test2.coll2: " + newShard.getPrimary().getDB("test2").coll2.find().itcount() + "\n" +
          "test3.coll3: " + newShard.getPrimary().getDB("test3").coll3.find().itcount() + "\n" +
          "test4.coll4: " + newShard.getPrimary().getDB("test4").coll4.find().itcount() + "\n" +
          "test5.coll5: " + newShard.getPrimary().getDB("test5").coll5.find().itcount() + "\n");
jsTestLog("origShard has the following number of docs for each collection:\n" +
          "test1.coll1: " + st.rs0.getPrimary().getDB("test1").coll1.find().itcount() + "\n" +
          "test2.coll2: " + st.rs0.getPrimary().getDB("test2").coll2.find().itcount() + "\n" +
          "test3.coll3: " + st.rs0.getPrimary().getDB("test3").coll3.find().itcount() + "\n" +
          "test4.coll4: " + st.rs0.getPrimary().getDB("test4").coll4.find().itcount() + "\n" +
          "test5.coll5: " + st.rs0.getPrimary().getDB("test5").coll5.find().itcount() + "\n");

// Print query plans
jsTestLog("The following shard is targeted for each of the following queries:\n" +
          "test1.coll1.find({_id: 1}) " +
          st.s.getDB("test1").coll1.find({_id: 1}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test2.coll2.find({_id: 1}) " +
          st.s.getDB("test2").coll2.find({_id: 1}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test3.coll3.find({_id: 1}) " +
          st.s.getDB("test3").coll3.find({_id: 1}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test4.coll4.find({_id: 1}) " +
          st.s.getDB("test4").coll4.find({_id: 1}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test4.coll4.find({_id: 5}) " +
          st.s.getDB("test4").coll4.find({_id: 5}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test5.coll5.find({_id: -1}) " +
          st.s.getDB("test5").coll5.find({_id: -1}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n" +
          "test5.coll5.find({_id: 3}) " +
          st.s.getDB("test5").coll5.find({_id: 3}).explain().queryPlanner.winningPlan.shards
              [0].shardName +
          "\n");

st.stop();
newShard.stopSet();
})();
