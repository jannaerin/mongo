/**
 * This test is labeled resource intensive because its total io_write is 95MB compared to a median
 * of 5MB across all sharding tests in wiredTiger. Its total io_write is 1086MB compared to a median
 * of 135MB in mmapv1.
 * @tags: [resource_intensive]
 */
(function() {
    "use strict";

    // TODO: SERVER-33444 remove shardAsReplicaSet: false
    var st = new ShardingTest({
        name: 'migrateBig_balancer',
        shards: 2,
        other: {enableBalancer: true/*, shardAsReplicaSet: false*/}
    });
    var mongos = st.s;
    jsTestLog("xxx migrate big balancer");
    var admin = mongos.getDB("admin");
    var db = mongos.getDB("test");
    var coll = db.getCollection("stuff");

    assert.commandWorked(admin.runCommand({enablesharding: coll.getDB().getName()}));
    st.ensurePrimaryShard(coll.getDB().getName(), st.shard1.shardName);
    jsTestLog("xxxx primary shard");
    var data = "x";
    var nsq = 16;
    var n = 255;

    for (var i = 0; i < nsq; i++)
        data += data;

    var dataObj = {};
    for (var i = 0; i < n; i++)
        dataObj["data-" + i] = data;
    jsTestLog("xxx going to insert");
    var bulk = coll.initializeUnorderedBulkOp();
    for (var i = 0; i < 40; i++) {
        bulk.insert({data: dataObj});
    }
    jsTestLog("xxx execute insert");
    assert.writeOK(bulk.execute());
    jsTestLog("xxx does that ever happen");
    assert.eq(40, coll.count(), "prep1");
    jsTestLog("xxx sharding");
    assert.commandWorked(admin.runCommand({shardcollection: "" + coll, key: {_id: 1}}));
    jsTestLog("xxx kkk thats okay");
    st.printShardingStatus();

    assert.lt(
        5, mongos.getDB("config").chunks.find({ns: "test.stuff"}).count(), "not enough chunks");
    jsTestLog("xxxx lt okay");
    assert.soon(function() {
        jsTestLog("xxx soon func");
        var res = mongos.getDB("config").chunks.group({
            cond: {ns: "test.stuff"},
            key: {shard: 1},
            reduce: function(doc, out) {
                out.nChunks++;
            },
            initial: {nChunks: 0}
        });
        jsTestLog("xxxx idk");
        //printjson(res);
        return res.length > 1 && Math.abs(res[0].nChunks - res[1].nChunks) <= 3;

    }, "never migrated", 10 * 60 * 1000, 1000);
    jsTestLog("xxxx stopping");
    st.stop();
})();
