/**
 * Tests whether new sharding is detected on insert by mongos
 *
 * This test is labeled resource intensive because its total io_write is 650MB compared to a median
 * of 135MB across all sharding tests in mmapv1.
 * @tags: [resource_intensive]
 */
(function() {
    'use strict';

    var st = new ShardingTest({shards: 10, mongos: 3});

    var mongosA = st.s0;
    var mongosB = st.s1;
    var mongosC = st.s2;

    var admin = mongosA.getDB("admin");
    var config = mongosA.getDB("config");

    var collA = mongosA.getCollection("foo.bar");
    var collB = mongosB.getCollection("" + collA);
    var collC = mongosB.getCollection("" + collA);

    var shards = [
        st.rs0,
        st.rs1,
        st.rs2,
        st.rs3,
        st.rs4,
        st.rs5,
        st.rs6,
        st.rs7,
        st.rs8,
        st.rs9
    ];

    assert.commandWorked(admin.runCommand({enableSharding: "" + collA.getDB()}));
    st.ensurePrimaryShard(collA.getDB().getName(), st.rs1.name);
    assert.commandWorked(admin.runCommand({shardCollection: "" + collA, key: {_id: 1}}));

    jsTestLog("Splitting up the collection...");

    // Split up the collection
    for (var i = 0; i < shards.length; i++) {
        assert.commandWorked(admin.runCommand({split: "" + collA, middle: {_id: i}}));
        assert.commandWorked(
            admin.runCommand({moveChunk: "" + collA, find: {_id: i}, to: shards[i].name}));
    }

    mongosB.getDB("admin").runCommand({flushRouterConfig: 1});
    mongosC.getDB("admin").runCommand({flushRouterConfig: 1});

    printjson(collB.count());
    printjson(collC.count());

    // Change up all the versions...
    for (var i = 0; i < shards.length; i++) {
        assert.commandWorked(admin.runCommand({
            moveChunk: "" + collA,
            find: {_id: i},
            to: shards[(i + 1) % shards.length].name
        }));
    }

    // Make sure mongos A is up-to-date
    mongosA.getDB("admin").runCommand({flushRouterConfig: 1});

    jsTestLog("Running count!");

    printjson(collB.count());
    printjson(collC.find().toArray());

    st.stop();

})();
