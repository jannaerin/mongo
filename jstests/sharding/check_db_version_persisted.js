/**
 * Tests that the db version is persisted.
 */
(function() {
    "use strict";

    const st = new ShardingTest({shards: 2});

    const db = "test";
    const coll = "bar";
    const nss = db + "." + coll;

    assert.commandWorked(st.s.adminCommand({enableSharding: db}));
    st.ensurePrimaryShard(db, st.shard0.shardName);

    assert.commandWorked(st.shard0.getDB("admin").runCommand(
        {configureFailPoint: 'callShardServerCallbackFn', mode: 'alwaysOn'}));

    assert.commandWorked(st.s.adminCommand({shardCollection: nss, key: {_id: 1}}));
    assert.writeOK(st.s.getDB(db).getCollection(coll).insert({x: 1}));

    // Check that the db version is persisted on the shard.
    const cacheDbEntry = st.shard0.getDB("config").cache.databases.findOne({_id: db});
    jsTestLog(tojson(cacheDbEntry));
    assert.neq(undefined, cacheDbEntry);
    assert.neq(undefined, cacheDbEntry.version);
    assert.neq(undefined, cacheDbEntry.version.uuid);
    assert.neq(undefined, cacheDbEntry.version.lastMod);
    assert.neq(undefined, cacheDbEntry.primary);
    assert.neq(undefined, cacheDbEntry.partitioned);

    st.stop();
})();
