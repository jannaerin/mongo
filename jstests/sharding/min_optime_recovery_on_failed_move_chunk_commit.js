/**
 * Tests that the shard will update the min optime recovery document after startup.
 * @tags: [requires_persistence]
 */
(function() {
    "use strict";

    // TODO: SERVER-33444 remove shardAsReplicaSet: false
    var st = new ShardingTest({shards: 1});

    // Insert a recovery doc with non-zero minOpTimeUpdaters to simulate a migration
    // process that crashed in the middle of the critical section.

    var recoveryDoc = {
        _id: 'minOpTimeRecovery',
        configsvrConnectionString: st.configRS.getURL(),
        shardName: st.shard0.shardName,
        minOpTime: {ts: Timestamp(0, 0), t: 0},
        minOpTimeUpdaters: 2
    };

    assert.writeOK(st.shard0.getDB('admin').system.version.insert(recoveryDoc));

    // Make sure test is setup correctly.
    var minOpTimeRecoveryDoc =
        st.shard0.getDB('admin').system.version.findOne({_id: 'minOpTimeRecovery'});

    assert.neq(null, minOpTimeRecoveryDoc);
    assert.eq(0, minOpTimeRecoveryDoc.minOpTime.ts.getTime());
    assert.eq(2, minOpTimeRecoveryDoc.minOpTimeUpdaters);

    //st.restartMongod(0);

        /*function restartReplSet(replSet, newOpts, user, pwd) {
        const numNodes = replSet.nodeList().length;

        for (let n = 0; n < numNodes; n++) {
            replSet.restart(n, newOpts);
        }

        shardConn = replSet.getPrimary();
        replSet.awaitSecondaryNodes();

        shardAdminDB = shardConn.getDB("admin");

        if (user && pwd) {
            shardAdminDB.auth(user, pwd);
        }
    }*/
    for (let n = 0; n < st.rs0.nodeList().length; n++) {
        st.rs0.restart(n)
    }
    jsTestLog("xx done restarting");
    st.rs0.awaitSecondaryNodes();
    jsTestLog('xxx done awaiting');
    /*var restartedRS0 = st.rs0.restart([0,1]);
    st.rs0 = restartedRS0;
    st._rsObjects[0] = restartedRS0;*/

    // After the restart, the shard should have updated the opTime and reset minOpTimeUpdaters.
    minOpTimeRecoveryDoc = st.rs0.getPrimary().getDB('admin').system.version.findOne({_id: 'minOpTimeRecovery'});

    assert.neq(null, minOpTimeRecoveryDoc);
    assert.gt(minOpTimeRecoveryDoc.minOpTime.ts.getTime(), 0);
    assert.eq(0, minOpTimeRecoveryDoc.minOpTimeUpdaters);

    st.stop();

})();
