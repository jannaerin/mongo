

(function() {
    'use strict';

    var st = new ShardingTest({mongos: 1, shards: 2});
    var kDbName = 'db';
    var mongos = st.s0;
    var shard0 = st.shard0.shardName;
    var shard1 = st.shard1.shardName;

    assert.commandWorked(mongos.adminCommand({enableSharding: kDbName}));
    st.ensurePrimaryShard(kDbName, shard0);

    function shardCollectionMoveChunks() {
        assert.commandWorked(mongos.getDB(kDbName).foo.createIndex({"x" : 1}));

        var ns = kDbName + '.foo';
        assert.eq(mongos.getDB('config').collections.count({_id: ns, dropped: false}), 0);

        assert.writeOK(mongos.getDB(kDbName).foo.insert({"x" : 4, "a" : 3}));
        assert.writeOK(mongos.getDB(kDbName).foo.insert({"x" : 100}));
        assert.writeOK(mongos.getDB(kDbName).foo.insert({"x" : 300, "a":3}));
        assert.writeOK(mongos.getDB(kDbName).foo.insert({"x" : 500, "a" : 6}));

        assert.commandWorked(mongos.adminCommand({shardCollection: ns, key: {"x" : 1}}));

        assert.commandWorked(mongos.adminCommand({split: ns, find: {x: 100}}));
        jsTestLog(tojson(mongos.getDB('config').chunks.find({ns: ns}).toArray()));
        assert.commandWorked(mongos.adminCommand({moveChunk: ns, find: {"x" : 300}, to: shard1}));
    }

    function cleanupOrphanedDocs(ns) {
        var nextKey = { };
        var result;

        while ( nextKey != null ) {
          result = st.rs0.getPrimary().adminCommand( { cleanupOrphaned: ns, startingFromKey: nextKey } );

          if (result.ok != 1)
             print("Unable to complete at this time: failure or timeout.");

          printjson(result);

          nextKey = result.stoppedAtKey;
        }
    }

    jsTestLog("MULTI STMT TXN");

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    let session = st.s.startSession();
    let sessionDB = session.getDatabase(kDbName);

    // ---------------------
    // WON'T CHANGE SHARDS
    // ---------------------

    // modify
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"$set" : {"x": 600, "a" : 9}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 30}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 30}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // replacement

    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"x": 600}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    mongos.getDB(kDbName).foo.drop();


    // find and modify
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    session.startTransaction();
    sessionDB.foo.findAndModify({query : {"x" : 300}, update : {$set : {"x": 600}}});
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    session.startTransaction();
    sessionDB.foo.findAndModify({query : {"x" : 4}, update : {$set : {"x": 30}}});
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 30}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    // ---------------------
    // CHANGE SHARDS
    // ---------------------


    // single udpates
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});
    jsTestLog("xxx look here:");
    // the shard we move the doc to has not seen _id
    let id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 510}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(510, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    // the shard we move doc to has seen _id

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$set" : {"x": 20}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    mongos.getDB(kDbName).foo.drop();

    // find and modify
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;

    session.startTransaction();
    sessionDB.foo.findAndModify({query : {"x" : 4}, update : {$set : {"x": 510}}});
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(510, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    session.startTransaction();
    sessionDB.foo.findAndModify({query : {"x" : 500}, update : {$set : {"x": 20}}});
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    mongos.getDB(kDbName).foo.drop();

    // -------------------
    // MULTIPLE UPDATES
    // -------------------

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 300}).toArray()[0]._id;

    // update same document twice -- move to shard 0 and then change to new value but remain on shard 0
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"$set" : {"x": 25}}));
    assert.writeOK(sessionDB.foo.update({"x" : 25}, {"$set" : {"x": 15}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 25}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 15}).toArray().length);
    assert.eq(15, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;
    
    // update same document twice -- move to shard 1 then back to shard 0
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 510}}));
    assert.writeOK(sessionDB.foo.update({"x" : 510}, {"$set" : {"x": 50}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 50}).toArray().length);
    assert.eq(50, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;
    let id2 = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;

    // move one doc shard 0 --> shard 1, another shard 1 --> shard 0, one doesn't move
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$set" : {"x": 20}}));
    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 310}}));
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"$set" : {"x": 600}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 310}).toArray().length);
    assert.eq(310, mongos.getDB(kDbName).foo.find({"_id": id2}).toArray()[0].x);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    // move doc shard 1 --> shard0 + $inc other fields
    session.startTransaction();
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$set" : {"x": 20}}));
    assert.writeOK(sessionDB.foo.insert({"x": 1, "a": 1}));
    assert.writeOK(sessionDB.foo.update({"x" : 20}, {"$inc" : {"a": 1}}));
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$inc" : {"a": 1}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"a": 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20, "a" : 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 1}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    // insert and $inc before moving doc
    session.startTransaction();
    assert.writeOK(sessionDB.foo.insert({"x": 1, "a": 1}));
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$inc" : {"a": 1}}));
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$set" : {"x": 20}}));
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"a": 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20, "a" : 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 1}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    // insert and $inc before moving doc
    session.startTransaction();
    assert.writeOK(sessionDB.foo.insert({"x": 1, "a": 1}));
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$inc" : {"a": 1}}));
    sessionDB.foo.findAndModify({query : {"x" : 500}, update : {$set : {"x": 20}}});
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"a": 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20, "a" : 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 1}).toArray().length);

    mongos.getDB(kDbName).foo.drop();


    // bulk write

    /*shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

jsTestLog("xxx bulk 0");
    session.startTransaction();
    let bulkOpp = sessionDB.foo.initializeOrderedBulkOp();
    bulkOpp.find({"x" : 300}).upsert().updateOne({"$set" : {"w": 25}});
    bulkOpp.find({"x" : 25}).upsert().updateOne({"$set" : {"w": 15}});
    assert.commandWorked(bulkOpp.execute());
    session.commitTransaction();

    id = mongos.getDB(kDbName).foo.find({"x": 300}).toArray()[0]._id;

    // update same document twice -- move to shard 0 and then change to new value but remain on shard 0
    jsTestLog("xxx bulk 1");
    session.startTransaction();
    let bulkOp = sessionDB.foo.initializeUnorderedBulkOp();
    bulkOp.find({"x" : 300}).upsert().updateOne({"$set" : {"x": 25}});
    bulkOp.find({"x" : 500}).upsert().updateOne({"$set" : {"x": 20}});
    //bulkOp.find({"x" : 25}).upsert().updateOne({"$set" : {"x": 15}});
    assert.commandWorked(bulkOp.execute());
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 25}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 15}).toArray().length);
    assert.eq(15, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;
    
    // update same document twice -- move to shard 1 then back to shard 0
    jsTestLog("xxx bulk 2");
    session.startTransaction();
    bulkOp = sessionDB.foo.initializeOrderedBulkOp();
    bulkOp.find({"x" : 4}).upsert().updateOne({"$set" : {"x": 510}});
    bulkOp.find({"x" : 510}).upsert().updateOne({"$set" : {"x": 50}});
    assert.commandWorked(bulkOp.execute());
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 50}).toArray().length);
    assert.eq(50, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;
    id2 = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;

    // move one doc shard 0 --> shard 1, another shard 1 --> shard 0, one doesn't move
    jsTestLog("xxx bulk 3");
    session.startTransaction();
    bulkOp = sessionDB.foo.initializeOrderedBulkOp();
    bulkOp.find({"x" : 500}).upsert().updateOne({"$set" : {"x": 20}});
    bulkOp.find({"x" : 4}).upsert().updateOne({"$set" : {"x": 310}});
    bulkOp.find({"x" : 300}).upsert().updateOne({"$set" : {"x": 600}});
    assert.commandWorked(bulkOp.execute());
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 310}).toArray().length);
    assert.eq(310, mongos.getDB(kDbName).foo.find({"_id": id2}).toArray()[0].x);
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // read so don't get ssv causing shard to abort txn
    mongos.getDB(kDbName).foo.insert({"x": 505});

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    // move doc shard 1 --> shard0 + $inc other fields
    jsTestLog("xxx bulk 4");
    session.startTransaction();
    bulkOp = sessionDB.foo.initializeOrderedBulkOp();
    bulkOp.find({"x" : 500}).upsert().updateOne({"$set" : {"x": 20}});
    bulkOp.find({"x" : 20}).upsert().updateOne({"$inc" : {"a": 1}});
    bulkOp.find({"x" : 500}).upsert().updateOne({"$inc" : {"a": 1}});
    bulkOp.execute();
    session.commitTransaction();

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"a": 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20, "a" : 7}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 1}).toArray().length);

    mongos.getDB(kDbName).foo.drop();*/




    jsTestLog("RETRYABLE WRITES");

    session = st.s.startSession({retryWrites: true});
    sessionDB = session.getDatabase(kDbName);

    // ---------------------
    // WON'T CHANGE SHARDS
    // ---------------------

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // modify
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"$set" : {"x": 600}}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 30}}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 30}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    // replacement
    assert.writeOK(sessionDB.foo.update({"x" : 300}, {"x": 600}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"x": 30}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 30}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    // find and modify
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    sessionDB.foo.findAndModify({query : {"x" : 300}, update : {$set : {"x": 600}}});

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 300}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 600}).toArray().length);

    sessionDB.foo.findAndModify({query : {"x" : 4}, update : {$set : {"x": 30}}});

    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 30}).toArray().length);

    mongos.getDB(kDbName).foo.drop();

    // ----------------------
    // CHANGE SHARDS
    // ----------------------

    // modify
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    //make sure won't get ssv
    mongos.getDB(kDbName).foo.insert({"x": 505});

    // the shard we move the doc to has not seen _id
    id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;

    // Update move from shard 0 to shard 1
    assert.writeOK(sessionDB.foo.update({"x" : 4}, {"$set" : {"x": 510}}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(510, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);


    // the shard we move the doc to has seen _id
    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    // Update move from shard 1 to shard 0
    assert.writeOK(sessionDB.foo.update({"x" : 500}, {"$set" : {"x": 20}}));
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    // _id issue --> two docs with _id : 5 
    /*assert.writeOK(sessionDB.foo.insert({"x":35, "a": 4, "_id":5}));
    assert.writeOK(sessionDB.foo.insert({"x":3005, "a": 46, "_id":5}));
    assert.writeOK(sessionDB.foo.update({"_id" : 5}, {"$set" : {"x": 45}}));
    jsTestLog(mongos.getDB(kDbName).foo.find({"_id":5}).toArray());*/

    mongos.getDB(kDbName).foo.drop();

    // ----------

    // find and modify
    shardCollectionMoveChunks();
    cleanupOrphanedDocs(kDbName + '.foo');

    //make sure won't get ssv
    mongos.getDB(kDbName).foo.insert({"x": 505});

    // the shard we move the doc to has not seen _id
    id = mongos.getDB(kDbName).foo.find({"x": 4}).toArray()[0]._id;
    jsTestLog("xxx will do find and mod here");
    sessionDB.foo.findAndModify({query : {"x" : 4}, update : {$set : {"x": 510}}});
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 4}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 510}).toArray().length);
    assert.eq(510, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    id = mongos.getDB(kDbName).foo.find({"x": 500}).toArray()[0]._id;

    sessionDB.foo.findAndModify({query : {"x" : 500}, update : {$set : {"x": 20}}});
    assert.eq(0, mongos.getDB(kDbName).foo.find({"x": 500}).toArray().length);
    assert.eq(1, mongos.getDB(kDbName).foo.find({"x": 20}).toArray().length);
    assert.eq(20, mongos.getDB(kDbName).foo.find({"_id": id}).toArray()[0].x);

    mongos.getDB(kDbName).foo.drop();

    st.stop();

})();