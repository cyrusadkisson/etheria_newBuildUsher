const AWS = require('aws-sdk');
var options = {
  maxRetries: 1,
  httpOptions: {
    timeout: 900000
  }
};
const lambda = new AWS.Lambda(options);
const dynamoDB = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });
const pako = require('pako');

// this "usher" ("helper", "factory", "manager" whatever) function does a few middleware things so 
// the other lambdas can stay clean and extremely *micro* service-y

// First, mapUpdaterAndEventDetectorDaemon is responsible for detecting Etheria state changes, updating the cached maps and firing "events" (discord msgs, delegating builds, etc)
//          but it is NOT responsible for generating renderable builds nor updating their storage (DynamoDB for now)
//
// Second, hexStringToBuild is entirely responsible for changing a hex string (from the chain) into Three.js data
//          but it is NOT responsible for compressing said data (required for dynamo) or storing in the database (btw lambda-to-lambda limit is 6 MB... plenty)
//
// This function 
//      1. Takes the "event" (hexString, tileIndex, blockNumber, version) from mapupdaterAndEventDetectorDaemon when a new build is made
//      2. Asks hexStringToBuild to generate the Three.js hexShapes object
//      3. Compresses the response
//      4. Inserts in the database
//      5. Updates the buildIndices globalvar noting the new build
//
// Note: With this delineation, we can easily insert new builds in the DB for whatever tileIndex and blockNumber 
//          we want without needing a blockchain event to fire
//       It also limits what dependencies each of these Lambdas needs. 
//          E.g. mapUpdaterAndEventDetectorDaemon is the only one that needs S3, web3 and Discord API (axios)
//          hexStringToBuild is the only one that needs Three.js
//          newBuildUsher is the only one that needs DynamoDB and pako

function isNumeric(str) {
    if (typeof str != "string")
        return false; // we only process strings!  
    return !isNaN(str) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
        !isNaN(parseFloat(str)); // ...and ensure strings of whitespace fail
}

exports.handler = async (event) => {
    console.log("event=" + JSON.stringify(event));

    return new Promise((resolve, reject) => {

        if (!event || Object.keys(event).length === 0) {
            reject(new Error("event is invalid or missing"));
            return;
        }

        if (!event.params) {
            reject(new Error("event.params is invalid or missing"));
            return;
        }

        if (!event.params.querystring) {
            reject(new Error("event.params.querystring is invalid or missing"));
            return;
        }
        console.log("querystring=" + JSON.stringify(event.params.querystring));

        if (!event.params.querystring.tileIndex || !isNumeric(event.params.querystring.tileIndex) || (event.params.querystring.tileIndex * 1) > 1088) { // non-existant, not numeric or greater than tile index limit
            reject(new Error("event.params.querystring.tileIndex is invalid or missing"));
            return;
        }
        console.log("tileIndex=" + JSON.stringify(event.params.querystring.tileIndex));

        if (!event.params.querystring.blockNumber || !isNumeric(event.params.querystring.blockNumber)) {
            reject(new Error("event.params.querystring.blockNumber is invalid or missing"));
            return;
        }
        console.log("blockNumber=" + JSON.stringify(event.params.querystring.blockNumber));

        if (!event.params.querystring.hexString) {
            reject(new Error("event.params.querystring.hexString is invalid or missing"));
            return;
        }
        console.log("hexString=" + JSON.stringify(event.params.querystring.hexString));

        if (!
            (
                event.params.querystring.version === "0.9" ||
                event.params.querystring.version === "1.0" ||
                event.params.querystring.version === "1.1" ||
                event.params.querystring.version === "1.2"
            )
        ) {
            reject(new Error("Invalid or missing version parameter"));
            return;
        }
        console.log("version=" + event.params.querystring.version);

        lambda.invoke({
            FunctionName: "arn:aws:lambda:us-east-1:540151370381:function:etheria_hexStringToBuild",
            Payload: JSON.stringify({
                "body-json": {},
                "params": {
                    "path": {},
                    "querystring": {
                        "hexString": event.params.querystring.hexString
                    }
                }
            }) // pass params
        }, function(err, data) {
            if (err) {
                reject(err);
            }
            else {
                //console.log("data back from getMapState=" + JSON.stringify(JSON.parse(data.Payload),null,2)); // successful response
                var hexShapes = JSON.parse(data.Payload);

                var compressed = pako.deflate(JSON.stringify(hexShapes), { to: 'string' });
                console.log("compressed.length=" + compressed.length);
                var params = {
                    TableName: 'EtheriaBuildsUnified',
                    Item: {
                        'tileIndexAndVersion': event.params.querystring.tileIndex + "v" + event.params.querystring.version,
                        'blockNumber': (event.params.querystring.blockNumber*1),
                        'build': compressed,
                        'tileIndex': event.params.querystring.tileIndex,
                        'version': event.params.querystring.version,
                        'hexString': event.params.querystring.hexString
                    }
                };

                dynamoDB.put(params, function(err, data) {
                    if (err) {
                        reject(err);
                    }
                    else {
                        console.log("EtheriaBuildsUnified put success.");

                        // update buildIndices globalVar. This is the running list of the indices with actual builds on them
                        // so we don't have to grab every build for every tile everytime the explorer or builder is loaded
                        var params = {
                            TableName: "EtheriaGlobalVars",
                            Key: {
                                "name": "buildIndicesV" + event.params.querystring.version
                            }
                        };

                        dynamoDB.get(params, function(err, globalVarEntry) {
                            if (err) {
                                console.log("Error", err);
                                reject(err);
                            }
                            else {
                                console.log("success getting buildIndices globalVar", JSON.stringify(globalVarEntry));
                                var buildIndices = JSON.parse(globalVarEntry.Item.value);
                                var buildIndicesSet = new Set(buildIndices); // convert to set to eliminate dupes
                                console.log("adding " + event.params.querystring.tileIndex + " to buildIndices globalVar...");
                                buildIndicesSet.add(event.params.querystring.tileIndex * 1);
                                buildIndices = [...buildIndicesSet]; // back to array
                                console.log("done. Array is now " + JSON.stringify(buildIndices));
                                var params2 = {
                                    TableName: "EtheriaGlobalVars",
                                    Item: {
                                        "name": "buildIndicesV" + event.params.querystring.version,
                                        "value": JSON.stringify(buildIndices)
                                    }
                                };
                                dynamoDB.put(params2, function(err, data) { // update
                                    if (err) {
                                        console.log(" error updating buildIndices globalVar", err);
                                        reject();
                                    }
                                    else {
                                        console.log(" success putting buildIndices globalVar");
                                        resolve();
                                    }
                                });
                            }
                        });

                    }
                });
            }
        });
    });
};
