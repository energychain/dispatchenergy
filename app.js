var ApiBuilder = require('claudia-api-builder'),
  api = new ApiBuilder();
const AWS = require('aws-sdk');

AWS.config.update({
    region: "eu-central-1"
});

module.exports = api;

const http_request = require("request");

const getDistanceFromLatLonInKm = function(lat1,lon1,lat2,lon2) {
  var R = 6371; // Radius of the earth in km
  var dLat = deg2rad(lat2-lat1);  // deg2rad below
  var dLon = deg2rad(lon2-lon1);
  var a =
    Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
    Math.sin(dLon/2) * Math.sin(dLon/2)
    ;
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  var d = R * c; // Distance in km
  return d;
}

const deg2rad = function(deg) {
  return deg * (Math.PI/180)
}

const main = async function() {
  return new Promise(async function (resolve, reject)  {
    http_request("https://api.corrently.io/core/dispatch",async function(e,r,b) {
      let market = JSON.parse(b);
      market.dispatches = [];
      /* Strategy:
       * Iterate all producers and dispatch to nearest consumers
       */
      for(let i=0; i<market.producers.length;i++) {
        let generation = market.producers[i].energy;
        market.producers[i].targets = [];
        market.producers[i].generation = generation;

        let nearest_idx =-1;
        let nearest_distance = 999999999999999999999;
        let min_distance = 0;
        let old_distance = -1;
        while((market.producers[i].energy>0)&&(old_distance<min_distance)) {
            for(let j=0; j<market.consumers.length;j++) {
                if(typeof market.consumers[j].demand == "undefined" ) market.consumers[j].demand=market.consumers[j].energy;
                if(typeof market.consumers[j].sources == "undefined" ) market.consumers[j].sources=[];

                let lat2 = Math.abs(market.producers[i].lat - market.consumers[j].lat);
                lat2 *= lat2;
                let lng2 = Math.abs(market.producers[i].lng - market.consumers[j].lng);
                lng2 *= lng2;
                let distance2 = lat2 + lng2;
                if((distance2 < nearest_distance)&&(distance2>min_distance)) {
                  nearest_distance = distance2;
                  nearest_idx = j;
                }
            }
            if(nearest_idx > -1) {
              if(market.producers[i].energy >= market.consumers[nearest_idx].energy) {
                let dispatch = {
                  generator:i,
                  consumer:nearest_idx,
                  energy:market.consumers[nearest_idx].energy
                };
                if(dispatch.energy >0) {
                  market.dispatches.push(dispatch);
                  market.producers[i].targets.push(dispatch);
                  market.consumers[nearest_idx].sources.push(dispatch);
                }
                market.producers[i].energy -= market.consumers[nearest_idx].energy;
                market.consumers[nearest_idx].energy =0;
              } else {
                let dispatch={
                  generator:i,
                  consumer:nearest_idx,
                  energy:market.producers[i].energy
                };
                if(dispatch.energy >0) {
                  market.dispatches.push(dispatch);
                  market.producers[i].targets.push(dispatch);
                  market.consumers[nearest_idx].sources.push(dispatch);
                }
                market.consumers[nearest_idx].energy -=market.producers[i].energy;
                market.producers[i].energy =0;
              }
            }
            old_distance = min_distance;
            min_distance=nearest_distance;
        }
      } // end for producers
      let costfactor = 0;
      let total_energy = 0;

      const singleDispatcher = async function(dispatch) {
          return new Promise(async function (resolve3, reject3)  {
            var docClient = new AWS.DynamoDB.DocumentClient();
            docClient.get({
              TableName:'dispatch-from',
              Key:{zip:dispatch.generator.zip}
            },function(err,data) {
              if(err) console.log(err);
              let item = {};
              item.zip = dispatch.generator.zip
              item.values={};
              if((data != null)&&(data.Item != null)) {
                item = data.Item;
              }
              if(typeof item.values["to_"+dispatch.consumer.zip] == "undefined") {
                item.values["to_"+dispatch.consumer.zip] = 0;
              }
              item.values["to_"+dispatch.consumer.zip] += 1* dispatch.energy;
              docClient.put({
                TableName:'dispatch-from',
                Item:item
              },function(err,data) {
                docClient.get({
                  TableName:'dispatch-to',
                  Key:{zip:dispatch.consumer.zip}
                },function(err,data) {
                  item = {};
                  item.zip = dispatch.consumer.zip
                  item.values={};
                  if((data != null)&&(data.Item != null)) {
                    item = data.Item;
                  }
                  if(typeof item.values["from_"+dispatch.generator.zip] == "undefined") {
                    item.values["from_"+dispatch.generator.zip] = 0;
                  }
                  item.values["from_"+dispatch.generator.zip] += 1* dispatch.energy;
                  docClient.put({
                    TableName:'dispatch-to',
                    Item:item
                  },function(err,data) {
                      resolve3();
                  });
              })
            })
          });
        });
      }
      for(let i=0;i<market.dispatches.length;i++) {
        market.dispatches[i].generator = market.producers[market.dispatches[i].generator];
        delete market.dispatches[i].generator.energy;
        delete market.dispatches[i].generator.targets;
        delete market.dispatches[i].generator.generation;
        market.dispatches[i].consumer = market.consumers[market.dispatches[i].consumer];
        delete market.dispatches[i].consumer.energy;
        delete market.dispatches[i].consumer.sources;
        delete market.dispatches[i].consumer.demand;

        market.dispatches[i].distance = getDistanceFromLatLonInKm(market.dispatches[i].consumer.lat,market.dispatches[i].consumer.lng,market.dispatches[i].generator.lat,market.dispatches[i].generator.lng);
        market.dispatches[i].costfactor = market.dispatches[i].distance / market.dispatches[i].energy;
        costfactor+=market.dispatches[i].costfactor;
        total_energy+=market.dispatches[i].energy;
        singleDispatcher(market.dispatches[i]);
      }
      // Ab hier sind alle Market Dispatches vorhanden und könnten gepatched werden.

      market.costfactor = costfactor/market.dispatches.length;
      market.updated = new Date().getTime();
      market.energy=total_energy;
      var docClient = new AWS.DynamoDB.DocumentClient();
      docClient.put({
        TableName:'corrently-dispatch',
        Item:{
          updated:market.updated,
          costfactor:market.costfactor,
          energy:market.energy,
          dispatches:market.dispatches.length,
          ttl:Math.round(market.updated/1000)+86400
        }
      },function(err,data) {
        resolve(market);
      })

    });
  });
}

api.get('/market',main);
