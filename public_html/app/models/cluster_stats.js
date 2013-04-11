var ClusterStats = Backbone.Model.extend({
    defaults: {stats: {}},

    update:function (data) {
        this.set("stats", jQuery.parseJSON(data));
console.log(data);
//        this.trigger("change:stats");
//        Firehose.vent.trigger("stats:updated");
        /*
         json = statsManager._stats_data;
         console.log(json[0].workerId);
         console.log(json[0].hostname);
         console.log(json[0].groupStatsList.length);
         json[0].groupStatsList.forEach(function (gs) {
         console.log("group: "+ gs.name);
         gs.avgStats.forEach(function (as) {
         console.log("  avgstat: "+ as.name);
         });
         gs.countStats.forEach(function (cs) {
         console.log("  countStat: "+ cs.name);
         });
         });

         var servers = "";
         json.forEach(function (slist) {
         servers = servers + " " + slist.hostname + "("+ slist.workerId + ") ";
         });
         console.log("server list:"+ servers);
         console.log("--------------------------------------");
         */
    },

    servers:function () {
        var servers = "";
        $.each(statsManager._stats_data, function (idx, slist) {
            servers = servers + " " + slist.hostname + "(" + slist.workerId + ") ";
        });
        console.log("server list:" + servers);
        console.log("--------------------------------------");
    }
});
var clusterStats = new ClusterStats();
