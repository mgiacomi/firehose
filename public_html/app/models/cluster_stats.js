var ClusterStats = Backbone.Model.extend({
    defaults:{
        stats:{},
        aggregateStats:{},
        activeBatches:{},
        periodStatuses:{}
    },

    update:function (data) {
        var json = jQuery.parseJSON(data);
        this.set("stats", json.stats);
        this.set("aggregateStats", json.aggregateStats);
        this.set("activeBatches", json.activeBatches);
        this.set("periodStatuses", json.periodStatuses)
    }
});
var clusterStats = new ClusterStats();
