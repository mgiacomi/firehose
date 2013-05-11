var ClusterStats = Backbone.Model.extend({
    defaults:{
        stats:{},
        aggregateStats:{},
        activeBatches:{}
    },

    update:function (data) {
        var json = jQuery.parseJSON(data);
        this.set("stats", json.stats);
        this.set("aggregateStats", json.aggregateStats);
        this.set("activeBatches", json.activeBatches);
    }
});
var clusterStats = new ClusterStats();
