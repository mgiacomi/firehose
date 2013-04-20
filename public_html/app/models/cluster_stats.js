var ClusterStats = Backbone.Model.extend({
    defaults:{
        stats:{},
        aggregateStats:{}
    },

    update:function (data) {
        var json = jQuery.parseJSON(data);
        this.set("aggregateStats", json.aggregateStats);
        this.set("stats", json.stats);
    }
});
var clusterStats = new ClusterStats();
