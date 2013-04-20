Firehose.module('Cluster.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'cluster_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'cluster_overview'
    });

    Views.Diagnostics = Marionette.ItemView.extend({
        template:'cluster_diagnostics'
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'cluster_performance'
    });

});