Firehose.module('Aggregator.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'aggregator_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'aggregator_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'aggregator_livestats',
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'aggregator_performance'
    });

});