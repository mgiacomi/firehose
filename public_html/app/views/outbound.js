Firehose.module('Outbound.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'outbound_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'outbound_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'outbound_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'outbound_performance'
    });

});