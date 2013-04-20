Firehose.module('Inbound.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'inbound_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'inbound_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'inbound_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'inbound_performance'
    });

});