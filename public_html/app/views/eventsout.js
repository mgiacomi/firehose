Firehose.module('Eventsout.Views', function(Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'eventsout_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template: 'eventsout_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template: 'eventsout_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template: 'eventsout_performance'
    });

});