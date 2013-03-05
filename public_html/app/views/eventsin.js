Firehose.module('Eventsin.Views', function(Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'eventsin_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template: 'eventsin_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template: 'eventsin_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template: 'eventsin_performance'
    });

});