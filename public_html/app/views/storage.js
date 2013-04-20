Firehose.module('Storage.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'storage_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'storage_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'storage_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'storage_performance'
    });

});