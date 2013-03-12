Firehose.module('Writer.Views', function(Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'writer_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template: 'writer_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template: 'writer_livestats'
    });

    Views.Performance = Marionette.ItemView.extend({
        template: 'writer_performance'
    });

});