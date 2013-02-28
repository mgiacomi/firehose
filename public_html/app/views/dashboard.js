Firehose.module('Dashboard.Views', function(Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'dashboard_nav'
    });

    Views.Main = Marionette.ItemView.extend({
        template: 'dashboard_overview'
    });

    Views.Roles = Marionette.ItemView.extend({
        template: 'dashboard_roles'
    });

    Views.Servers = Marionette.ItemView.extend({
        template: 'dashboard_servers'
    });

    Views.Performance = Marionette.ItemView.extend({
        template: 'dashboard_performance'
    });

});