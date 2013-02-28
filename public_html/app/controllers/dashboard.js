Firehose.module('Dashboard', function (Dashboard, App, Backbone, Marionette, $, _) {

    // Router
    Dashboard.Router = Marionette.AppRouter.extend({
        appRoutes:{
            '':'index',
            'dashboard_roles':'roles',
            'dashboard_servers':'servers',
            'dashboard_performance':'performance'
        }
    });

    // Controller
    Dashboard.Controller = function () {
    };

    _.extend(Dashboard.Controller.prototype, {

        // Default action
        index: function() {
            App.content.show(new Dashboard.Views.Main());
            App.middlenav.show(new Dashboard.Views.Nav());
        },

        roles: function() {
            App.content.show(new Dashboard.Views.Roles());
        },

        servers: function() {
            App.content.show(new Dashboard.Views.Servers());
        },

        performance: function() {
            App.content.show(new Dashboard.Views.Performance());
        }
    });

    // Initializer
    Dashboard.addInitializer(function () {
        var controller = new Dashboard.Controller();
        new Dashboard.Router({
            controller:controller
        });
    });

});
