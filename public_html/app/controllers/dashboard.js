Firehose.module('Dashboard', function (Dashboard, App, Backbone, Marionette, $, _) {

    // Router
    Dashboard.Router = Marionette.AppRouter.extend({
        appRoutes:{
            '':'index',
            'dashboard/roles':'roles',
            'dashboard/servers':'servers',
            'dashboard/performance':'performance'
        }
    });

    // Controller
    Dashboard.Controller = function () {};

    _.extend(Dashboard.Controller.prototype, {

        // Default action
        index: function() {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Main());
        },

        roles: function() {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Roles());
        },

        servers: function() {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Servers());
        },

        performance: function() {
            App.middlenav.show(new Dashboard.Views.Nav());
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
