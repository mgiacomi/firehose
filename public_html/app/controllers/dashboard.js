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
    Dashboard.Controller = function () {
    };

    _.extend(Dashboard.Controller.prototype, {

        // Default action
        index:function () {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Main({model:clusterStats}));
        },

        roles:function () {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Roles({model:clusterStats}));
        },

        servers:function () {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Servers({model:clusterStats}));
        },

        performance:function () {
            App.middlenav.show(new Dashboard.Views.Nav());
            App.content.show(new Dashboard.Views.Performance({model:clusterStats}));
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
