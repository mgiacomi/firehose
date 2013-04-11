Firehose.module('Outbound', function (Outbound, App, Backbone, Marionette, $, _) {

    // Router
    Outbound.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'outbound/overview':'overview',
            'outbound/live/stats':'live_stats',
            'outbound/performance':'performance'
        }
    });

    // Controller
    Outbound.Controller = function () {};

    _.extend(Outbound.Controller.prototype, {

        overview: function() {
            App.middlenav.show(new Outbound.Views.Nav());
            App.content.show(new Outbound.Views.Overview({model:clusterStats}));
        },

        live_stats: function() {
            App.middlenav.show(new Outbound.Views.Nav());
            App.content.show(new Outbound.Views.LiveStats({model:clusterStats}));
        },

        performance: function() {
            App.middlenav.show(new Outbound.Views.Nav());
            App.content.show(new Outbound.Views.Performance({model:clusterStats}));
        }
    });

    // Initializer
    Outbound.addInitializer(function () {
        var controller = new Outbound.Controller();
        new Outbound.Router({
            controller:controller
        });
    });

});
