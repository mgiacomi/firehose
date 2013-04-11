Firehose.module('Inbound', function (Inbound, App, Backbone, Marionette, $, _) {

    // Router
    Inbound.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'inbound/overview':'overview',
            'inbound/live/stats':'live_stats',
            'inbound/performance':'performance'
        }
    });

    // Controller
    Inbound.Controller = function () {};

    _.extend(Inbound.Controller.prototype, {

        overview: function() {
            App.middlenav.show(new Inbound.Views.Nav());
            App.content.show(new Inbound.Views.Overview({model:clusterStats}));
        },

        live_stats: function() {
            App.middlenav.show(new Inbound.Views.Nav());
            App.content.show(new Inbound.Views.LiveStats({model:clusterStats}));
        },

        performance: function() {
            App.middlenav.show(new Inbound.Views.Nav());
            App.content.show(new Inbound.Views.Performance({model:clusterStats}));
        }
    });

    // Initializer
    Inbound.addInitializer(function () {
        var controller = new Inbound.Controller();
        new Inbound.Router({
            controller:controller
        });
    });

});
