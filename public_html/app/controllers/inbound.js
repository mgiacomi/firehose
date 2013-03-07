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
            App.content.show(new Inbound.Views.Overview());
            App.middlenav.show(new Inbound.Views.Nav());
        },

        live_stats: function() {
            App.content.show(new Inbound.Views.LiveStats());
            App.middlenav.show(new Inbound.Views.Nav());
        },

        performance: function() {
            App.content.show(new Inbound.Views.Performance());
            App.middlenav.show(new Inbound.Views.Nav());
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
