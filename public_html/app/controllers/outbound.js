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
            App.content.show(new Outbound.Views.Overview());
            App.middlenav.show(new Outbound.Views.Nav());
        },

        live_stats: function() {
            App.content.show(new Outbound.Views.LiveStats());
            App.middlenav.show(new Outbound.Views.Nav());
        },

        performance: function() {
            App.content.show(new Outbound.Views.Performance());
            App.middlenav.show(new Outbound.Views.Nav());
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
