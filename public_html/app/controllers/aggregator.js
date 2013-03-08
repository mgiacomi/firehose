Firehose.module('Aggregator', function (Aggregator, App, Backbone, Marionette, $, _) {

    // Router
    Aggregator.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'aggregator/overview':'overview',
            'aggregator/live/stats':'live_stats',
            'aggregator/performance':'performance'
        }
    });

    // Controller
    Aggregator.Controller = function () {
    };

    _.extend(Aggregator.Controller.prototype, {

        overview:function () {
            App.content.show(new Aggregator.Views.Overview());
            App.middlenav.show(new Aggregator.Views.Nav());
        },

        live_stats:function () {
            App.content.show(new Aggregator.Views.LiveStats());
            App.middlenav.show(new Aggregator.Views.Nav());
        },

        performance:function () {
            App.content.show(new Aggregator.Views.Performance());
            App.middlenav.show(new Aggregator.Views.Nav());
        }
    });

    // Initializer
    Aggregator.addInitializer(function () {
        var controller = new Aggregator.Controller();
        new Aggregator.Router({
            controller:controller
        });
    });

});
