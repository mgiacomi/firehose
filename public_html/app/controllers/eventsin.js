Firehose.module('Eventsin', function (Eventsin, App, Backbone, Marionette, $, _) {

    // Router
    Eventsin.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'eventsin/overview':'overview',
            'eventsin/live/stats':'live_stats',
            'eventsin/performance':'performance'
        }
    });

    // Controller
    Eventsin.Controller = function () {};

    _.extend(Eventsin.Controller.prototype, {

        overview: function() {
            App.content.show(new Eventsin.Views.Overview());
            App.middlenav.show(new Eventsin.Views.Nav());
        },

        live_stats: function() {
            App.content.show(new Eventsin.Views.LiveStats());
            App.middlenav.show(new Eventsin.Views.Nav());
        },

        performance: function() {
            App.content.show(new Eventsin.Views.Performance());
            App.middlenav.show(new Eventsin.Views.Nav());
        }
    });

    // Initializer
    Eventsin.addInitializer(function () {
        var controller = new Eventsin.Controller();
        new Eventsin.Router({
            controller:controller
        });
    });

});
