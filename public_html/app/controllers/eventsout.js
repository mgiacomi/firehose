Firehose.module('Eventsout', function (Eventsout, App, Backbone, Marionette, $, _) {

    // Router
    Eventsout.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'events/out/overview':'overview',
            'events/out/live/stats':'live_stats',
            'events/out/performance':'performance'
        }
    });

    // Controller
    Eventsout.Controller = function () {};

    _.extend(Eventsout.Controller.prototype, {

        overview: function() {
            App.content.show(new Eventsout.Views.Overview());
            App.middlenav.show(new Eventsout.Views.Nav());
        },

        live_stats: function() {
            App.content.show(new Eventsout.Views.LiveStats());
            App.middlenav.show(new Eventsout.Views.Nav());
        },

        performance: function() {
            App.content.show(new Eventsout.Views.Performance());
            App.middlenav.show(new Eventsout.Views.Nav());
        }
    });

    // Initializer
    Eventsout.addInitializer(function () {
        var controller = new Eventsout.Controller();
        new Eventsout.Router({
            controller:controller
        });
    });

});
