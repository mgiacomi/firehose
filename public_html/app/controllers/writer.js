Firehose.module('Writer', function (Writer, App, Backbone, Marionette, $, _) {

    // Router
    Writer.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'writer/overview':'overview',
            'writer/live/stats':'live_stats',
            'writer/performance':'performance'
        }
    });

    // Controller
    Writer.Controller = function () {
    };

    _.extend(Writer.Controller.prototype, {

        overview:function () {
            App.content.show(new Writer.Views.Overview());
            App.middlenav.show(new Writer.Views.Nav());
        },

        live_stats:function () {
            App.content.show(new Writer.Views.LiveStats());
            App.middlenav.show(new Writer.Views.Nav());
        },

        performance:function () {
            App.content.show(new Writer.Views.Performance());
            App.middlenav.show(new Writer.Views.Nav());
        }
    });

    // Initializer
    Writer.addInitializer(function () {
        var controller = new Writer.Controller();
        new Writer.Router({
            controller:controller
        });
    });

});
