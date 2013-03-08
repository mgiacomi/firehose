Firehose.module('Storage', function (Storage, App, Backbone, Marionette, $, _) {

    // Router
    Storage.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'storage/overview':'overview',
            'storage/live/stats':'live_stats',
            'storage/performance':'performance'
        }
    });

    // Controller
    Storage.Controller = function () {
    };

    _.extend(Storage.Controller.prototype, {

        overview:function () {
            App.content.show(new Storage.Views.Overview());
            App.middlenav.show(new Storage.Views.Nav());
        },

        live_stats:function () {
            App.content.show(new Storage.Views.LiveStats());
            App.middlenav.show(new Storage.Views.Nav());
        },

        performance:function () {
            App.content.show(new Storage.Views.Performance());
            App.middlenav.show(new Storage.Views.Nav());
        }
    });

    // Initializer
    Storage.addInitializer(function () {
        var controller = new Storage.Controller();
        new Storage.Router({
            controller:controller
        });
    });

});
