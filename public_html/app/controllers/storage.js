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
            App.middlenav.show(new Storage.Views.Nav());
            App.content.show(new Storage.Views.Overview({model:clusterStats}));
        },

        live_stats:function () {
            App.middlenav.show(new Storage.Views.Nav());
            App.content.show(new Storage.Views.LiveStats({model:clusterStats}));
        },

        performance:function () {
            App.middlenav.show(new Storage.Views.Nav());
            App.content.show(new Storage.Views.Performance({model:clusterStats}));
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
