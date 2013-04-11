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
            App.middlenav.show(new Writer.Views.Nav());
            App.content.show(new Writer.Views.Overview({model:clusterStats}));
        },

        live_stats:function () {
            App.middlenav.show(new Writer.Views.Nav());
            App.content.show(new Writer.Views.LiveStats({model:clusterStats}));
        },

        performance:function () {
            App.middlenav.show(new Writer.Views.Nav());
            App.content.show(new Writer.Views.Performance({model:clusterStats}));
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
