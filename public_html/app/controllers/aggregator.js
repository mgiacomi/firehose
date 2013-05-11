Firehose.module('Aggregator', function (Aggregator, App, Backbone, Marionette, $, _) {

    // Router
    Aggregator.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'aggregator/overview':'overview',
            'aggregator/active/batches':'active_batches',
            'aggregator/live/stats':'live_stats',
            'aggregator/performance':'performance'
        }
    });

    // Controller
    Aggregator.Controller = function () {
    };

    _.extend(Aggregator.Controller.prototype, {

        overview:function () {
            App.middlenav.show(new Aggregator.Views.Nav());
            App.content.show(new Aggregator.Views.Overview({model:clusterStats}));
        },

        active_batches:function () {
            App.middlenav.show(new Aggregator.Views.Nav());
            App.content.show(new Aggregator.Views.ActiveBatches({model:clusterStats}));
        },

        live_stats:function () {
            App.middlenav.show(new Aggregator.Views.Nav());
            App.content.show(new Aggregator.Views.LiveStats({model:clusterStats}));
        },

        performance:function () {
            App.middlenav.show(new Aggregator.Views.Nav());
            App.content.show(new Aggregator.Views.Performance({model:clusterStats}));
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
