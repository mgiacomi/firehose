Firehose.module('Cluster', function (Cluster, App, Backbone, Marionette, $, _) {

    // Router
    Cluster.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'cluster/overview':'overview',
            'cluster/diagnostics':'diagnostics',
            'cluster/performance':'performance'
        }
    });

    // Controller
    Cluster.Controller = function () {
    };

    _.extend(Cluster.Controller.prototype, {

        overview:function () {
            App.middlenav.show(new Cluster.Views.Nav());
            App.content.show(new Cluster.Views.Overview({model:clusterStats}));
        },

        diagnostics:function () {
            App.middlenav.show(new Cluster.Views.Nav());
            App.content.show(new Cluster.Views.Diagnostics({model:clusterStats}));
        },

        performance:function () {
            App.middlenav.show(new Cluster.Views.Nav());
            App.content.show(new Cluster.Views.Performance({model:clusterStats}));
        }
    });

    // Initializer
    Cluster.addInitializer(function () {
        var controller = new Cluster.Controller();
        new Cluster.Router({
            controller:controller
        });
    });

});
