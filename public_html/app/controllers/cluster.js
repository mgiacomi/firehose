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
            App.content.show(new Cluster.Views.Overview());
            App.middlenav.show(new Cluster.Views.Nav());
        },

        diagnostics:function () {
            App.content.show(new Cluster.Views.Diagnostics());
            App.middlenav.show(new Cluster.Views.Nav());
        },

        performance:function () {
            App.content.show(new Cluster.Views.Performance());
            App.middlenav.show(new Cluster.Views.Nav());
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
