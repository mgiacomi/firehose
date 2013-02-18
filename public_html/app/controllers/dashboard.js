Firehose.module('Dashboard', function (Dashboard, App, Backbone, Marionette, $, _) {

    // Router
    Dashboard.Router = Marionette.AppRouter.extend({
        appRoutes:{
            '':'index'
        }
    });

    // Controller
    Dashboard.Controller = function () {
    };

    _.extend(Dashboard.Controller.prototype, {

        // Default action
        index: function() {
            App.content.show(new Dashboard.Views.MainView());
        }
    });

    // Initializer
    Dashboard.addInitializer(function () {
        var controller = new Dashboard.Controller();
        new Dashboard.Router({
            controller:controller
        });
    });

});
