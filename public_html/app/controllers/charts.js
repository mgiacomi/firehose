Firehose.module('Charts', function (Charts, App, Backbone, Marionette, $, _) {

    // Router
    Charts.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'charts':'index'
        }
    });

    // Controller
    Charts.Controller = function () {
    };

    _.extend(Charts.Controller.prototype, {

        // Default action
        index: function() {
            App.content.show(new Charts.Views.MainView());
        }
    });

    // Initializer
    Charts.addInitializer(function () {
        var controller = new Charts.Controller();
        new Charts.Router({
            controller:controller
        });
    });

});
