Firehose.module('Eventsin', function (Eventsin, App, Backbone, Marionette, $, _) {

    // Router
    Eventsin.Router = Marionette.AppRouter.extend({
        appRoutes:{
            'eventsin':'index'
        }
    });

    // Controller
    Eventsin.Controller = function () {
    };

    _.extend(Eventsin.Controller.prototype, {

        // Default action
        index: function() {
            App.content.show(new Eventsin.Views.Main());
            App.middlenav.show(new Eventsin.Views.Nav());
        }
    });

    // Initializer
    Eventsin.addInitializer(function () {
        var controller = new Eventsin.Controller();
        new Eventsin.Router({
            controller:controller
        });
    });

});
