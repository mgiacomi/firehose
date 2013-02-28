Firehose.module('Layout', function (Layout, App, Backbone, Marionette, $, _) {

    // Controller
    Layout.Controller = function () {
    };

    _.extend(Layout.Controller.prototype, {

        start:function () {
            App.topbar.show(new App.Layout.Views.Topbar());
//            App.middlenav.show(new App.Dashboard.Views.Nav());
            App.leftnav.show(new App.Layout.Views.Leftnav());
            App.footer.show(new App.Layout.Views.Footer());
        }
    });

    // Initializer
    Layout.addInitializer(function () {
        var controller = new Layout.Controller();
        controller.start();
    });

});
