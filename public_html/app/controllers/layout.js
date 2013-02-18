Firehose.module('Layout', function (Layout, App, Backbone, Marionette, $, _) {

    // Controller
    Layout.Controller = function () {
    };

    _.extend(Layout.Controller.prototype, {

        start:function () {
            var topbar = new App.Layout.Views.Topbar();
            App.topbar.show(topbar);

            var header = new App.Layout.Views.Header();
            App.header.show(header);

            var leftnav = new App.Layout.Views.Leftnav();
            App.leftnav.show(leftnav);

            var footer = new App.Layout.Views.Footer();
            App.footer.show(footer);
        }
    });

    // Initializer
    Layout.addInitializer(function () {
        var controller = new Layout.Controller();
        controller.start();
    });

});
