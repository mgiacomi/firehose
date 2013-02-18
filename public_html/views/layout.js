Firehose.module('Layout', function(Layout, App, Backbone, Marionette, $, _) {
    // Layout Topbar View
    Layout.Topbar = Backbone.Marionette.ItemView.extend({
        template: 'topbar'
    });

    // Layout Header View
    Layout.Header = Backbone.Marionette.ItemView.extend({
        template: 'header'
    });

    // Layout Leftnav View
    Layout.Leftnav = Backbone.Marionette.ItemView.extend({
        template: 'leftnav'
    });

    // Layout Footer View
    Layout.Footer = Backbone.Marionette.ItemView.extend({
        template: 'footer'
    });

});