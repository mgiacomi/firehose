Firehose.module('Layout.Views', function (Views, App, Backbone, Marionette, $, _) {
    // Layout Topbar View
    Views.Topbar = Marionette.ItemView.extend({
        template:'topbar'
    });

    // Layout Leftnav View
    Views.Leftnav = Marionette.ItemView.extend({
        template:'leftnav',

        events:{
            'click .leftNavLink':'leftNavLink'
        },

        leftNavLink:function (e) {
            $(".leftNavLink").each(function () {
                $(this).removeClass('active');
            });

            $(e.currentTarget).addClass('active');
        }

    });

    // Layout Footer View
    Views.Footer = Marionette.ItemView.extend({
        template:'footer'
    });

});