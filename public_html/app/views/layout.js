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
        },

        onRender:function () {
            setTimeout(function() {
                if(Backbone.history.fragment.length > 1) {
                    $(".leftNavLink").each(function () {
                        $(this).removeClass('active');
                        if('#'+Backbone.history.fragment.split("/")[0] == $(this).attr('href').split("/")[0]) {
                            $(this).addClass('active');
                        }
                    });
                }
            }, 0);
        }
    });

    // Layout Footer View
    Views.Footer = Marionette.ItemView.extend({
        template:'footer'
    });

});