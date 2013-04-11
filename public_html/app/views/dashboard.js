Firehose.module('Dashboard.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'dashboard_nav'
    });

    Views.Main = Marionette.ItemView.extend({
        template:'dashboard_overview',

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Roles = Marionette.ItemView.extend({
        template:'dashboard_roles',

        modelEvents:{
            //"change:stats":"render"
        }
    });

    Views.Servers = Marionette.ItemView.extend({
        template:'dashboard_servers',

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'dashboard_performance',

        modelEvents:{
            "change:stats":"updateData"
        },

        updateData:function () {
            console.log("update data now");
        }
    });

});