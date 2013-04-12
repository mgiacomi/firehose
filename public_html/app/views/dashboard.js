Firehose.module('Dashboard.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'dashboard_nav'
    });

    Views.Main = Marionette.ItemView.extend({
        template:'dashboard_overview',

        modelEvents:{
            //"change:stats":"render"
        }
    });

    Views.Roles = Marionette.ItemView.extend({
        template:'dashboard_roles',
        templateHelpers: clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Servers = Marionette.ItemView.extend({
        template:'dashboard_servers',
        templateHelpers: clusterStatsHelpers,

        modelEvents:{
            "change:stats":"updateTable"
        },

        updateTable:function() {
            oTable.fnClearTable();


            $.each(this.model.get("stats"), function (idx, server) {
                var row = [];

                row[0] = server.hostname;
                row[1] = server.workerId;

                var roles = "";
                $.each(server.roles, function (idx, role) {
                    if(idx > 0) {
                        roles += ", ";
                    }
                    roles += role;
                });

                row[2] = roles;
                row[3] = '<strong class="'+ clusterStatsHelpers.colorByStatus(server.status) +'">'+ server.status +'</strong>';
                row[4] = clusterStatsHelpers.prettyDate(server.joinDate);


                oTable.fnAddData(row);
            });
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