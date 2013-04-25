Firehose.module('Inbound.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'inbound_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'inbound_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'inbound_livestats',
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'inbound_performance',
        messagePerSecPlot:{},
        messagePerSecData:{workerData:{}},
        totalPoints:200,

        initialize:function () {
            this.listenTo(this, 'render', this.afterRender);
        },

        modelEvents:{
            "change:stats":"updateData"
        },

        afterRender:function () {
            var that = this;
            _.defer(function (caller) {
                that.messagePerSecPlot = $.plot($("#messagePerSec"), [], that.plotOptions);
            }, this);
        },

        updateData:function () {
            this.messagePerSecData.lastValueMap = {};
            var that = this;

            $.each(this.model.get("stats"), function (idx, server) {
                $.each(server.groupStatsList, function (idx2, groupStat) {
                    if (groupStat.name == "Inbound") {
                        var statValue = clusterStatsHelpers.roundedWithCommas(server.groupStatsList.Inbound.countStats.AddMessage_Count.sec5 / 5);
                        that.messagePerSecData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};
                    }
                });
            });

            this.updateObjectArray(this.messagePerSecData);

            var messagePerSecDataArray = [];
            for(var workerId in this.messagePerSecData.workerData) {
                messagePerSecDataArray.push(this.messagePerSecData.workerData[workerId].flotProperties);
            }

            this.messagePerSecPlot.setData(messagePerSecDataArray);
            this.messagePerSecPlot.setupGrid();
            this.messagePerSecPlot.draw();
        },

        plotOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", Formatter:flotTotFormatter }],
            series:{ lines:{ lineWidth:1 }}
        },

        updateObjectArray:function(object) {

            for(var workerId in object.workerData) {
                var newValue = 0;
                var hostname = object.lastValueMap[workerId].hostname;
                if(workerId in object.lastValueMap) {
                    newValue = object.lastValueMap[workerId].statValue;
                }
                object.workerData[workerId].data = this.updateDataArray(object.workerData[workerId].data, newValue);
                object.workerData[workerId].flotProperties = { data:this.toFlotArray(object.workerData[workerId].data), label:hostname };

            }

            for(var workerId in object.lastValueMap) {
                if(!(workerId in object.workerData)) {
                    var newValue = object.lastValueMap[workerId].statValue;
                    var hostname = object.lastValueMap[workerId].hostname;
                    object.workerData[workerId] = {};
                    object.workerData[workerId].data = [];
                    object.workerData[workerId].data = this.updateDataArray(object.workerData[workerId].data, newValue);
                    var flotData = this.toFlotArray(object.workerData[workerId].data);
                    object.workerData[workerId].flotProperties = { data:flotData, label:hostname };
                }
            }

            return object;
        },

        updateDataArray:function (data, value) {
            // Remove oldest
            if (data.length > 0 && data.length > this.totalPoints) {
                data = data.slice(1);
            }

            // Add newest
            data.push(value);

            return data;
        },

        toFlotArray:function (data) {
            var res = [];
            for (var i = 0; i < data.length; ++i) {
                res.push([i, data[i]])
            }
            return res;
        }
    });
});