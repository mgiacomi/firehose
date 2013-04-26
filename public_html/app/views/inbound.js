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
        messageSizePlot:{},
        messageSizeData:{workerData:{}},
        processTimePlot:{},
        processTimeData:{workerData:{}},
        requestQueuePlot:{},
        requestQueueData:{workerData:{}},
        vmCpuPlot:{},
        vmCpuData:{workerData:{}},
        loadAvgPlot:{},
        loadAvgData:{workerData:{}},
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
                that.messagePerSecPlot = $.plot($("#messagePerSec"), [], that.plotTotOptions);
                that.messageSizePlot = $.plot($("#messageSize"), [], that.plotByteOptions);
                that.processTimePlot = $.plot($("#processTime"), [], that.plotAgeMsOptions);
                that.requestQueuePlot = $.plot($("#requestQueue"), [], that.plotTotOptions);
                that.vmCpuPlot = $.plot($("#vmCpu"), [], that.plotPercentOptions);
                that.loadAvgPlot = $.plot($("#loadAvg"), [], that.plotTotOptions);
            }, this);
        },

        updateData:function () {
            var that = this;
            this.messagePerSecData.lastValueMap = {};
            this.messageSizeData.lastValueMap = {};
            this.processTimeData.lastValueMap = {};
            this.requestQueueData.lastValueMap = {};
            this.vmCpuData.lastValueMap = {};
            this.loadAvgData.lastValueMap = {};

            $.each(this.model.get("stats"), function (idx, server) {
                $.each(server.groupStatsList, function (idx2, groupStat) {
                    if (groupStat.name == "Inbound") {
                        var statValue = clusterStatsHelpers.roundedWithCommas(server.groupStatsList.Inbound.countStats.AddMessage_Count.sec5 / 5);
                        that.messagePerSecData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Inbound.avgStats.AddMessage_Size.sec5.average;
                        that.messageSizeData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Inbound.avgStats.AddMessage_Time.sec5.average;
                        that.processTimeData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = clusterStatsHelpers.roundedWithCommas(server.groupStatsList.Common.avgStats.ActiveRequest_Count.sec5.average / 5);
                        that.requestQueueData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = clusterStatsHelpers.roundedWithCommas(server.groupStatsList.Common.avgStats.CPU.sec5.average);
                        that.vmCpuData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = clusterStatsHelpers.roundedWithCommas(server.groupStatsList.Common.avgStats.LoadAvg.sec5.average);
                        that.loadAvgData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};
                    }
                });
            });

            this.updateObjectArray(this.messagePerSecData);
            this.updateObjectArray(this.messageSizeData);
            this.updateObjectArray(this.processTimeData);
            this.updateObjectArray(this.requestQueueData);
            this.updateObjectArray(this.vmCpuData);
            this.updateObjectArray(this.loadAvgData);

            var messagePerSecDataArray = [];
            for(var workerId in this.messagePerSecData.workerData) {
                messagePerSecDataArray.push(this.messagePerSecData.workerData[workerId].flotProperties);
            }
            this.messagePerSecPlot.setData(messagePerSecDataArray);
            this.messagePerSecPlot.setupGrid();
            this.messagePerSecPlot.draw();

            var messageSizeDataArray = [];
            for(workerId in this.messageSizeData.workerData) {
                messageSizeDataArray.push(this.messageSizeData.workerData[workerId].flotProperties);
            }
            this.messageSizePlot.setData(messageSizeDataArray);
            this.messageSizePlot.setupGrid();
            this.messageSizePlot.draw();

            var processTimeDataArray = [];
            for(workerId in this.processTimeData.workerData) {
                processTimeDataArray.push(this.processTimeData.workerData[workerId].flotProperties);
            }
            this.processTimePlot.setData(processTimeDataArray);
            this.processTimePlot.setupGrid();
            this.processTimePlot.draw();

            var requestQueueDataArray = [];
            for(workerId in this.requestQueueData.workerData) {
                requestQueueDataArray.push(this.requestQueueData.workerData[workerId].flotProperties);
            }
            this.requestQueuePlot.setData(requestQueueDataArray);
            this.requestQueuePlot.setupGrid();
            this.requestQueuePlot.draw();

            var vmCpuDataArray = [];
            for(workerId in this.vmCpuData.workerData) {
                vmCpuDataArray.push(this.vmCpuData.workerData[workerId].flotProperties);
            }
            this.vmCpuPlot.setData(vmCpuDataArray);
            this.vmCpuPlot.setupGrid();
            this.vmCpuPlot.draw();

            var loadAvgDataArray = [];
            for(workerId in this.loadAvgData.workerData) {
                loadAvgDataArray.push(this.loadAvgData.workerData[workerId].flotProperties);
            }
            this.loadAvgPlot.setData(loadAvgDataArray);
            this.loadAvgPlot.setupGrid();
            this.loadAvgPlot.draw();
        },

        plotAgeMsOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", tickFormatter:flotAgeMsFormatter }],
            series:{ lines:{ lineWidth:1 }}
        },

        plotTotOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", tickFormatter:flotTotFormatter }],
            series:{ lines:{ lineWidth:1 }}
        },

        plotByteOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", tickFormatter:flotByteFormatter }],
            series:{ lines:{ lineWidth:1 }}
        },

        plotPercentOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", tickFormatter:flotPercentFormatter }],
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