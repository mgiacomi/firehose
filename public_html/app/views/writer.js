Firehose.module('Writer.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'writer_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'writer_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'writer_livestats',
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'writer_performance',
        avgTimeWritingBatchPlot:{},
        avgTimeWritingBatchData:{workerData:{}},
        totalBatchesWrittenPlot:{},
        totalBatchesWrittenData:{workerData:{}},
        currentlyWritingBatchesPlot:{},
        currentlyWritingBatchesData:{workerData:{}},
        totalMessagesWrittenPlot:{},
        totalMessagesWrittenData:{workerData:{}},
        totalBytesWrittenPlot:{},
        totalBytesWrittenData:{workerData:{}},
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
                that.avgTimeWritingBatchPlot = $.plot($("#avgTimeWritingBatch"), [], that.plotAgeMsOptions);
                that.totalBatchesWrittenPlot = $.plot($("#totalBatchesWritten"), [], that.plotTotOptions);
                that.currentlyWritingBatchesPlot = $.plot($("#currentlyWritingBatches"), [], that.plotTotOptions);
                that.totalMessagesWrittenPlot = $.plot($("#totalMessagesWritten"), [], that.plotTotOptions);
                that.totalBytesWrittenPlot = $.plot($("#totalBytesWritten"), [], that.plotByteOptions);
                that.vmCpuPlot = $.plot($("#vmCpu"), [], that.plotPercentOptions);
                that.loadAvgPlot = $.plot($("#loadAvg"), [], that.plotTotOptions);
            }, this);
        },

        updateData:function () {
            var that = this;
            this.avgTimeWritingBatchData.lastValueMap = {};
            this.totalBatchesWrittenData.lastValueMap = {};
            this.currentlyWritingBatchesData.lastValueMap = {};
            this.totalMessagesWrittenData.lastValueMap = {};
            this.totalBytesWrittenData.lastValueMap = {};
            this.vmCpuData.lastValueMap = {};
            this.loadAvgData.lastValueMap = {};

            $.each(this.model.get("stats"), function (idx, server) {
                $.each(server.groupStatsList, function (idx2, groupStat) {
                    if (groupStat.name == "StorageWriter") {
                        var statValue = server.groupStatsList.StorageWriter.avgStats.BatchesWritten_AvgTime.sec5.average;
                        that.avgTimeWritingBatchData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.StorageWriter.countStats.BatchesWritten_Count.sec5;
                        that.totalBatchesWrittenData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.StorageWriter.avgStats.WritingBatches_Avg.sec5.average;
                        that.currentlyWritingBatchesData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.StorageWriter.countStats.MessagesWritten_Count.sec5;
                        that.totalMessagesWrittenData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.StorageWriter.countStats.MessagesWritten_Size.sec5;
                        that.totalBytesWrittenData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Common.avgStats.CPU.sec5.average;
                        that.vmCpuData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Common.avgStats.LoadAvg.sec5.average;
                        that.loadAvgData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};
                    }
                });
            });

            updateObjectArray(this.avgTimeWritingBatchData);
            updateObjectArray(this.totalBatchesWrittenData);
            updateObjectArray(this.currentlyWritingBatchesData);
            updateObjectArray(this.totalMessagesWrittenData);
            updateObjectArray(this.totalBytesWrittenData);
            updateObjectArray(this.vmCpuData);
            updateObjectArray(this.loadAvgData);

            var avgTimeWritingBatchDataArray = [];
            for(var workerId in this.avgTimeWritingBatchData.workerData) {
                avgTimeWritingBatchDataArray.push(this.avgTimeWritingBatchData.workerData[workerId].flotProperties);
            }
            this.avgTimeWritingBatchPlot.setData(avgTimeWritingBatchDataArray);
            this.avgTimeWritingBatchPlot.setupGrid();
            this.avgTimeWritingBatchPlot.draw();

            var totalBatchesWrittenDataArray = [];
            for(workerId in this.totalBatchesWrittenData.workerData) {
                totalBatchesWrittenDataArray.push(this.totalBatchesWrittenData.workerData[workerId].flotProperties);
            }
            this.totalBatchesWrittenPlot.setData(totalBatchesWrittenDataArray);
            this.totalBatchesWrittenPlot.setupGrid();
            this.totalBatchesWrittenPlot.draw();

            var currentlyWritingBatchesDataArray = [];
            for(workerId in this.currentlyWritingBatchesData.workerData) {
                currentlyWritingBatchesDataArray.push(this.currentlyWritingBatchesData.workerData[workerId].flotProperties);
            }
            this.currentlyWritingBatchesPlot.setData(currentlyWritingBatchesDataArray);
            this.currentlyWritingBatchesPlot.setupGrid();
            this.currentlyWritingBatchesPlot.draw();

            var totalMessagesWrittenDataArray = [];
            for(workerId in this.totalMessagesWrittenData.workerData) {
                totalMessagesWrittenDataArray.push(this.totalMessagesWrittenData.workerData[workerId].flotProperties);
            }
            this.totalMessagesWrittenPlot.setData(totalMessagesWrittenDataArray);
            this.totalMessagesWrittenPlot.setupGrid();
            this.totalMessagesWrittenPlot.draw();

            var totalBytesWrittenDataArray = [];
            for(workerId in this.totalBytesWrittenData.workerData) {
                totalBytesWrittenDataArray.push(this.totalBytesWrittenData.workerData[workerId].flotProperties);
            }
            this.totalBytesWrittenPlot.setData(totalBytesWrittenDataArray);
            this.totalBytesWrittenPlot.setupGrid();
            this.totalBytesWrittenPlot.draw();

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
        }
    });
});