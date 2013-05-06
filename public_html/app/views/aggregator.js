Firehose.module('Aggregator.Views', function (Views, App, Backbone, Marionette, $, _) {

    Views.Nav = Marionette.ItemView.extend({
        template:'aggregator_nav'
    });

    Views.Overview = Marionette.ItemView.extend({
        template:'aggregator_overview'
    });

    Views.LiveStats = Marionette.ItemView.extend({
        template:'aggregator_livestats',
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'aggregator_performance',
        messageRecvPerSecPlot:{},
        messageRecvPerSecData:{workerData:{}},
        messageCollectedPerSecPlot:{},
        messageCollectedPerSecData:{workerData:{}},
        timeCollectBatchPlot:{},
        timeCollectBatchData:{workerData:{}},
        messagesPerBatchPlot:{},
        messagesPerBatchData:{workerData:{}},
        activeBatchesPlot:{},
        activeBatchesData:{workerData:{}},
        queueSizePlot:{},
        queueSizeData:{workerData:{}},
        totalMessagesPlot:{},
        totalMessagesData:{workerData:{}},
        oldestMessagePlot:{},
        oldestMessageData:{workerData:{}},
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
                that.messageRecvPerSecPlot = $.plot($("#messageRecvPerSec"), [], that.plotTotOptions);
                that.messageCollectedPerSecPlot = $.plot($("#messageCollectedPerSec"), [], that.plotTotOptions);
                that.timeCollectBatchPlot = $.plot($("#timeCollectBatch"), [], that.plotTotOptions);
                that.messagesPerBatchPlot = $.plot($("#messagesPerBatch"), [], that.plotTotOptions);
                that.activeBatchesPlot = $.plot($("#activeBatches"), [], that.plotTotOptions);
                that.queueSizePlot = $.plot($("#queueSize"), [], that.plotByteOptions);
                that.totalMessagesPlot = $.plot($("#totalMessages"), [], that.plotTotOptions);
                that.oldestMessagePlot = $.plot($("#oldestMessage"), [], that.plotAgeOptions);
            }, this);
        },

        updateData:function () {
            var that = this;
            this.messageRecvPerSecData.lastValueMap = {};
            this.messageCollectedPerSecData.lastValueMap = {};
            this.timeCollectBatchData.lastValueMap = {};
            this.messagesPerBatchData.lastValueMap = {};
            this.activeBatchesData.lastValueMap = {};
            this.queueSizeData.lastValueMap = {};
            this.totalMessagesData.lastValueMap = {};
            this.oldestMessageData.lastValueMap = {};

            $.each(this.model.get("stats"), function (idx, server) {
                $.each(server.groupStatsList, function (idx2, groupStat) {
                    if (groupStat.name == "Aggregator") {
                        var totalMessages = server.groupStatsList.Aggregator.countStats.AddMessage_Count.sec5;
                        totalMessages += server.groupStatsList.Aggregator.countStats.AddBackupMessage_Count.sec5;
                        var statValue = Math.round(totalMessages / 5);
                        that.messageRecvPerSecData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        totalMessages = server.groupStatsList.Aggregator.avgStats.MessagesCollectedPerBatch_Avg.sec5.total;
                        totalMessages += server.groupStatsList.Aggregator.avgStats.BackupMessagesCollectedPerBatch_Avg.sec5.total;
                        statValue = Math.round(totalMessages / 5);
                        that.messageCollectedPerSecData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        totalMessages = server.groupStatsList.Aggregator.avgStats.BatchCollect_Time.sec5.average;
                        totalMessages += server.groupStatsList.Aggregator.avgStats.BackupBatchCollect_Time.sec5.average;
                        statValue = Math.round(totalMessages);
                        that.timeCollectBatchData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        totalMessages = server.groupStatsList.Aggregator.avgStats.MessagesCollectedPerBatch_Avg.sec5.average;
                        totalMessages += server.groupStatsList.Aggregator.avgStats.BackupMessagesCollectedPerBatch_Avg.sec5.average;
                        statValue = Math.round(totalMessages);
                        that.messagesPerBatchData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Aggregator.avgStats.ActiveBatches_Avg.sec5.average;
                        that.activeBatchesData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        var totalSize = server.groupStatsList.Aggregator.avgStats.TotalQueueSize_Avg.sec5.average;
                        totalSize += server.groupStatsList.Aggregator.avgStats.TotalBackupQueueSize_Avg.sec5.average;
                        statValue = Math.round(totalSize);
                        that.queueSizeData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        totalMessages = server.groupStatsList.Aggregator.avgStats.MessagesInQueue_Avg.sec5.average;
                        totalMessages += server.groupStatsList.Aggregator.avgStats.BackupMessagesInQueue_Avg.sec5.average;
                        statValue = Math.round(totalMessages);
                        that.totalMessagesData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};

                        statValue = server.groupStatsList.Aggregator.avgStats.OldestInQueue_Avg.sec5.average;
                        that.oldestMessageData.lastValueMap[server.workerId] = {hostname: server.hostname, statValue: statValue};
                    }
                });
            });

            this.updateObjectArray(this.messageRecvPerSecData);
            this.updateObjectArray(this.messageCollectedPerSecData);
            this.updateObjectArray(this.timeCollectBatchData);
            this.updateObjectArray(this.messagesPerBatchData);
            this.updateObjectArray(this.activeBatchesData);
            this.updateObjectArray(this.queueSizeData);
            this.updateObjectArray(this.totalMessagesData);
            this.updateObjectArray(this.oldestMessageData);

            var messageRecvPerSecDataArray = [];
            for(var workerId in this.messageRecvPerSecData.workerData) {
                messageRecvPerSecDataArray.push(this.messageRecvPerSecData.workerData[workerId].flotProperties);
            }
            this.messageRecvPerSecPlot.setData(messageRecvPerSecDataArray);
            this.messageRecvPerSecPlot.setupGrid();
            this.messageRecvPerSecPlot.draw();

            var messageCollectedPerSecDataArray = [];
            for(workerId in this.messageCollectedPerSecData.workerData) {
                messageCollectedPerSecDataArray.push(this.messageCollectedPerSecData.workerData[workerId].flotProperties);
            }
            this.messageCollectedPerSecPlot.setData(messageCollectedPerSecDataArray);
            this.messageCollectedPerSecPlot.setupGrid();
            this.messageCollectedPerSecPlot.draw();

            var timeCollectBatchDataArray = [];
            for(workerId in this.timeCollectBatchData.workerData) {
                timeCollectBatchDataArray.push(this.timeCollectBatchData.workerData[workerId].flotProperties);
            }
            this.timeCollectBatchPlot.setData(timeCollectBatchDataArray);
            this.timeCollectBatchPlot.setupGrid();
            this.timeCollectBatchPlot.draw();

            var messagesPerBatchDataArray = [];
            for(workerId in this.messagesPerBatchData.workerData) {
                messagesPerBatchDataArray.push(this.messagesPerBatchData.workerData[workerId].flotProperties);
            }
            this.messagesPerBatchPlot.setData(messagesPerBatchDataArray);
            this.messagesPerBatchPlot.setupGrid();
            this.messagesPerBatchPlot.draw();

            var activeBatchesDataArray = [];
            for(workerId in this.activeBatchesData.workerData) {
                activeBatchesDataArray.push(this.activeBatchesData.workerData[workerId].flotProperties);
            }
            this.activeBatchesPlot.setData(activeBatchesDataArray);
            this.activeBatchesPlot.setupGrid();
            this.activeBatchesPlot.draw();

            var queueSizeDataArray = [];
            for(workerId in this.queueSizeData.workerData) {
                queueSizeDataArray.push(this.queueSizeData.workerData[workerId].flotProperties);
            }
            this.queueSizePlot.setData(queueSizeDataArray);
            this.queueSizePlot.setupGrid();
            this.queueSizePlot.draw();

            var totalMessagesDataArray = [];
            for(workerId in this.totalMessagesData.workerData) {
                totalMessagesDataArray.push(this.totalMessagesData.workerData[workerId].flotProperties);
            }
            this.totalMessagesPlot.setData(totalMessagesDataArray);
            this.totalMessagesPlot.setupGrid();
            this.totalMessagesPlot.draw();

            var oldestMessageDataArray = [];
            for(workerId in this.oldestMessageData.workerData) {
                oldestMessageDataArray.push(this.oldestMessageData.workerData[workerId].flotProperties);
            }
            this.oldestMessagePlot.setData(oldestMessageDataArray);
            this.oldestMessagePlot.setupGrid();
            this.oldestMessagePlot.draw();
        },

        plotAgeOptions:{
            xaxis:{ ticks:false },
            yaxes:[{ min:0, position:"right", tickFormatter:flotAgeFormatter }],
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