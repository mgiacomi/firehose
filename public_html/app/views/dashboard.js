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
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"render"
        }
    });

    Views.Servers = Marionette.ItemView.extend({
        template:'dashboard_servers',
        templateHelpers:clusterStatsHelpers,

        modelEvents:{
            "change:stats":"updateTable"
        },

        updateTable:function () {
            oTable.fnClearTable();

            $.each(this.model.get("stats"), function (idx, server) {
                var row = [];

                row[0] = server.hostname;
                row[1] = server.workerId;

                var roles = "";
                $.each(server.roles, function (idx, role) {
                    if (idx > 0) {
                        roles += ", ";
                    }
                    roles += role;
                });

                row[2] = roles;
                row[3] = '<strong class="' + clusterStatsHelpers.colorByStatus(server.status) + '">' + server.status + '</strong>';
                row[4] = clusterStatsHelpers.prettyDate(server.joinDate);


                oTable.fnAddData(row);
            });
        }
    });

    Views.Performance = Marionette.ItemView.extend({
        template:'dashboard_performance',
        inboundPlot:{},
        inboundLoad:[],
        inboundAvgMsgSize:[],
        inboundMsgPerSec:[],

        aggregatorPlot:{},
        aggregatorMsgInQue:[],
        aggregatorQueSize:[],
        aggregatorQueAge:[],

        storageWriterPlot:{},
        storageWriterMsgPerSec:[],
        storageWriterBytesPerSec:[],
        storageWriterBatchesBeingWritten:[],

        outboundPlot:{},

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
                that.inboundPlot = $.plot($("#inbound"), [
                    { data:that.updateDataArray(that.inboundMsgPerSec, 0) },
                    { data:that.updateDataArray(that.inboundAvgMsgSize, 0) },
                    { data:that.updateDataArray(that.inboundLoad, 0) }
                ], that.inboundOptions);

                that.aggregatorPlot = $.plot($("#aggregator"), [
                    { data:that.updateDataArray(that.aggregatorMsgInQue, 0) },
                    { data:that.updateDataArray(that.aggregatorQueSize, 0) },
                    { data:that.updateDataArray(that.aggregatorQueAge, 0) }
                ], that.aggregatorOptions);

                that.storageWriterPlot = $.plot($("#storageWriter"), [
                    { data:that.updateDataArray(that.storageWriterMsgPerSec, 0) },
                    { data:that.updateDataArray(that.storageWriterBytesPerSec, 0) },
                    { data:that.updateDataArray(that.storageWriterBatchesBeingWritten, 0) }
                ], that.storageWriterOptions);

                /*
                 that.outboundPlot = $.plot($("#outbound"), [
                 { data:that.getRandomData() },{ data:that.getRandomData2() },{ data:that.getRandomData3() }
                 ], that.outboundOptions);
                 */

            }, this);
        },

        updateData:function () {
            this.inboundMsgPerSec = this.updateDataArray(this.inboundMsgPerSec, this.model.get("aggregateStats").inboundMsgPerSec);
            this.inboundAvgMsgSize = this.updateDataArray(this.inboundAvgMsgSize, this.model.get("aggregateStats").inboundAvgMsgSize);
            this.inboundLoad = this.updateDataArray(this.inboundLoad, this.model.get("aggregateStats").inboundLoad);

            this.inboundPlot.setData([
                { data:this.toFlotArray(this.inboundMsgPerSec), color:"#54cb4b", label:"Messages Per/Sec" },
                { data:this.toFlotArray(this.inboundAvgMsgSize), color:"#6db6ee", label:"Avg Message Size", yaxis:2 },
                { data:this.toFlotArray(this.inboundLoad), color:"#ee7951", label:"Load Avg", yaxis:3 }
            ]);
            this.inboundPlot.setupGrid();
            this.inboundPlot.draw();

            this.aggregatorMsgInQue = this.updateDataArray(this.aggregatorMsgInQue, this.model.get("aggregateStats").aggregatorMsgInQue);
            this.aggregatorQueSize = this.updateDataArray(this.aggregatorQueSize, this.model.get("aggregateStats").aggregatorQueSize);
            this.aggregatorQueAge = this.updateDataArray(this.aggregatorQueAge, this.model.get("aggregateStats").aggregatorQueAge);

            this.aggregatorPlot.setData([
                { data:this.toFlotArray(this.aggregatorMsgInQue), color:"#54cb4b", label:"Messages In Queue" },
                { data:this.toFlotArray(this.aggregatorQueSize), color:"#6db6ee", label:"Queue Size", yaxis:2 },
                { data:this.toFlotArray(this.aggregatorQueAge), color:"#ee7951", label:"Queue Age (seconds)", yaxis:3 }
            ]);
            this.aggregatorPlot.setupGrid();
            this.aggregatorPlot.draw();

            this.storageWriterMsgPerSec = this.updateDataArray(this.storageWriterMsgPerSec, this.model.get("aggregateStats").storageWriterMsgPerSec);
            this.storageWriterBytesPerSec = this.updateDataArray(this.storageWriterBytesPerSec, this.model.get("aggregateStats").storageWriterBytesPerSec);
            this.storageWriterBatchesBeingWritten = this.updateDataArray(this.storageWriterBatchesBeingWritten, this.model.get("aggregateStats").storageWriterBatchesBeingWritten);

            this.storageWriterPlot.setData([
                { data:this.toFlotArray(this.storageWriterMsgPerSec), color:"#54cb4b", label:"Messages Per/Sec" },
                { data:this.toFlotArray(this.storageWriterBytesPerSec), color:"#6db6ee", label:"Bytes Per/Sec", yaxis:2 },
                { data:this.toFlotArray(this.storageWriterBatchesBeingWritten), color:"#ee7951", label:"Batches Being Written", yaxis:3 }
            ]);
            this.storageWriterPlot.setupGrid();
            this.storageWriterPlot.draw();

            /*
             this.outboundPlot.setData([
             { data:this.getRandomData(), color:"#54cb4b", label:"Messages Per/Sec" },
             { data:this.getRandomData2(), color:"#6db6ee", label:"Avg Message Size", yaxis:2 },
             { data:this.getRandomData3(), color:"#ee7951", label:"Active Queries", yaxis:3 }
             ]);
             this.outboundPlot.setupGrid();
             this.outboundPlot.draw();
             */
        },

        // Inbound
        inboundOptions:{
            xaxis:{ ticks:false },
            yaxes:[
                {
                    min:0,
                    position:"right",
                    color:"#54cb4b",
                    tickFormatter:flotTotFormatter
                },
                {
                    min:0,
                    position:"right",
                    color:"#6db6ee",
                    tickFormatter:flotByteFormatter
                },
                {
                    min:0,
                    max:100,
                    position:"right",
                    color:"#ee7951"
                }
            ],
            series:{
                lines:{ lineWidth:1 }
            }
        },

        // Aggregator
        aggregatorOptions:{
            xaxis:{ ticks:false },
            yaxes:[
                {
                    min:0,
                    position:"right",
                    color:"#54cb4b",
                    tickFormatter:flotTotFormatter
                },
                {
                    min:0,
                    position:"right",
                    color:"#6db6ee",
                    tickFormatter:flotByteFormatter
                },
                {
                    min:0,
                    position:"right",
                    color:"#ee7951",
                    tickFormatter:flotAgeFormatter
                }
            ],
            series:{
                lines:{ lineWidth:1 }
            }
        },

        // StorageWriter
        storageWriterOptions:{
            xaxis:{ ticks:false },
            yaxes:[
                {
                    min:0,
                    position:"right",
                    color:"#54cb4b",
                    tickFormatter:flotTotFormatter
                },
                {
                    min:0,
                    position:"right",
                    color:"#6db6ee",
                    tickFormatter:flotByteFormatter
                },
                {
                    min:0,
                    position:"right",
                    color:"#ee7951"
                }
            ],
            series:{
                lines:{ lineWidth:1 }
            }
        },

        // Outbound
        outboundOptions:{
            xaxis:{ ticks:false },
            yaxes:[
                {
                    min:0,
                    position:"right",
                    color:"#54cb4b",
                    tickFormatter:flotTotFormatter
                },
                {
                    alignTicksWithAxis:1,
                    position:"right",
                    color:"#6db6ee",
                    tickFormatter:flotByteFormatter
                },
                {
                    min:0,
                    max:100,
                    position:"right",
                    color:"#ee7951"
                }
            ],
            series:{
                lines:{ lineWidth:1 }
            }
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