var clusterStatsHelpers = {
    getStat:function (obj, propString) {

        if (!propString) {
            return obj;
        }

        var prop, props = propString.split('.');

        for (var i = 0, iLen = props.length - 1; i < iLen; i++) {
            prop = props[i];

            if (typeof obj == 'object' && obj !== null && prop in obj) {
                obj = obj[prop];
            } else {
                break;
            }
        }
        return obj[props[i]];
    },

    roundedWithCommas:function (x) {
        return this.withCommas(Math.round(x));
    },

    withCommas:function (x) {
        if (x != null) {
            return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        }
    },

    byteFormat:function (value) {
        if (value != null) {
            if (value < 1000) {
                return Math.round(value) + " b";
            }
            if (value < 1000000) {
                return Math.round((value / 1000)) + " kb";
            }
            if (value < 1000000000) {
                return Math.round((value / 1000000)) + " mb";
            }
            return (Math.round((value / 10000000)) / 100) + " gb";
        }
    },

    ageFormat:function (value) {
        if (value < 60) {
            return value + "sec";
        }
        if (value < 3600) {
            return (value / 60) + "min";
        }
        return (value / 3600) + 'hr';
    },


    serversByRole:function (role) {
        var servers = [];
        $.each(this.stats, function (idx, server) {
            if (_.indexOf(server.roles, role) > -1) {
                servers[idx] = {workerId:server.workerId, hostname:server.hostname, status:server.status, joinDate:server.joinDate};
            }
        });

        return servers;
    },

    serversByGroupStat:function (groupName) {
        var servers = [];
        $.each(this.stats, function (idx, server) {
            $.each(server.groupStatsList, function (idx2, groupStat) {
                if (groupStat.name == groupName) {
                    servers[idx] = {workerId:server.workerId, hostname:server.hostname, status:server.status, joinDate:server.joinDate, groupStat:groupStat};
                }
            });
        });

        return servers;
    },

    commonStatByWorkerId:function (workerId) {
        var returnStat = {};
        var count = 0;

        $.each(this.stats, function (idx, server) {
            if (server.workerId == workerId) {
                $.each(server.groupStatsList, function (idx2, groupStat) {
                    if (groupStat.name == "Common") {
                        returnStat = groupStat;
                    }
                });
            }
        });

        return returnStat;
    },

    colorByStatus:function (status) {
        if (status == 'Running') {
            return "green";
        }
        if (status == 'Offline') {
            return "orange";
        }
        if (status == 'Unknown') {
            return "red";
        }
    },

    colorByServers:function (servers) {
        var color = "greenNum";

        $.each(this.stats, function (idx, server) {
            if (server.status == "Offline" && color != "redNum") {
                color = "blueNum";
            }
            if (server.status == "Unknown") {
                return "redNum";
            }
        });

        return color;
    },

    prettyDate:function (date_str) {
        var time_formats = [
            [60, 'just now', 1],
            // 60
            [120, '1 minute ago', '1 minute from now'],
            // 60*2
            [3600, 'minutes', 60],
            // 60*60, 60
            [7200, '1 hour ago', '1 hour from now'],
            // 60*60*2
            [86400, 'hours', 3600],
            // 60*60*24, 60*60
            [172800, 'yesterday', 'tomorrow'],
            // 60*60*24*2
            [604800, 'days', 86400],
            // 60*60*24*7, 60*60*24
            [1209600, 'last week', 'next week'],
            // 60*60*24*7*4*2
            [2419200, 'weeks', 604800],
            // 60*60*24*7*4, 60*60*24*7
            [4838400, 'last month', 'next month'],
            // 60*60*24*7*4*2
            [29030400, 'months', 2419200],
            // 60*60*24*7*4*12, 60*60*24*7*4
            [58060800, 'last year', 'next year'],
            // 60*60*24*7*4*12*2
            [2903040000, 'years', 29030400],
            // 60*60*24*7*4*12*100, 60*60*24*7*4*12
            [5806080000, 'last century', 'next century'],
            // 60*60*24*7*4*12*100*2
            [58060800000, 'centuries', 2903040000] // 60*60*24*7*4*12*100*20, 60*60*24*7*4*12*100
        ];
        //var time = ('' + date_str).replace(/-/g, "/").replace(/[TZ]/g, " ").replace(/^\s\s*/, '').replace(/\s\s*$/, '');
        var time = date_str;
        if (time.substr(time.length - 4, 1) == ".") {
            time = time.substr(0, time.length - 4);
        }
        var seconds = (new Date - new Date(time)) / 1000;
        var token = 'ago', list_choice = 1;
        if (seconds < 0) {
            seconds = Math.abs(seconds);
            token = 'from now';
            list_choice = 2;
        }
        var i = 0, format;
        while (format = time_formats[i++])
            if (seconds < format[0]) {
                if (typeof format[2] == 'string') {
                    return format[list_choice];
                }
                else {
                    return Math.floor(seconds / format[2]) + ' ' + format[1] + ' ' + token;
                }
            }
        return time;
    }
};