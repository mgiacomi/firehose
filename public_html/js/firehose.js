function flotPercentFormatter(v, axis) {
    return v.toFixed(axis.tickDecimals) + "%";
}

function flotTotFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 1000) {
        return value;
    }
    if(value < 1000000) {
        return (value / 1000) + "k";
    }
    if(value < 1000000000) {
        return Math.round((value / 10000) / 100) + "m";
    }
    return Math.round((value / 10000000) / 100) + "bn";
}

function flotByteFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

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

function flotAgeMsFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 1000) {
        return value + "ms";
    }
    if(value < 60000) {
        return Math.round(value / 1000) + "sec";
    }
    if(value < 3600000) {
        return Math.round(value / 60000) + "min";
    }
    return Math.round(value / 3600000) + 'hr';
}

function flotAgeFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 60) {
        return value + " sec";
    }
    if(value < 3600) {
        return Math.round(value / 60) + " min";
    }
    return Math.round(value / 3600) + ' hr';
}

function updateObjectArray(object) {
    for(var workerId in object.workerData) {
        if(object.lastValueMap[workerId] != null) {
            var newValue = 0;
            var hostname = object.lastValueMap[workerId].hostname;
            if(workerId in object.lastValueMap) {
                newValue = object.lastValueMap[workerId].statValue;
            }
            object.workerData[workerId].data = this.updateDataArray(object.workerData[workerId].data, newValue);
            object.workerData[workerId].flotProperties = { data:this.toFlotArray(object.workerData[workerId].data), label:hostname };
        }
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
}

function updateDataArray(data, value) {
    // Remove oldest
    if (data.length > 0 && data.length > this.totalPoints) {
        data = data.slice(1);
    }

    // Add newest
    data.push(value);

    return data;
}

function toFlotArray(data) {
    var res = [];
    for (var i = 0; i < data.length; ++i) {
        res.push([i, data[i]])
    }
    return res;
}

function initAccordion() {
    //===== Accordion =====//
    $('div.menu_body:eq(0)').show();
    $('.acc .head:eq(0)').show().css({color:"#2B6893"});

    $(".acc .head").click(function () {
        $(this).css({color:"#2B6893"}).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");
        $(this).siblings().css({color:"#404040"});
    });
}

function initGeneral() {
    //===== Collapsible elements management =====//
    $('.exp').collapsible({
        defaultOpen: 'current',
        cookieName: 'navAct',
        cssOpen: 'active corner',
        cssClose: 'inactive',
        speed: 300
    });

    $('.opened').collapsible({
        defaultOpen: 'opened,toggleOpened',
        cssOpen: 'inactive',
        cssClose: 'normal',
        speed: 200
    });

    $('.closed').collapsible({
        defaultOpen: '',
        cssOpen: 'inactive',
        cssClose: 'normal',
        speed: 200
    });

    //===== ToTop =====//
    $().UItoTop({ easingType:'easeOutQuart' });
}
