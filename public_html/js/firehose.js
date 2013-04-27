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
        return (value / 1000000) + "m";
    }
    return (value / 1000000000) + "bn";
}

function flotByteFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 1000) {
        return value + " b";
    }
    if(value < 1000000) {
        return (value / 1000) + " kb";
    }
    if(value < 1000000000) {
        return (value / 1000000) + " mb";
    }
    return (value / 1000000000) + " gb";
}

function flotAgeMsFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 1000) {
        return value + "ms";
    }
    if(value < 60000) {
        return (value / 1000) + "sec";
    }
    if(value < 3600000) {
        return (value / 60000) + "min";
    }
    return (value / 3600000) + 'hr';
}

function flotAgeFormatter(v, axis) {
    var value = v.toFixed(axis.tickDecimals);

    if(value < 60) {
        return value + " sec";
    }
    if(value < 3600) {
        return (value / 60) + " min";
    }
    return (value / 3600) + ' hr';
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
