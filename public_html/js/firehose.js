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
