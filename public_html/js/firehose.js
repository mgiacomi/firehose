var Firehose = new Backbone.Marionette.Application();

Firehose.addRegions({
    topbar: '#topbarWrapper',
    header: '#headerWrapper',
    leftnav: '#leftnavWrapper',
    footer: '#footerWrapper'
});

// Router
var AppRouter = Backbone.Router.extend({

    routes:{
        "":"list"
    },

    list:function () {
    }

});

Firehose.on('initialize:after', function() {
    var app = new AppRouter();

    var header = new Firehose.Layout.Header();
    Firehose.header.show(header);

    var leftnav = new Firehose.Layout.Leftnav();
    Firehose.leftnav.show(leftnav);

    Backbone.history.start();
});

Backbone.Marionette.TemplateCache.prototype.loadTemplate = function (templateId) {
    return templateLoader.get(templateId);
};

templateLoader = {
    // Hash of preloaded templates for the app
    templates:{},

    // Recursively pre-load all the templates for the app.
    // This implementation should be changed in a production environment:
    // All the template files should be concatenated in a single file.
    load:function (names, callback) {
        var that = this;

        var loadTemplate = function (index) {
            var name = names[index];
            console.log('Loading template: ' + name);
            $.get('templates/' + name + '.html', function (data) {
                that.templates[name] = data;
                index++;
                if (index < names.length) {
                    loadTemplate(index);
                } else {
                    callback();
                }
            });
        }

        loadTemplate(0);
    },

    // Get template by name from hash of preloaded templates
    get:function (name) {
        return this.templates[name];
    }
};