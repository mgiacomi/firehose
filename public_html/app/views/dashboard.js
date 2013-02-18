Firehose.module('Dashboard.Views', function(Views, App, Backbone, Marionette, $, _) {

    Views.MainView = Marionette.ItemView.extend({
        template: 'dashboard',

        ui: {
//            edit: '.edit'
        },

        events : {
//            'click .destroy': 'destroy',
        },

        initialize: function() {
//            this.bindTo(this.model, 'change', this.render, this);
        },

        onRender: function() {
//            this.$el.removeClass('active completed');
        }

//        destroy: function() {
//            this.model.destroy();
//        },
    });

});