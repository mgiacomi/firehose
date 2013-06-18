var webSocketChannel = {
    init : function() {
        console.log("Initializing WebSocketChannel.");

        var protocol = "ws:";
        if (window.location.protocol == "https:") {
            protocol = "wss:";
        }
        var location = protocol + "//"+ window.location.hostname + (window.location.port ? ":"+ window.location.port : "") +"/socket/stats";

        this._ws = new WebSocket(location);
        this._ws.onopen = this._onopen;
        this._ws.onmessage = this._onmessage;
        this._ws.onclose = this._onclose;
        this._ws.onerror = this._onerror;
    },

    _onopen : function() {
        console.log("WebSocket connection opened.");
    },

    _onmessage : function(m) {
        if (m.data) {
            clusterStats.update(m.data);
        }
    },

    _onclose : function(m) {
        this._ws = null;
        console.log("WebSocket connection closed.");
        setTimeout(function(){webSocketChannel.init()}, 1000);
    },

    _onerror : function(e) {
        console.log("WebSocket connection error. "+ e);
        alert(e);
    },

    _send : function(user, message) {
        if (this._ws) {
            //this._ws.send(user + ':' + message);
        }
    }
};
