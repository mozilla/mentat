(function() {var implementors = {};
implementors["hyper"] = [{text:"impl <a class=\"trait\" href=\"hyper/server/trait.Service.html\" title=\"trait hyper::server::Service\">Service</a> for <a class=\"struct\" href=\"hyper/client/struct.HttpConnector.html\" title=\"struct hyper::client::HttpConnector\">HttpConnector</a>",synthetic:false,types:["hyper::client::connect::HttpConnector"]},{text:"impl&lt;C, B&gt; <a class=\"trait\" href=\"hyper/server/trait.Service.html\" title=\"trait hyper::server::Service\">Service</a> for <a class=\"struct\" href=\"hyper/client/struct.Client.html\" title=\"struct hyper::client::Client\">Client</a>&lt;C, B&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;C: <a class=\"trait\" href=\"hyper/client/trait.Connect.html\" title=\"trait hyper::client::Connect\">Connect</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;B: <a class=\"trait\" href=\"futures/stream/trait.Stream.html\" title=\"trait futures::stream::Stream\">Stream</a>&lt;Error = <a class=\"enum\" href=\"hyper/error/enum.Error.html\" title=\"enum hyper::error::Error\">Error</a>&gt; + 'static,<br>&nbsp;&nbsp;&nbsp;&nbsp;B::<a class=\"type\" href=\"futures/stream/trait.Stream.html#associatedtype.Item\" title=\"type futures::stream::Stream::Item\">Item</a>: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.slice.html\">[</a><a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a><a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.slice.html\">]</a>&gt;,&nbsp;</span>",synthetic:false,types:["hyper::client::Client"]},];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
