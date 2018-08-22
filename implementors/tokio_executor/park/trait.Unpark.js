(function() {var implementors = {};
implementors["tokio_executor"] = [];
implementors["tokio_reactor"] = ["impl <a class=\"trait\" href=\"tokio_executor/park/trait.Unpark.html\" title=\"trait tokio_executor::park::Unpark\">Unpark</a> for <a class=\"struct\" href=\"tokio_reactor/struct.Handle.html\" title=\"struct tokio_reactor::Handle\">Handle</a>",];
implementors["tokio_threadpool"] = ["impl <a class=\"trait\" href=\"tokio_executor/park/trait.Unpark.html\" title=\"trait tokio_executor::park::Unpark\">Unpark</a> for <a class=\"struct\" href=\"tokio_threadpool/park/struct.DefaultUnpark.html\" title=\"struct tokio_threadpool::park::DefaultUnpark\">DefaultUnpark</a>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
