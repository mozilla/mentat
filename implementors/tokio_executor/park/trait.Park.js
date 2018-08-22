(function() {var implementors = {};
implementors["tokio_executor"] = [];
implementors["tokio_reactor"] = ["impl <a class=\"trait\" href=\"tokio_executor/park/trait.Park.html\" title=\"trait tokio_executor::park::Park\">Park</a> for <a class=\"struct\" href=\"tokio_reactor/struct.Reactor.html\" title=\"struct tokio_reactor::Reactor\">Reactor</a>",];
implementors["tokio_threadpool"] = ["impl <a class=\"trait\" href=\"tokio_executor/park/trait.Park.html\" title=\"trait tokio_executor::park::Park\">Park</a> for <a class=\"struct\" href=\"tokio_threadpool/park/struct.DefaultPark.html\" title=\"struct tokio_threadpool::park::DefaultPark\">DefaultPark</a>",];
implementors["tokio_timer"] = ["impl&lt;T, N&gt; <a class=\"trait\" href=\"tokio_executor/park/trait.Park.html\" title=\"trait tokio_executor::park::Park\">Park</a> for <a class=\"struct\" href=\"tokio_timer/timer/struct.Timer.html\" title=\"struct tokio_timer::timer::Timer\">Timer</a>&lt;T, N&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"tokio_executor/park/trait.Park.html\" title=\"trait tokio_executor::park::Park\">Park</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;N: Now,&nbsp;</span>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
