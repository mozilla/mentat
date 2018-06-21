---
# Feel free to add content and custom Front Matter to this file.
# To modify the layout, see https://jekyllrb.com/docs/themes/#overriding-theme-defaults

layout: home
---

{{ site.description }}


<h2 class="post-list-heading">{{ page.apiheading | default: "API Documentation" }}</h2>
<ul class="post-list">
    <li>
        <h3>
            <a class="post-link" href="/apis/rust/mentat/index.html">
            Rust API
            </a>
        </h3>
    </li>
    <li>
        <h3>
            <a class="post-link" href="/apis/swift/index.html">
            Swift SDK API
            </a>
        </h3>
    </li>
    <li>
        <h3>
            <a class="post-link" href="/apis/java/index.html">
            Android SDK API
            </a>
        </h3>
    </li>
</ul>
