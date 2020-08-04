import livereload
import portray


def render_as_html():
    portray.in_browser()


server = livereload.Server()
server.watch("README.md", render_as_html)
server.watch("docs/**", render_as_html)
server.serve(root="site")
