from flask import Flask
#from markets import markets
from prices import prices
import logging

app = Flask(__name__)

app.config['DEBUG'] = True
app.debug = True

#app.register_blueprint(markets)
app.register_blueprint(prices)

app.logger_name = "flask.app"

#@app.before_first_request
#def setup_logging():
#    if not app.debug:
        # In production mode, add log handler to sys.stderr.
#    app.logger.addHandler(logging.StreamHandler())
#    app.logger.setLevel(logging.INFO)

@app.route("/")
def hello():
#    app.logger.error("Hello World!")
    return "<h1 style='color:blue'>TCE Relay for Elite Dangerous</h1><p>See https://forums.frontier.co.uk/showthread.php/223056-RELEASE-Trade-Computer-Extension-Mk-II</p>"

if __name__ == "__main__":
    app.run(host='0.0.0.0')
