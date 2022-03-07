from flask import Flask, render_template, session, request, redirect, url_for

from resources.database import get_chart

app = Flask(__name__, static_url_path='', static_folder='')
app.secret_key = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
app.url_map.strict_slashes = False

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == 'POST':
        user = request.form.get("username")
        pw = request.form.get("password")

        if user == "FH10" and pw == "demo":
            session['id'] = user
            return redirect(url_for("home"))
        else:
            return render_template('login.html', error="Invalid username or password")
    else:
        return render_template('login.html')

@app.route("/")
def home():
    if session.get('id', None):
        return render_template('home.html')
    else:
        return render_template('login.html', error="Please login to access the site")

chart_page_content = {
    "RTSL": ["real-time-system-load", "longer description here."],
    "RTSC": ["real-time-system-c", "longer description to come"],
    "SASC": ["real-time-system-c", "longer description to come"],
    "SEL": ["real-time-system-c", "longer description to come"],
    "SPP": ["real-time-system-c", "longer description to come"],
    "SWL": ["real-time-system-c", "longer description to come"],
    "WPP": ["real-time-system-c", "longer description to come"],
}

@app.route('/chart/<chart_type>')
@app.route('/chart/<chart_type>/<start_date>/<end_date>')
def swd(chart_type = 'RTSL', start_date = 'today', end_date = 'today'):
    if session.get('id', None):
        chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
        return render_template('chart.html', chart_data=chartdata, chart_labels=chartlabels, page_content=chart_page_content[chart_type])
    else:
        return render_template('login.html', error="You are not signed in.")


@app.errorhandler(404)
def FUN_404(error):
    return render_template("404.html"), 404

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5001)
