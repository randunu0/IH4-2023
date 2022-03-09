from tracemalloc import start
from flask import Flask, render_template, session, request, redirect, url_for
import datetime

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
    "RTSL": ["real-time-system-load", "longer description here.", "units here"],
    "RTSC": ["RTSC Name Here", "RTSC longer description to come", "units here"],
    "SASC": ["SASC Name Here ", "longer description to come", "units here"],
    "SEL": ["SEL Name Here ", "longer description to come", "units here"],
    "SPP": ["SPP Name Here", "longer description to come", "units here"],
    "SWL": ["SWL Name Here", "longer description to come", "units here"],
    "WPP": ["WPP Name Here", "longer description to come", "units here"],
}

@app.route('/chart/<chart_type>', methods=["GET", "POST"])
@app.route('/chart/<chart_type>/<start_date>/<end_date>', methods=["GET", "POST"])
def swd(chart_type='RTSL', start_date=None, end_date=None):
    if session.get('id', None):
        if request.method == "GET":
            if chart_type in chart_page_content:
                if start_date and end_date:
                    try:
                        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%Y')
                        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y')
                    except:
                        chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                        start_date = datetime.datetime.strptime(chartlabels[0], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                        end_date = datetime.datetime.strptime(chartlabels[-1], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                    chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                else:
                    chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                    start_date = datetime.datetime.strptime(chartlabels[0], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                    end_date = datetime.datetime.strptime(chartlabels[-1], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                return render_template('chart.html', chart_data=chartdata, chart_labels=chartlabels, page_content=chart_page_content[chart_type], chart_start_date=start_date, chart_end_date=end_date)
            else:
                return render_template('home.html', error="Chart not found")
        if request.method == "POST":
            if chart_type in chart_page_content:
                start_date = request.form.get("start_date")
                end_date = request.form.get("end_date")
                chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                return render_template('chart.html', chart_data=chartdata, chart_labels=chartlabels, page_content=chart_page_content[chart_type], chart_start_date=start_date, chart_end_date=end_date)
            else:
                return render_template('home.html', error="Chart not found")
        else:
            return render_template('home.html', error="Request method not valid")    
    else:
        return render_template('login.html', error="You are not signed in.")

@app.errorhandler(404)
def FUN_404(error):
    return render_template("404.html"), 404

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5001)
