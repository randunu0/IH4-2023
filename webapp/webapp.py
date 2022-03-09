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
        return render_template('login.html')

chart_page_content = {
    "system-wide-demand": ["System-Wide Demand", "System-Wide Demand displays the current power demand of the ERCOT, which is the expected peak electrical power that is required to be provided to the entire grid. This data is updated every 15 minutes. <br/> <strong>Fun Fact:</strong> The ERCOT system had a record peak demand of 74,820 MW in August 2019. (<a href='https://www.ercot.com/files/docs/2021/12/30/ERCOT_Fact_Sheet.pdf'>Source</a>) <br/><br/>  <a href='https://sa.ercot.com/misapp/GetReports.do?reportTypeId=12340&reportTitle=System-Wide%20Demand&showHTMLView=&mimicKey'>Data Sources</a>", "Demand in GW (SWD)"],
    "fuel-type-generation": ["Total Generation by Fuel Type", "Here, you can observe the real-time and historical power generation of the entire ERCOT grid, as well as the contributions by each distinct fuel source", ["Generation in GW (SEL)", "Generation in GW (GFT)"]],
    "wind-and-solar": ["Wind + Solar Power Generation", "Wind/Solar Power Generation separately displays the total generation of wind and solar power. Since nonrenewable generation greatly exceeds renewable generation, it's useful to observe renewable generation at its relative power scale separately. The data is updated hourly. <br/> <strong>Fun Fact:</strong> The ERCOT system had a record wind generation of 24,681 MW on December 23, 2021, and record solar generation of 7,036 MW on August 3, 2021.", ["Generation in MW (WPP)", "Generation in MW (SPP)"]],
    "system-frequency": ["System Frequency", "System Frequency displays the grid electrical frequency, which is an important parameter that indicates the balance between generation and demand. In North America, 60 Hz is the default electrical frequency in power systems. If demand starts exceeding generation, the system frequency will fall below 60 Hz. If generation starts exceeding demand, the system frequency will rise above 60 Hz. <br/><br/> <strong>Fun Fact:</strong> In the Texas winter storm in February 2021, the system frequency dropped below a critical lower frequency threshold of 59.4 Hz. A protection scheme kicked into place, and would have completely shut down the grid if the frequency had remained under that threshold for 9 minutes. This is done to prevent massive irreversible damage to the grid. Unfortunately, such a collapse can last for weeks. Luckily, with 4 minutes and 37 seconds left to spare, ERCOT was able to restore the frequency to safer levels above the threshold.", "Frequency in Hz (RTSC)"],
    "electricity-prices": ["Real-Time Market Prices", "Real-Time Market Prices displays a critical electricity pricing metric called the Settlement Point Price that ERCOT utilizes to schedule generation while maximizing efficiency in power losses and generation cost. An important distinction is that this metric is not an exact measurement of any specific customer's electricity price, but is rather a closely related value that approximates the changes in customers' actual electricity prices depending on location and retail electricity provider. This data is updated every 15 minutes.", "Load Zone Settlement Point Prices in $ (SMPP)"],

    "RTSL": ["Real Time System Load", "longer description here.", "units here"],
    "RTSC": ["Real Time System Condition", "RTSC longer description to come", "units here"],
    "SASC": ["System Ancilliary Service", "longer description to come", "units here"],
    "SEL": ["State Estimator Load", "longer description to come", "units here"],
    "SPP": ["Solar Power Production", "longer description to come", "units here"],
    "SWL": ["System Wide Load", "longer description to come", "units here"],
    "WPP": ["Wind Power Production", "longer description to come", "units here"],
}

@app.route('/chart/<chart_type>', methods=["GET", "POST"])
@app.route('/chart/<chart_type>/<start_date>/<end_date>', methods=["GET", "POST"])
def chart(chart_type='RTSL', start_date=None, end_date=None):
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
