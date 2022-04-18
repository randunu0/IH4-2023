from flask import Flask, render_template, request, redirect, url_for
import datetime

from resources.database import get_chart

application = app = Flask(__name__, static_url_path='', static_folder='')
app.secret_key = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
app.url_map.strict_slashes = False

@app.before_request
def before_request_secure():
    scheme = request.headers.get('X-Forwarded-Proto')
    if scheme and scheme == 'http' and request.url.startswith('http://'):
        url = request.url.replace('http://', 'https://', 1)
        code = 301
        return redirect(url, code=code)

# This dictionary is used to populate each page with content
# Format is <URL>: {<Title>, <Menu Title (shorter)>, <Image>, <Icon>, <Units for Chart>, <Data Refresh Rate>, <Short Description>, <Longer HTML Description>}
# For charts with multiple lines, the Units are an array instead of a string
chart_page_content = {
    "system-wide-demand": {
        "title": "System-Wide Demand",
        "menu_title": "System Wide Demand",
        "image": "https://images.unsplash.com/photo-1413882353314-73389f63b6fd?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2070&q=80",
        "icon": "mdi-led-on",
        "units": "Demand in GW (SWD)",
        "refresh": "5 minutes",
        "short_description": "",
        "html_description": "System-Wide Demand displays the current and historical power demand of the ERCOT grid, which is the expected total electrical power that is required to be provided to the entire grid. The scheduled generation for the total load ('load' is an engineering term that means the recipient/consumer of electrical power) is calculated with the expected total demand of all customers, among many other variables, so ERCOT uses dynamic forecasting models to try to maximally reduce the error between the forecasted demand and actual demand. Naturally, one may wonder how difficult correctly predicting the total system demand could be, or why it's so important to be as accurate as possible. For starters, ERCOT serves at least 26 million Texans, most of whom are residential consumers with smaller loads plus a smaller number of commercial and other non-residential consumers with typically much larger loads. Each load contributes its own variability to the total load and demand of the system. A few other major factors of variability in demand include weather and social events (such as the Super Bowl). It's important to be as accurate as possible so that the total generation is controlled to be as close to the total actual demand; the difference in power between generation and demand causes variance in the system frequency, which is very sensitive and must be kept relatively stable. This data is updated every 15 minutes. <br/> <br/> <strong>Fun Fact:</strong> The ERCOT system had a record peak demand of 74,820 MW in August 2019. (<a href='https://www.ercot.com/files/docs/2021/12/30/ERCOT_Fact_Sheet.pdf'>Source</a>) <br/><br/>  <a href='https://sa.ercot.com/misapp/GetReports.do?reportTypeId=12340&reportTitle=System-Wide%20Demand&showHTMLView=&mimicKey'>Data Sources</a>"
    },
    "fuel-type-generation": {
        "title": "Generation by Fuel Type",
        "menu_title": "Fuel Type Generation",
        "image": "https://images.unsplash.com/photo-1481127303226-3f47f8af862d?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2940&q=80",
        "icon": "mdi-gas-cylinder",
        "units": "Generation in GWh (SEL)",
        "refresh": "15 minutes",
        "short_description": "",
        "html_description": "Generation by Fuel Type displays the real-time and historical power generation of the entire ERCOT grid, as well as the contributions by each distinct fuel source. To decide how to execute power generation for its grid, ERCOT uses an algorithm called Security Constrained Economic Dispatch (SCED) that calculates which combination of available power generators' offers (a.k.a. the amount of power a generator is willing to provide for a specific price) meets total generation requirements, results in the least total cost of dispatch (a.k.a. the actual execution of power delivery), and minimizes the total losses over the grid's web of transmission lines. Both datasets are in 15 minute intervals, with the stacked bar chart of the fuel source contributions being updated at the beginning of each month for the last month's data. <br/> <br/> <strong>Fun Fact:</strong> Texas is the largest energy-producer and energy-consumer in the US. It produces the most crude oil, natural gas and wind in the nation. (<a href='https://www.eia.gov/state/?sid=TX#:~:text=Quick%20Facts&text=Texas%20leads%20the%20nation%20in,power%20plants%20combined%20in%202020'>Source</a>) <br/><br/><a href='https://www.ercot.com/files/docs/2021/11/08/IntGenbyFuel2021.xlsx'>Data Source (2021)</a>, <a href='https://www.ercot.com/files/docs/2022/02/08/IntGenbyFuel2022.xlsx'>Data Source (2022)</a>"
    },
    "system-frequency": {
        "title": "System Frequency",
        "menu_title": "System Frequency",
        "image": "https://images.unsplash.com/photo-1609669371960-f5f046c8e9e2?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2940&q=80",
        "icon": "mdi-pulse",
        "units": "Frequency in Hz",
        "refresh": "1 minute",
        "short_description": "",
        "html_description": "System Frequency displays the grid electrical frequency, which is an important parameter that indicates the balance between generation and demand. In North America, 60 Hz is the default electrical frequency in power systems. If demand starts exceeding generation, the system frequency will fall below 60 Hz. If generation starts exceeding demand, the system frequency will rise above 60 Hz.<br/> <br/> When generation drops significantly below demand, it's important to prevent the system frequency from dropping below certain critical thresholds to avoid catastrophe. For instance, in the Texas winter storm in February 2021, the system frequency dropped below one such threshold, at 59.4 Hz. A protection scheme kicked into place and would have completely shut down the grid if the frequency remained under that threshold for 9 minutes. This is to prevent irreversible damage to the grid but unfortunately results in the complete collapse of the grid, which could have lasted for weeks. Fortunately, with 4 minutes and 37 seconds left to spare, the frequency was restored to safer levels above the threshold. <br/><br/> <a href='http://www.ercot.com/content/cdr/html/real_time_system_conditions.html'>Data Sources</a>"
    },
    "wind-and-pv": {
        "title": "Wind + Photovoltaic (PV) Power Generation",
        "menu_title": "Wind and PV",
        "image": "https://images.unsplash.com/photo-1466611653911-95081537e5b7?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2940&q=80",
        "icon": "mdi-weather-sunny",
        "units": ["Generation in MW (Wind)", "Generation in MW (PV)"],
        "refresh": "5 minutes",
        "short_description": "",
        "html_description": "Wind and Photovoltaic (PV) Power Generation displays the system-wide wind and photovoltaic power production. This data is updated every hour. <br/><br/> Photovoltaic Power Generation depends on four factors, namely: sun intensity, cloud cover, module temperature, and humidity. Most solar energy is collected when the sun is at its peak, which is usually midday. When it is a cloudy day, there is a decrease in power generation, because the cloud reflects some of the sun’s rays and limits the amount of sun absorption by the solar panels. However, solar cells work best at low temperatures so when the temperature is at its highest, typically during the summer, the heat causes the semiconductor properties to shift, which results in the panel’s performance reduction. Another factor is humidity because it can reduce solar power output by reflecting or refracting the sunlight away from solar cells, which reduces the amount of sunlight that hits the panels. When humidity penetrates the solar panel, it can also degrade the solar panel itself. (<a href='https://www.sciencedirect.com/science/article/pii/S1364032111000256?casa_token=niTg6lWrHLcAAAAA:isUFaywvmhSpvOFR7PzvymGpu_CIiT7qyNA92qunCQKjl1T1U6ncgOvPc9DnWINXO3rgx-6UTrES'>Source 1</a>), (<a href='https://www.renewablegreenenergypower.com/solar-energy/solar-panel-efficiency'>Source 2</a>) <br/><br/> Wind power generation in an area depends on three factors: wind speed, air density, and weather temperature. A wind turbine is usually shut down when the wind speed is either below or above the cut-in values. When the wind speed is above the cut-in value, the wind turbine extracts the maximum amount of power. However, the relationship between wind speed and output power is not linearly proportional. Meanwhile, the energy produced by the wind is directly proportional to air density. The higher the air density, the larger the amount of power extracted from the wind turbines. Wind power generation is inversely proportional to temperature. The lower the weather temperature the larger the output power from the wind turbines. (<a href= 'https://ieeexplore.ieee.org/document/8301377'>Source</a>) <br/><br /><strong>Fun Fact:</strong> The ERCOT system had a record wind generation of 24,681 MW on December 23, 2021, and a record photovoltaic generation of 7,036 MW on August 3, 2021. (<a href='https://www.ercot.com/files/docs/2021/12/30/ERCOT_Fact_Sheet.pdf'>Source</a>) <br/><br/><a href='https://sa.ercot.com/misapp/GetReports.do?reportTypeId=13028&amp;reportTitle=Wind%20Power%20Production%20-%20Hourly%20Averaged%20Actual%20and%20Forecasted%20Values&amp;showHTMLView=&amp;mimicKey'>Data Source (Wind)</a>, <a href='https://sa.ercot.com/misapp/GetReports.do?reportTypeId=13483&amp;reportTitle=Solar%20Power%20Production%20-%20Hourly%20Averaged%20Actual%20and%20Forecasted%20Values&amp;showHTMLView=&amp;mimicKey'>Data Source (PV)</a>"
    },
    
    "electricity-prices": {
        "title": "Real-Time Market Prices",
        "menu_title": "Market Prices",
        "image": "https://images.unsplash.com/photo-1611187401884-254eb9d99ed6?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=2940&q=80",
        "icon": "mdi-currency-usd",
        "units": "Load Zone Settlement Point Prices in $/MWh (SMPP)",
        "refresh": "month",
        "short_description": "",
        "html_description": "Real-Time Market Prices displays a critical electricity pricing metric called the Settlement Point Price (SPP), which is an important measure that's calculated by ERCOT and used by electricity retailers to determine pricing plans for customers. SPPs are calculated with Locational Marginal Prices (LMPs), which are determined at each generator with ERCOT's SCED algorithm, plus additional operational costs for maintenance of constantly available power reserves that exist in case actual demand is much larger than forecasted demand. An important distinction is that SPPs are not an exact measurement of any specific customer's electricity price, but is rather a closely related value that approximates the changes in customers' actual electricity prices depending on location and retail electricity provider. This data is updated every 15 minutes. <br/><br/> <a href='https://www.ercot.com/misapp/GetReports.do?reportTypeId=12301&reportTitle=Settlement%20Point%20Prices%20at%20Resource%20Nodes,%20Hubs%20and%20Load%20Zones&showHTMLView=&mimicKey'>Data Sources</a>"
    }
}


@app.context_processor
def inject_charts():
    return dict(chart_page_content=chart_page_content)

@app.route("/")
def home():
    return render_template('home.html')


@app.route('/chart/<chart_type>', methods=["GET", "POST"])
@app.route('/chart/<chart_type>/<start_date>/<end_date>', methods=["GET", "POST"])
def chart(chart_type=None, start_date=None, end_date=None):
    if request.method == "GET":
        if chart_type in chart_page_content:
            if start_date and end_date:
                try:
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%Y')
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y')
                    chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                    start_date = datetime.datetime.strptime(chartlabels[0], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                    end_date = datetime.datetime.strptime(chartlabels[-1], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                except:
                    chartdata, chartlabels = get_chart(chart_type, start_date, end_date)
                    start_date = datetime.datetime.strptime(chartlabels[0], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
                    end_date = datetime.datetime.strptime(chartlabels[-1], '%Y-%m-%d %H:%M').strftime('%m/%d/%Y')
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

@app.errorhandler(404)
def FUN_404(error):
    return render_template("404.html"), 404

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5001)
