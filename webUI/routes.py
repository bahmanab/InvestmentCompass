from flask import Flask, request
from flask import render_template
from models import db, PopularCompanies
from forms import DateTimeForm, CompanyListForm
import os
import datetime

app = Flask(__name__)


app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:'+os.environ["postgres_password"]+'@localhost/edgar'
db.init_app(app)

app.secret_key = 'development-key'

def chart_setup():
    chartID = 'chart_ID'
    chart_type = 'line'
    chart_height = '500px'
    chart = {"renderTo": chartID, "type": chart_type, "chart_height": chart_height,}
    title = {"text": 'Popularity Trend Over Time'}
    xAxis = {"type": 'datetime',
             "dateTimeLabelFormats": {"month": '%e. %b', "year": '%b'}, 
             "title": {"text": '', "style": {"fontSize":'20px'}},
             "labels": {"style": {"fontSize":'15px'}}}
    yAxis = {"title": {"text": 'Relative Popularity', "style": {"fontSize":'18px', "width" : '20px'}, 
                       "rotation": 0, "margin": 40}, 
             "min": 0,
             "labels": {"style": {"fontSize":'15px'}}}
    tooltip = {"headerFormat": '<b>{series.name}</b><br>', "pointFormat": '{point.x:%e. %b}: {point.y:.2f} m'}
    plotOptions = {"spline": {"marker": {"enabled": "true"}}}
    credits = {"credits": {"enabled": "false"}}
    return chartID, chart, title, xAxis, yAxis, tooltip, plotOptions, credits


def process_check_boxes():
    show_chart, checked_list, cik_list, name_list = False, [], [], []

    checkbox_values = request.form.getlist('check') 

    if len(checkbox_values) > 0:
        show_chart = True        # turn of plotting chart

    for value in checkbox_values:  
        company_info = value[1:-1].split(',')   
        check_box_index = int(company_info[1])
        cik = company_info[0]
        company_name = company_info[2]
        checked_list.append(check_box_index)
        cik_list.append(cik)
        name_list.append(company_name)

    return show_chart, checked_list, cik_list, name_list


def get_plot_data(popular_companies, name_list, cik_list, date_range):
    if len(name_list):
        series = [{"name": name_list[i], 
                   "data": popular_companies.get_trend_data_for_cik(cik_list[i], date_range)} 
                   for i in range(len(name_list))]
    else:
        series = []

    return series


@app.route("/", methods = ['GET', 'POST'])
def home():
    form = DateTimeForm()
    listform = CompanyListForm();

    # init variables 
    top_companies = ()

    if request.method == 'POST':
        # get the date-time range
        date_time_range = form.date_time_range.data
        date_range = date_time_range.split(' - ')

        # find top 10 popular companies
        popular_companies = PopularCompanies()
        tops = popular_companies.query_top_10(date_range)
        top_companies = popular_companies.convert_query_result_to_list(tops)

        show_chart, checked_list, cik_list, name_list = process_check_boxes()
                                                                                            
        series = get_plot_data(popular_companies, name_list, cik_list, date_range)
        
        chartID, chart, title, xAxis, yAxis, tooltip, plotOptions, credits = chart_setup()

        return render_template('home.html', form=form, top_companies=top_companies, listform=listform,
                                chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, 
                                yAxis=yAxis, tooltip=tooltip, plotOptions=plotOptions, 
                                show_chart=show_chart, credits=credits, checked_list=checked_list)
 
    elif request.method == 'GET':
        return render_template('home.html', form=form)


if __name__ == "__main__":
    app.run(host='0.0.0.0',debug=True)
